//! btrfs-send engine: CoW snapshot → `btrfs send` → `zstd` → stream file.
//!
//! Progress is real: we count every raw byte that flows through the pipe and
//! poll the output file for the compressed size. No `pv` needed.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Context, Result};
use serde::Serialize;

/// Sanitize a source path into a snapshot/stream name.
/// `/` → `rootfs`, `/home` → `home`, `/var/lib/portables` → `var_lib_portables`.
pub fn name_for(source: &Path) -> String {
    let s = source.to_string_lossy();
    if s == "/" {
        return "rootfs".into();
    }
    s.trim_start_matches('/').replace('/', "_")
}

/// Timestamp suffix for snapshots: UTC `YYYY-MM-DD-HH-MM-SS`.
pub fn ts_utc() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let (y, mo, d, h, mi, s) = civil(secs);
    format!("{y:04}-{mo:02}-{d:02}-{h:02}-{mi:02}-{s:02}")
}

/// UTC date `YYYY-MM-DD` from unix seconds (civil_from_days).
pub fn date_utc(secs: i64) -> String {
    let (y, mo, d, _, _, _) = civil(secs);
    format!("{y:04}-{mo:02}-{d:02}")
}

fn civil(secs: i64) -> (i64, i64, i64, i64, i64, i64) {
    let days = secs.div_euclid(86400);
    let rem = secs.rem_euclid(86400);
    let h = rem / 3600;
    let mi = (rem % 3600) / 60;
    let s = rem % 60;
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d, h, mi, s)
}

/// Deletes its snapshots on drop — on any exit path (success, error, panic,
/// early Ctrl-C). Call `consume()` on success to keep the cleanup list empty
/// (the stream is self-contained; the anchor snapshot is no longer needed).
pub struct SnapGuard(Vec<PathBuf>);

impl SnapGuard {
    pub fn new() -> Self {
        Self(Vec::new())
    }
    pub fn add(&mut self, p: PathBuf) {
        self.0.push(p);
    }
    /// Drop all tracked snapshots (idempotent).
    pub fn consume(&mut self) {
        for s in self.0.drain(..) {
            let _ = Command::new("btrfs").args(["subvolume", "delete"]).arg(&s).output();
        }
    }
}

impl Drop for SnapGuard {
    fn drop(&mut self) {
        self.consume();
    }
}

/// Result of one sent subvolume.
#[derive(Debug, Clone)]
pub struct SentStream {
    pub name: String,
    pub snapshot: PathBuf,
    pub out: PathBuf,
    /// Raw bytes emitted by `btrfs send`.
    pub raw_bytes: u64,
    /// Compressed file size on disk.
    pub size_bytes: u64,
}

/// Take a read-only CoW snapshot of a subvolume at the consistent moment
/// `t`. Returns the snapshot path. Caller owns cleanup via [`SnapGuard`].
pub fn snapshot(source: &Path) -> Result<PathBuf> {
    let name = name_for(source);
    let snap = PathBuf::from(format!("/.keel-{name}-{}", ts_utc()));
    let so = Command::new("btrfs")
        .args(["subvolume", "snapshot", "-r"])
        .arg(source)
        .arg(&snap)
        .output()
        .context("failed to spawn `btrfs subvolume snapshot`")?;
    if !so.status.success() {
        let err = String::from_utf8_lossy(&so.stderr);
        bail!("snapshot of {} failed: {}", source.display(), err.trim());
    }
    Ok(snap)
}

pub struct SendStreamConfig<'a> {
    /// Display/manifest name for this source (e.g. `home`).
    pub name: &'a str,
    /// Snapshot to send (created with [`snapshot`]).
    pub snapshot: &'a Path,
    pub out: &'a Path,
    pub zstd_level: u8,
    /// `None` → `-T0` (auto).
    pub zstd_threads: Option<usize>,
    /// raw_bytes, compressed_so_far
    pub on_progress: &'a dyn Fn(u64, u64),
}

/// `btrfs send <snapshot> | zstd` → stream file. No snapshot management:
/// the caller keeps the anchor snapshot alive for as long as it wants (e.g.
/// while a borg archive of the same snapshot runs) and deletes it afterwards.
pub fn send_stream(cfg: &SendStreamConfig) -> Result<SentStream> {
    let name = cfg.name.to_string();
    let snap = cfg.snapshot.to_path_buf();

    // 2) Compressor: zstd reads stdin, writes the output file.
    let mut zargs: Vec<String> = Vec::new();
    zargs.push(format!("-{}", cfg.zstd_level));
    zargs.push(match cfg.zstd_threads {
        Some(n) => format!("-T{n}"),
        None => "-T0".to_string(),
    });
    zargs.extend(["-q", "-f", "-o"].iter().map(|s| s.to_string()));
    zargs.push(cfg.out.to_string_lossy().to_string());

    let mut zstd = Command::new("zstd")
        .args(&zargs)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn zstd")?;

    // 3) Sender: btrfs send <snapshot> → stdout.
    let mut sender = Command::new("btrfs")
        .args(["send"])
        .arg(&snap)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn `btrfs send`")?;

    let mut s_out = sender.stdout.take().expect("stdout piped");
    let mut z_in = zstd.stdin.take().expect("stdin piped");
    let raw = std::sync::Arc::new(AtomicU64::new(0));

    // Pump thread: sender → compressor, counting raw bytes.
    let raw_pump = raw.clone();
    let pump = std::thread::spawn(move || {
        let mut buf = [0u8; 1 << 16];
        loop {
            match s_out.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if z_in.write_all(&buf[..n]).is_err() {
                        break; // compressor died
                    }
                    raw_pump.fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(_) => break,
            }
        }
        // z_in dropped here → compressor sees EOF.
    });

    // 4) Progress: poll until the compressor finishes.
    loop {
        if zstd.try_wait().ok().flatten().is_some() {
            break;
        }
        let written = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
        (cfg.on_progress)(raw.load(Ordering::Relaxed), written);
        std::thread::sleep(std::time::Duration::from_millis(120));
    }

    let s_err_h = sender.stderr.take();
    let s_status = sender.wait().context("waiting for btrfs send")?;
    let s_err = s_err_h.map(|mut h| stderr_text(&mut h)).unwrap_or_default();
    let pump_res = pump.join();
    let z_err_h = zstd.stderr.take();
    let z_status = zstd.wait().context("waiting for zstd")?;
    let z_err = z_err_h.map(|mut h| stderr_text(&mut h)).unwrap_or_default();

    let raw_total = raw.load(Ordering::Relaxed);

    if pump_res.is_err() {
        bail!("pipe pump thread panicked while sending {}", snap.display());
    }
    if !s_status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!(
            "`btrfs send {}` failed ({}): {}",
            snap.display(),
            s_status.code().map(|c| c.to_string()).unwrap_or_default(),
            s_err.trim()
        );
    }
    if !z_status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!("zstd compression failed: {}", z_err.trim());
    }

    let size = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
    Ok(SentStream {
        name,
        snapshot: snap,
        out: cfg.out.to_path_buf(),
        raw_bytes: raw_total,
        size_bytes: size,
    })
}

pub struct TarStreamConfig<'a> {
    /// Display/manifest name for this source (e.g. `boot`).
    pub name: &'a str,
    /// Directory whose *contents* are archived (the tree root).
    pub src: &'a Path,
    /// Exclude globs relative to the tree root (tar-style, unanchored),
    /// e.g. `blobs` skips `blobs/` wherever it appears under the root.
    pub excludes: &'a [String],
    pub out: &'a Path,
    pub zstd_level: u8,
    /// `None` → `-T0` (auto).
    pub zstd_threads: Option<usize>,
    /// raw_bytes, compressed_so_far
    pub on_progress: &'a dyn Fn(u64, u64),
}

/// `tar -c --exclude=… -C <src> . | zstd` → stream file.
///
/// Archive paths are relative to the tree root (`./a/b.txt`), so extracting
/// into any directory reproduces the tree. Excludes are applied by tar
/// itself — excluded files are never read or stored. Works on any filesystem
/// (the point of this engine); when the source is a btrfs subvolume the
/// caller should point `src` at the CoW snapshot for consistency.
pub fn tar_stream(cfg: &TarStreamConfig) -> Result<SentStream> {
    let name = cfg.name.to_string();

    // Compressor: zstd reads stdin, writes the output file.
    let mut zargs: Vec<String> = Vec::new();
    zargs.push(format!("-{}", cfg.zstd_level));
    zargs.push(match cfg.zstd_threads {
        Some(n) => format!("-T{n}"),
        None => "-T0".to_string(),
    });
    zargs.extend(["-q", "-f", "-o"].iter().map(|s| s.to_string()));
    zargs.push(cfg.out.to_string_lossy().to_string());

    let mut zstd = Command::new("zstd")
        .args(&zargs)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn zstd")?;

    // Archiver: options FIRST (GNU tar rejects options after positional
    // args, and `-C` is positional: everything after it is archived from
    // that directory). `.` = the whole tree, stored root-relative.
    let mut targs: Vec<String> = vec!["-c".to_string()];
    for e in cfg.excludes {
        targs.push(format!("--exclude={e}"));
    }
    targs.push("-C".to_string());
    targs.push(cfg.src.to_string_lossy().to_string());
    targs.push(".".to_string());

    let mut tar = Command::new("tar")
        .args(&targs)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn `tar`")?;

    let mut t_out = tar.stdout.take().expect("stdout piped");
    let mut z_in = zstd.stdin.take().expect("stdin piped");
    let raw = std::sync::Arc::new(AtomicU64::new(0));

    // Pump thread: tar → compressor, counting raw bytes.
    let raw_pump = raw.clone();
    let pump = std::thread::spawn(move || {
        let mut buf = [0u8; 1 << 16];
        loop {
            match t_out.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if z_in.write_all(&buf[..n]).is_err() {
                        break; // compressor died
                    }
                    raw_pump.fetch_add(n as u64, Ordering::Relaxed);
                }
                Err(_) => break,
            }
        }
        // z_in dropped here → compressor sees EOF.
    });

    // Progress: poll until the compressor finishes.
    loop {
        if zstd.try_wait().ok().flatten().is_some() {
            break;
        }
        let written = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
        (cfg.on_progress)(raw.load(Ordering::Relaxed), written);
        std::thread::sleep(std::time::Duration::from_millis(120));
    }

    let t_err_h = tar.stderr.take();
    let t_status = tar.wait().context("waiting for tar")?;
    let t_err = t_err_h.map(|mut h| stderr_text(&mut h)).unwrap_or_default();
    let pump_res = pump.join();
    let z_err_h = zstd.stderr.take();
    let z_status = zstd.wait().context("waiting for zstd")?;
    let z_err = z_err_h.map(|mut h| stderr_text(&mut h)).unwrap_or_default();

    let raw_total = raw.load(Ordering::Relaxed);

    if pump_res.is_err() {
        bail!("pipe pump thread panicked while tarring {}", cfg.src.display());
    }
    if !t_status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!(
            "`tar {}` failed ({}): {}",
            cfg.src.display(),
            t_status.code().map(|c| c.to_string()).unwrap_or_default(),
            t_err.trim()
        );
    }
    if !z_status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!("zstd compression failed: {}", z_err.trim());
    }

    let size = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
    Ok(SentStream {
        name,
        snapshot: cfg.src.to_path_buf(), // path actually read (snapshot or live tree)
        out: cfg.out.to_path_buf(),
        raw_bytes: raw_total,
        size_bytes: size,
    })
}

/// Run `bin` as-is, or (when `run_as` is set) wrap it in
/// `runuser -u <user> -- bin` — keel itself runs as root.
/// How to exec `bin` when the profile asks for `run_as`.
///
/// Prefer `machinectl shell <user>@ -- <abs-path>`: it starts a real PAM
/// session inside the target user's own systemd user slice. Rootless
/// container engines (podman) must manage their own cgroup/user namespaces
/// at startup — and they refuse to do that from a root-owned session
/// cgroup (`runuser` keeps the caller's cgroup, so rootless podman dies with
/// "error opening namespace handles: Permission denied"; verified on
/// FELINE, 2026-11-27). machinectl also demands an absolute binary path, so
/// we resolve it; if machinectl or the binary is unresolvable we fall back
/// to plain `runuser` (fine for tools that don't need their own cgroup).
fn as_user(bin: &str, run_as: Option<&str>) -> (String, Vec<String>) {
    let Some(u) = run_as else {
        return (bin.to_string(), Vec::new());
    };
    if let (Some(mach), Some(abspath)) =
        (crate::detect::which("machinectl"), crate::detect::which(bin))
    {
        return (
            mach,
            vec!["shell".into(), format!("{u}@"), "--".into(), abspath],
        );
    }
    (
        "runuser".to_string(),
        vec!["-u".into(), u.to_string(), "--".into(), bin.to_string()],
    )
}

/// Expand exclude patterns for mksquashfs (squashfs-tools 4.7.4 semantics,
/// verified empirically): with `-wildcards`, `*` never crosses `/` and `**`
/// collapses to `*` — so no single pattern can match a name at *any* depth.
/// For a simple name (no `/` or glob characters), or the portable `**/name`
/// form, emit a depth ladder — `name`, `*/name`, `*/*/name`, … — that covers
/// the top level and every nesting depth. Other patterns pass through as-is.
pub fn squashfs_excludes(excludes: &[String]) -> Vec<String> {
    const MAX_DEPTH: usize = 8;
    let mut out = Vec::new();
    for e in excludes {
        let core = e.strip_prefix("**/").unwrap_or(e.as_str());
        let simple = !core.is_empty()
            && !core.contains('/')
            && !core.contains('*')
            && !core.contains('?')
            && !core.contains('[');
        if simple {
            for depth in 0..=MAX_DEPTH {
                let pre = vec!["*".to_string(); depth].join("/");
                out.push(if depth == 0 {
                    core.to_string()
                } else {
                    format!("{pre}/{core}")
                });
            }
        } else {
            out.push(e.clone());
        }
    }
    out
}

pub struct SquashfsConfig<'a> {
    /// Display/manifest name for this source (e.g. `home`).
    pub name: &'a str,
    /// Directory whose contents are imaged (the tree root).
    pub src: &'a Path,
    /// Exclude patterns, already expanded with [`squashfs_excludes`].
    pub excludes: &'a [String],
    pub out: &'a Path,
    /// zstd level for `-Xcompression-level` (mksquashfs bundles zstd).
    pub zstd_level: u8,
    /// raw_estimate, compressed_so_far
    pub on_progress: &'a dyn Fn(u64, u64),
}

/// `mksquashfs <src> <out> -quiet -comp zstd -Xcompression-level N
/// [-wildcards -e …]` → read-only image artifact.
///
/// Raw bytes are estimated with `du -sb` (counts excluded files too — it is a
/// progress estimate, like any whole-tree archive). Excluded files are never
/// imaged (mksquashfs prunes them from the tree).
pub fn squashfs(cfg: &SquashfsConfig) -> Result<SentStream> {
    let name = cfg.name.to_string();

    // Raw-size estimate for the progress bar.
    let raw_estimate: u64 = match Command::new("du").args(["-sb"]).arg(cfg.src).output() {
        Ok(o) if o.status.success() => String::from_utf8_lossy(&o.stdout)
            .split_whitespace()
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0),
        _ => 0,
    };

    let mut args: Vec<String> = Vec::new();
    args.push(cfg.src.to_string_lossy().to_string());
    args.push(cfg.out.to_string_lossy().to_string());
    args.push("-quiet".into());
    args.extend(["-comp".into(), "zstd".into()]);
    args.extend(["-Xcompression-level".into(), cfg.zstd_level.to_string()]);
    if !cfg.excludes.is_empty() {
        args.push("-wildcards".into());
        for e in cfg.excludes {
            args.push("-e".into());
            args.push(e.clone());
        }
    }

    let mut mksq = Command::new("mksquashfs")
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .context("failed to spawn mksquashfs")?;

    // Progress: the image file grows as blocks are written.
    loop {
        if mksq.try_wait().ok().flatten().is_some() {
            break;
        }
        let written = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
        (cfg.on_progress)(raw_estimate, written);
        std::thread::sleep(std::time::Duration::from_millis(120));
    }

    let e_h = mksq.stderr.take();
    let status = mksq.wait().context("waiting for mksquashfs")?;
    let err = e_h.map(|mut h| stderr_text(&mut h)).unwrap_or_default();
    if !status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!(
            "`mksquashfs {}` failed ({}): {}",
            cfg.src.display(),
            status.code().map(|c| c.to_string()).unwrap_or_default(),
            err.trim()
        );
    }
    let size = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
    if size == 0 {
        let _ = std::fs::remove_file(cfg.out);
        bail!("mksquashfs produced an empty image (source {}?)", cfg.src.display());
    }
    Ok(SentStream {
        name,
        snapshot: cfg.src.to_path_buf(), // path actually read
        out: cfg.out.to_path_buf(),
        raw_bytes: raw_estimate,
        size_bytes: size,
    })
}

pub struct PgDumpConfig<'a> {
    pub database: &'a str,
    pub user: Option<&'a str>,
    pub host: Option<&'a str>,
    pub port: Option<u16>,
    /// OS user to run pg_dump as (via `runuser`).
    pub run_as: Option<&'a str>,
    /// Output dump file.
    pub out: &'a Path,
}

/// `pg_dump -Fc -f <out> [-U user] [-h host] [-p port] <db>` — custom-format
/// (self-compressing, sectioned) dump. The artifact is the portable form;
/// restore with `pg_restore`.
pub fn pg_dump(cfg: &PgDumpConfig) -> Result<u64> {
    let mut args: Vec<String> = vec!["-Fc".into(), "-f".into(), cfg.out.to_string_lossy().to_string()];
    if let Some(u) = cfg.user {
        args.push("-U".into());
        args.push(u.into());
    }
    if let Some(h) = cfg.host {
        args.push("-h".into());
        args.push(h.into());
    }
    if let Some(p) = cfg.port {
        args.push("-p".into());
        args.push(p.to_string());
    }
    args.push(cfg.database.into());

    let (bin, pre) = as_user("pg_dump", cfg.run_as);
    let o = Command::new(&bin)
        .args(&pre)
        .args(&args)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| format!("failed to spawn pg_dump (database {})", cfg.database))?;
    if !o.status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!(
            "`pg_dump {}` failed ({}): {}",
            cfg.database,
            o.status.code().map(|c| c.to_string()).unwrap_or_default(),
            String::from_utf8_lossy(&o.stderr).trim()
        );
    }
    let size = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
    if size == 0 {
        let _ = std::fs::remove_file(cfg.out);
        bail!("pg_dump produced an empty dump (database {})", cfg.database);
    }
    Ok(size)
}

pub struct PodmanSaveConfig<'a> {
    pub images: &'a [String],
    /// Commit a live container to `tag` first, then save that tag too.
    pub commit: Option<&'a PodmanCommitSpec>,
    /// OS user to run podman as (via `runuser`).
    pub run_as: Option<&'a str>,
    pub out: &'a Path,
}

/// Container image spec for [`podman_save`].
pub struct PodmanCommitSpec {
    pub container: String,
    pub tag: String,
}

/// Optional `podman commit <container> <tag>` (consistent CoW state of a
/// live container — the legacy script's quadlet pattern), then
/// `podman save -o <out> <images…>`. Always `-o` (saving to stdout is
/// unreliable: progress text can leak into the tar). Returns the file size.
pub fn podman_save(cfg: &PodmanSaveConfig) -> Result<u64> {
    let mut to_save: Vec<String> = cfg.images.to_vec();
    if let Some(c) = cfg.commit {
        if !to_save.iter().any(|i| i == &c.tag) {
            to_save.push(c.tag.clone());
        }
        let (bin, pre) = as_user("podman", cfg.run_as);
        let o = Command::new(&bin)
            .args(&pre)
            .args(["commit", &c.container, &c.tag])
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .with_context(|| format!("failed to spawn podman commit {}", c.container))?;
        if !o.status.success() {
            bail!(
                "`podman commit {} {}` failed ({}): {}",
                c.container,
                c.tag,
                o.status.code().map(|c| c.to_string()).unwrap_or_default(),
                String::from_utf8_lossy(&o.stderr).trim()
            );
        }
    }
    if to_save.is_empty() {
        bail!("podman_save: nothing to save (no images and no commit)");
    }
    let (bin, pre) = as_user("podman", cfg.run_as);
    let mut cmd = Command::new(&bin);
    cmd.args(&pre).arg("save").arg("-o").arg(cfg.out);
    for i in &to_save {
        cmd.arg(i);
    }
    let o = cmd
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .with_context(|| "failed to spawn podman save")?;
    if !o.status.success() {
        let _ = std::fs::remove_file(cfg.out);
        bail!(
            "`podman save {}` failed ({}): {}",
            to_save.join(", "),
            o.status.code().map(|c| c.to_string()).unwrap_or_default(),
            String::from_utf8_lossy(&o.stderr).trim()
        );
    }
    let size = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
    if size == 0 {
        let _ = std::fs::remove_file(cfg.out);
        bail!(
            "podman save wrote no data to {} (image missing in that user's store, or the user cannot create files in the destination dir)",
            cfg.out.display()
        );
    }
    Ok(size)
}

/// One row of `systemctl list-unit-files`.
#[derive(Debug, Clone, Serialize)]
pub struct UnitFile {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
    pub state: String,
}

#[derive(Debug, Serialize)]
struct SystemdStateDoc {
    format: String,
    captured_utc: String,
    units: Vec<UnitFile>,
}

/// `systemctl list-unit-files` → YAML artifact (unit, type, state) for
/// bare-metal restore: on the fresh system, `systemctl enable` the enabled
/// units and `systemctl mask` the masked ones. Returns the file size.
pub fn systemd_state(out: &Path) -> Result<u64> {
    let o = Command::new("systemctl")
        .args(["list-unit-files", "--no-legend", "--no-pager"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("failed to spawn systemctl list-unit-files")?;
    if !o.status.success() {
        bail!(
            "`systemctl list-unit-files` failed ({}): {}",
            o.status.code().map(|c| c.to_string()).unwrap_or_default(),
            String::from_utf8_lossy(&o.stderr).trim()
        );
    }
    let mut units = Vec::new();
    for line in String::from_utf8_lossy(&o.stdout).lines() {
        let mut it = line.split_whitespace();
        let (Some(name), Some(ty), Some(state)) = (it.next(), it.next(), it.next()) else {
            continue;
        };
        units.push(UnitFile {
            name: name.into(),
            ty: ty.into(),
            state: state.into(),
        });
    }
    if units.is_empty() {
        bail!("systemctl list-unit-files returned no units — cannot capture systemd state");
    }
    let doc = SystemdStateDoc {
        format: "keel/systemd-state/1".into(),
        captured_utc: ts_utc(),
        units,
    };
    let text = serde_yaml::to_string(&doc).context("serializing systemd state")?;
    std::fs::write(out, text.as_bytes())
        .with_context(|| format!("writing systemd state to {}", out.display()))?;
    let size = std::fs::metadata(out).map(|m| m.len()).unwrap_or(0);
    Ok(size)
}

fn stderr_text(h: &mut std::process::ChildStderr) -> String {
    use std::io::Read as _;
    let mut b = Vec::new();
    let _ = h.read_to_end(&mut b);
    String::from_utf8_lossy(&b).into_owned()
}

/// Human-readable bytes (binary).
pub fn human(bytes: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes} B")
    } else {
        format!("{v:.1} {}", UNITS[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert_eq!(name_for(Path::new("/")), "rootfs");
        assert_eq!(name_for(Path::new("/home")), "home");
        assert_eq!(name_for(Path::new("/var/lib/portables")), "var_lib_portables");
    }

    #[test]
    fn squashfs_exclude_ladder() {
        let ex = squashfs_excludes(&vec!["**/blobs".to_string()]);
        assert_eq!(ex[0], "blobs");
        assert_eq!(ex[1], "*/blobs");
        assert_eq!(ex[2], "*/*/blobs");
        assert_eq!(ex.len(), 9); // depths 0..=8
        assert!(ex[1..].iter().all(|e| e.ends_with("/blobs")));
        // portable **/name and plain name give the same ladder
        assert_eq!(squashfs_excludes(&vec!["blobs".to_string()]), ex);
        // wildcard patterns pass through untouched
        let ex2 = squashfs_excludes(&vec!["*.cache".to_string(), "dir/sub".to_string()]);
        assert_eq!(ex2, vec!["*.cache".to_string(), "dir/sub".to_string()]);
    }

    #[test]
    fn civil_date() {
        // 1970-01-01 00:00:00 UTC
        assert_eq!(civil(0), (1970, 1, 1, 0, 0, 0));
        // 2026-01-01 00:00:00 UTC = 20454 days
        assert_eq!(civil(20454 * 86400), (2026, 1, 1, 0, 0, 0));
        // 2026-01-01 13:45:07 UTC
        assert_eq!(civil(20454 * 86400 + 13 * 3600 + 45 * 60 + 7), (2026, 1, 1, 13, 45, 7));
    }

    /// Live: real `mksquashfs` with excludes + real `systemctl` capture.
    /// Gated on binary presence (CI without squashfs-tools/systemd skips).
    #[test]
    fn live_squashfs_and_systemd_state() {
        let base = std::env::temp_dir().join(format!("keel-live-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let src = base.join("src");
        std::fs::create_dir_all(src.join("keep")).unwrap();
        std::fs::create_dir_all(src.join("excl")).unwrap();
        std::fs::write(src.join("keep/data.txt"), b"hello phase4").unwrap();
        std::fs::write(src.join("excl/big.bin"), vec![7u8; 1 << 20]).unwrap();

        // squashfs + exclude ladder (gated on mksquashfs)
        if crate::detect::which("mksquashfs").is_some() {
            let out = base.join("out.squashfs");
            let s = squashfs(&SquashfsConfig {
                name: "keel-test",
                src: &src,
                excludes: &squashfs_excludes(&["excl".to_string()]),
                out: &out,
                zstd_level: 3,
                on_progress: &|_, _| {},
            })
            .expect("mksquashfs run failed");
            assert!(s.size_bytes > 0);
            if crate::detect::which("unsquashfs").is_some() {
                let o = Command::new("unsquashfs")
                    .arg("-l")
                    .arg(out.to_string_lossy().as_ref())
                    .output()
                    .expect("unsquashfs -l");
                let list = String::from_utf8_lossy(&o.stdout).to_string();
                assert!(list.contains("keep/data.txt"), "kept file missing from image:\n{list}");
                assert!(!list.contains("big.bin"), "excluded file leaked into image:\n{list}");
            }
        }

        // systemd state capture (gated on systemctl)
        if crate::detect::which("systemctl").is_some() {
            let out = base.join("state.yaml");
            let size = systemd_state(&out).expect("systemd_state failed");
            assert!(size > 0);
            let text = std::fs::read_to_string(&out).unwrap();
            assert!(text.contains("keel/systemd-state/1"), "missing format tag");
            let n_units = text.lines().filter(|l| l.trim_start().starts_with("- name:")).count();
            assert!(
                n_units > 0,
                "no units in capture:\n{}",
                &text[..text.len().min(400)]
            );
        }

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn as_user_no_run_as_is_plain() {
        let (bin, args) = as_user("podman", None);
        assert_eq!(bin, "podman");
        assert!(args.is_empty());
    }

    #[test]
    fn as_user_run_as_prefers_user_session() {
        let (bin, args) = as_user("podman", Some("kyo"));
        if crate::detect::which("machinectl").is_some()
            && crate::detect::which("podman").is_some()
        {
            // machinectl demands an absolute binary path and shells into the
            // target user's own session (their slice/cgroup).
            assert_eq!(
                bin,
                crate::detect::which("machinectl").unwrap()
            );
            assert_eq!(
                args,
                vec![
                    "shell".to_string(),
                    "kyo@".to_string(),
                    "--".to_string(),
                    crate::detect::which("podman").unwrap(),
                ]
            );
        } else {
            assert_eq!(bin, "runuser");
            assert!(args.contains(&"kyo".to_string()));
        }
    }
}
