//! btrfs-send engine: CoW snapshot → `btrfs send` → `zstd` → stream file.
//!
//! Progress is real: we count every raw byte that flows through the pipe and
//! poll the output file for the compressed size. No `pv` needed.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{bail, Context, Result};

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

pub struct SendConfig<'a> {
    pub source: &'a Path,
    pub out: &'a Path,
    pub zstd_level: u8,
    /// `None` → `-T0` (auto).
    pub zstd_threads: Option<usize>,
    /// raw_bytes, compressed_so_far
    pub on_progress: &'a dyn Fn(u64, u64),
}

/// Snapshot + send + compress one subvolume. Returns the stream stats.
/// Snapshots are cleaned on any path (guard); on success the anchor snapshot
/// is deleted because the stream is self-contained.
pub fn send(cfg: &SendConfig) -> Result<SentStream> {
    let name = name_for(cfg.source);
    let snap = PathBuf::from(format!("/.keel-{name}-{}", ts_utc()));
    let mut guard = SnapGuard::new();

    // 1) CoW snapshot (consistent moment).
    let so = Command::new("btrfs")
        .args(["subvolume", "snapshot", "-r"])
        .arg(cfg.source)
        .arg(&snap)
        .output()
        .context("failed to spawn `btrfs subvolume snapshot`")?;
    if !so.status.success() {
        let err = String::from_utf8_lossy(&so.stderr);
        bail!("snapshot of {} failed: {}", cfg.source.display(), err.trim());
    }
    guard.add(snap.clone());

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
        bail!("pipe pump thread panicked while sending {}", cfg.source.display());
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

    // Success: the stream is self-contained → drop the anchor snapshot now.
    guard.consume();

    let size = std::fs::metadata(cfg.out).map(|m| m.len()).unwrap_or(0);
    Ok(SentStream {
        name,
        snapshot: snap,
        out: cfg.out.to_path_buf(),
        raw_bytes: raw_total,
        size_bytes: size,
    })
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
    fn civil_date() {
        // 1970-01-01 00:00:00 UTC
        assert_eq!(civil(0), (1970, 1, 1, 0, 0, 0));
        // 2026-01-01 00:00:00 UTC = 20454 days
        assert_eq!(civil(20454 * 86400), (2026, 1, 1, 0, 0, 0));
        // 2026-01-01 13:45:07 UTC
        assert_eq!(civil(20454 * 86400 + 13 * 3600 + 45 * 60 + 7), (2026, 1, 1, 13, 45, 7));
    }
}
