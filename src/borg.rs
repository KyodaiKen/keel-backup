//! borg engine: deduplicated, incremental archives from a consistent snapshot.
//!
//! Conventions (intentionally compatible with the legacy `start-backup`
//! repos on the rotating SSD):
//! - one shared repo per mount point, archives from all hosts intermixed
//! - archive names: `<host>-<source>-<UTC ts>` (e.g. `feline-home-2026-11-23-21-58-00`)
//! - passphrase comes from a file (`BORG_PASSPHRASE` in-process env — never
//!   argv, never logs), typically the existing `/home/kyo/bp.txt`
//! - `BORG_RELOCATED_REPO_ACCESS_IS_OK=yes`: the repo lives on a drive that
//!   moves between machines by design (the legacy transfer script sets the
//!   same flag for exactly this reason)
//!
//! Security posture (safest unattended mode):
//! - new repos are created with `repokey`: the key sits with the
//!   repo (on the SSD) and the passphrase lives elsewhere (passphrase file).
//!   Stealing the SSD alone yields nothing.
//! - keel NEVER invents a passphrase for an existing repo — that would be a
//!   silent data-loss footgun. It refuses with a clear error instead.
//! - bootstrap (no repo yet, no passphrase file): a 256-bit random
//!   passphrase is generated, stored `0400`, and a loud notice is printed.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

use crate::detect::hostname;
use crate::engine;

/// `repo/config` exists → borg considers the repo initialized.
pub fn repo_exists(repo: &Path) -> bool {
    repo.join("config").is_file()
}

/// True when the given pattern list would be empty.
pub fn valid_excludes(excludes: &[String]) -> Result<()> {
    for e in excludes {
        if e.trim().is_empty() {
            bail!("empty exclude pattern in profile (excludes: [''])");
        }
    }
    Ok(())
}

/// Load the passphrase from its file, or bootstrap one when (and only when)
/// the repo does not exist yet.
///
/// Returns `(passphrase, bootstrapped)`. The passphrase value is never
/// logged by keel — only the file path.
pub fn load_or_bootstrap_passphrase(file: &Path, repo: &Path) -> Result<(String, bool)> {
    if file.is_file() {
        let text = std::fs::read_to_string(file)
            .with_context(|| format!("reading passphrase file {}", file.display()))?;
        let p = text.trim();
        if p.is_empty() {
            bail!("passphrase file {} is empty", file.display());
        }
        return Ok((p.to_string(), false));
    }
    if repo_exists(repo) {
        bail!(
            "passphrase file {} is missing, but repo {} already exists — \
             keel will not invent a passphrase for a live repo (you would never \
             unlock it). Restore the file (e.g. your bp.txt) or point \
             `methods.borg.passphrase_file` at the right one.",
            file.display(),
            repo.display()
        );
    }
    // Bootstrap: repo and file are both absent → generate a strong passphrase.
    let mut buf = [0u8; 32];
    let mut f = std::fs::File::open("/dev/urandom")
        .context("opening /dev/urandom for passphrase bootstrap")?;
    use std::io::Read as _;
    f.read_exact(&mut buf)
        .context("reading 256 bits from /dev/urandom")?;
    let hex: String = buf.iter().map(|b| format!("{b:02x}")).collect();

    let mut out = std::fs::File::create(file)
        .with_context(|| format!("creating passphrase file {}", file.display()))?;
    use std::io::Write as _;
    use std::os::unix::fs::PermissionsExt as _;
    out.write_all(hex.as_bytes())
        .and_then(|_| out.write_all(b"\n"))
        .and_then(|_| out.flush())
        .with_context(|| format!("writing passphrase file {}", file.display()))?;
    // chmod 0400 — owner (root) read-only.
    std::fs::set_permissions(file, std::fs::Permissions::from_mode(0o400))
        .with_context(|| format!("setting 0400 on {}", file.display()))?;
    Ok((hex, true))
}

/// Base environment shared by every borg invocation here.
fn borg_env(passphrase: &str) -> Vec<(&str, &str)> {
    vec![
        ("BORG_PASSPHRASE", passphrase),
        // The repo is on a drive that moves between machines by design.
        ("BORG_RELOCATED_REPO_ACCESS_IS_OK", "yes"),
        // Defensive parity with the legacy transfer script; harmless for
        // encrypted repos.
        ("BORG_UNKNOWN_UNENCRYPTED_REPO_ACCESS_IS_OK", "yes"),
    ]
}

fn run_borg(args: &[String], passphrase: &str, stderr_label: &str) -> Result<()> {
    let mut cmd = Command::new("borg");
    cmd.args(args);
    for (k, v) in borg_env(passphrase) {
        cmd.env(k, v);
    }
    let o = cmd
        .output()
        .with_context(|| format!("failed to spawn `borg {stderr_label}`"))?;
    if !o.status.success() {
        let err = String::from_utf8_lossy(&o.stderr);
        bail!("`borg {}` failed ({}): {}", stderr_label, o.status.code().map(|c| c.to_string()).unwrap_or_default(), err.trim());
    }
    Ok(())
}

pub struct BorgCreateConfig {
    pub repo: PathBuf,
    pub passphrase_file: PathBuf,
    /// Snapshot to archive (a read-only snapshot of the live subvolume).
    pub snapshot: PathBuf,
    /// Archive name (no `::`), e.g. `feline-home-2026-11-23-21-58-00`.
    pub archive: String,
    /// Compression spec, e.g. `zstd,6`.
    pub compression: String,
    /// borg exclude patterns (`-e`), e.g. `re:(?i).*/[^/]*cache[^/]*(/|$)`.
    pub excludes: Vec<String>,
}

/// `borg init` (only if needed) + `borg create` of one snapshot into the repo.
///
/// Returns `(passphrase, bootstrapped)` so the caller can issue a loud
/// notice when a new passphrase was generated, and reuse the passphrase for
/// the end-of-run `borg prune`.
pub fn create(cfg: &BorgCreateConfig) -> Result<(String, bool)> {
    valid_excludes(&cfg.excludes)?;
    let (passphrase, bootstrapped) = load_or_bootstrap_passphrase(&cfg.passphrase_file, &cfg.repo)?;

    let repo_parent = cfg.repo.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(repo_parent)
        .with_context(|| format!("creating parent for borg repo {}", cfg.repo.display()))?;

    if !repo_exists(&cfg.repo) {
        let mut init_args: Vec<String> = vec!["init".into(), "--encryption=repokey".into()];
        init_args.push(cfg.repo.to_string_lossy().to_string());
        run_borg(&init_args, &passphrase, "init")?;
    }

    let mut args: Vec<String> = vec!["create".to_string(), "--stats".to_string()];
    args.push(format!("-C{}", cfg.compression));
    for e in &cfg.excludes {
        args.push("-e".into());
        args.push(e.clone());
    }
    args.push(format!("{}::{}", cfg.repo.display(), cfg.archive));
    args.push(cfg.snapshot.to_string_lossy().to_string());

    let mut cmd = Command::new("borg");
    cmd.args(&args);
    for (k, v) in borg_env(&passphrase) {
        cmd.env(k, v);
    }
    let o = cmd
        .output()
        .context("failed to spawn `borg create`")?;
    if !o.status.success() {
        let err = String::from_utf8_lossy(&o.stderr);
        let out = String::from_utf8_lossy(&o.stdout);
        bail!(
            "`borg create {}` failed ({}):\n{}",
            cfg.archive,
            o.status.code().map(|c| c.to_string()).unwrap_or_default(),
            if err.trim().is_empty() { out.trim() } else { err.trim() }
        );
    }
    Ok((passphrase, bootstrapped))
}

/// Prune the shared repo to the configured retention. Run once per keel run,
/// after all archives are in (the repo is shared across hosts and sources).
pub fn prune(
    repo: &Path,
    keep_daily: u32,
    keep_weekly: u32,
    keep_monthly: u32,
    passphrase: &str,
) -> Result<()> {
    let args: Vec<String> = vec![
        "prune".into(),
        format!("--keep-daily={keep_daily}"),
        format!("--keep-weekly={keep_weekly}"),
        format!("--keep-monthly={keep_monthly}"),
        repo.to_string_lossy().to_string(),
    ];
    run_borg(&args, passphrase, "prune")?;
    Ok(())
}

/// `host-name-timestamp` archive name (UTC timestamp, matching keel's other
/// artifacts). Borg forbids `:` and `/` in archive names — both are absent
/// from hostnames/source names by construction; defend anyway.
pub fn archive_name(source_label: &str) -> String {
    let host: String = hostname().chars().filter(|c| *c != ':' && *c != '/').collect();
    let name: String = source_label.chars().filter(|c| *c != ':' && *c != '/').collect();
    format!("{host}-{name}-{}", engine::ts_utc())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    #[test]
    fn passphrase_missing_repo_missing_bootstraps() {
        let dir = std::env::temp_dir().join(format!("keel-bp-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("bp");
        let repo = dir.join("repo");
        let (p, bootstrapped) = load_or_bootstrap_passphrase(&file, &repo).unwrap();
        assert!(bootstrapped);
        assert_eq!(p.len(), 64); // 32 bytes → 64 hex chars
        assert!(file.is_file());
        let meta = std::fs::metadata(&file).unwrap();
        use std::os::unix::fs::PermissionsExt as _;
        assert_eq!(meta.permissions().mode() & 0o777, 0o400);
        // Second call must reuse the file, not bootstrap.
        let (p2, boot2) = load_or_bootstrap_passphrase(&file, &repo).unwrap();
        assert!(!boot2);
        assert_eq!(p, p2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn passphrase_missing_repo_exists_refuses() {
        let dir = std::env::temp_dir().join(format!("keel-bp-test2-{}", std::process::id()));
        std::fs::create_dir_all(dir.join("repo")).unwrap();
        std::fs::write(dir.join("repo").join("config"), b"dummy").unwrap();
        let file = dir.join("bp");
        let e = load_or_bootstrap_passphrase(&file, &dir.join("repo")).unwrap_err();
        assert!(e.to_string().contains("will not invent a passphrase"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn passphrase_file_used_verbatim() {
        let dir = std::env::temp_dir().join(format!("keel-bp-test3-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let file = dir.join("bp");
        let mut f = std::fs::File::create(&file).unwrap();
        f.write_all(b"my-secret-pass\n").unwrap();
        let (p, bootstrapped) = load_or_bootstrap_passphrase(&file, &dir.join("repo")).unwrap();
        assert!(!bootstrapped);
        assert_eq!(p, "my-secret-pass"); // trailing newline trimmed
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn archive_names_are_borg_safe() {
        let n = archive_name("home");
        assert!(!n.contains(':'));
        assert!(!n.contains('/'));
        let parts: Vec<&str> = n.split('-').collect();
        assert!(parts.len() >= 6); // host + name + 4 ts fields minimum (name may itself contain '-')
        assert_eq!(parts[parts.len() - 1].len(), 2); // seconds
    }
}
