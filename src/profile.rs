//! Profile model: everything about *where* and *what* to back up lives here.
//!
//! Design rule: the binary contains no machine-specific facts. Paths, UUIDs,
//! users, and service names come exclusively from the profile (and runtime
//! detection). The profile is the single source of truth per host.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// A complete backup profile for one host (or one purpose).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// Stable, human-friendly name (used in logs, not in paths).
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    /// Optional file with `KEY=VALUE` secret lines (e.g. `BORG_PASSPHRASE=...`).
    /// Loaded into the process environment before any engine starts.
    /// Values are never printed, logged, or written into the manifest.
    #[serde(default)]
    pub env_file: Option<PathBuf>,
    pub destination: Destination,
    /// What to back up. Empty means "nothing to do" — the profile must say.
    #[serde(default)]
    pub sources: Vec<Source>,
    #[serde(default)]
    pub engines: Engines,
    /// Method registry: settings shared by all sources that use a method.
    #[serde(default)]
    pub methods: Methods,
    #[serde(default)]
    pub retention: Retention,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Destination {
    /// Candidates are tried in order until one is mounted (or usable, for
    /// `path`) and passes the write test. At least one is required to run.
    #[serde(default)]
    pub candidates: Vec<DestinationCandidate>,
    /// Mount point used for the run (created if missing).
    pub mount_point: PathBuf,
    /// Subdirectory under the mount point that holds the day directories.
    #[serde(default = "default_backups_dir")]
    pub backups_dir: String,
    /// Optional lower bound on free space, e.g. `"50G"`. Checked before run.
    #[serde(default)]
    pub min_free: Option<String>,
}

fn default_backups_dir() -> String {
    "backups".into()
}

/// One way to find the backup drive. Tried in profile order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum DestinationCandidate {
    /// Mount by filesystem UUID.
    Uuid { uuid: String },
    /// An already-mounted directory, used directly (not mounted/unmounted by keel).
    Path { path: PathBuf },
    /// Mount the block device directly (whole-disk drives).
    Device { device: PathBuf },
    /// Mount by filesystem label.
    Label { label: String },
}

/// One subvolume/directory to back up.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Source {
    /// Absolute path of the subvolume (btrfs) or directory to back up.
    pub path: PathBuf,
    /// Category used for the day-dir subdirectory: `os`, `home`, or `extra`.
    #[serde(default = "default_category")]
    pub category: String,
    /// Human-readable label for the manifest (defaults to the path).
    #[serde(default)]
    pub name: Option<String>,
    /// Which methods back up this source, in order.
    /// Valid names: `btrfs` (snapshot stream + zstd), `borg` (deduplicated
    /// archive into the configured repo).
    ///
    /// Omitted (or `null`) → legacy behavior: `btrfs` if the path is a btrfs
    /// subvolume, error otherwise. A source can use any number of methods —
    /// e.g. `[btrfs, borg]` produces both a fast stream and an incremental,
    /// deduplicated copy from the *same* consistent snapshot.
    #[serde(default)]
    pub methods: Option<Vec<String>>,
    /// Exclusion patterns (engines that support them; borg patterns such as
    /// `re:(?i).*/[^/]*cache[^/]*(/|$)`). btrfs streams carry the whole
    /// subvolume — excludes do not apply to them.
    #[serde(default)]
    pub excludes: Vec<String>,
}

fn default_category() -> String {
    "extra".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Engines {
    /// zstd compression level for stream compression.
    #[serde(default = "default_zstd_level")]
    pub zstd_level: u8,
    /// zstd thread count; `null` = auto (`-T0`).
    #[serde(default)]
    pub zstd_threads: Option<usize>,
}

fn default_zstd_level() -> u8 {
    6
}

impl Default for Engines {
    fn default() -> Self {
        Self {
            zstd_level: default_zstd_level(),
            zstd_threads: None,
        }
    }
}

/// Shared method settings. A source's `methods:` list names the methods; the
/// per-method settings live here (repo location, passphrase file, …).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Methods {
    /// Borg: deduplicated, incremental archives in one repo (shared across
    /// hosts, like the legacy `start-backup` layout: `<mount>/borg-backups`).
    #[serde(default)]
    pub borg: Option<BorgMethod>,
}

/// Borg method settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BorgMethod {
    /// Repo directory (local path; ssh:// URLs work too — borg handles them).
    /// Created automatically on first run with `repokey-passphrase`
    /// encryption (the safest unattended mode: the repo alone is useless
    /// without the passphrase).
    pub repo: PathBuf,
    /// File holding the repo passphrase (one line, ideally `chmod 600`/`0400`,
    /// readable by the user keel runs as). Reuse the existing passphrase file
    /// (e.g. `/home/kyo/bp.txt`) to open pre-existing repos.
    ///
    /// Safety behavior: if the repo already exists this file MUST exist
    /// (keel never invents a passphrase for a live repo). If the repo does
    /// not exist yet and the file is missing, keel generates a strong random
    /// passphrase, stores it there with `0400`, and prints a loud notice.
    pub passphrase_file: PathBuf,
    /// Borg compression spec, default `zstd,6`.
    #[serde(default = "default_borg_compression")]
    pub compression: String,
    /// Prune policy applied to the repo after a successful run (whole repo,
    /// all hosts — it is a shared, deduplicated store).
    #[serde(default = "default_keep_daily")]
    pub keep_daily: u32,
    #[serde(default = "default_keep_weekly")]
    pub keep_weekly: u32,
    #[serde(default = "default_keep_monthly")]
    pub keep_monthly: u32,
}

fn default_borg_compression() -> String {
    "zstd,6".into()
}
fn default_keep_daily() -> u32 {
    2
}
fn default_keep_weekly() -> u32 {
    4
}
fn default_keep_monthly() -> u32 {
    12
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retention {
    /// Keep at most this many day directories per host (oldest pruned).
    #[serde(default)]
    pub keep: Option<u32>,
}

impl Default for Retention {
    fn default() -> Self {
        Self { keep: None }
    }
}

/// Resolve a profile reference to a file path.
///
/// Order: explicit `--profile` (file path or name) → `$KEEL_PROFILE` (same)
/// → `~/.config/keel/profile.yaml`.
pub fn resolve_path(explicit: Option<&str>) -> Result<PathBuf> {
    let candidates: Vec<PathBuf> = {
        let mut v = Vec::new();
        for ref_ in [explicit, std::env::var("KEEL_PROFILE").ok().as_deref()].into_iter().flatten()
        {
            let p = PathBuf::from(ref_);
            if p.is_file() {
                v.push(p);
            } else {
                if let Some(home) = std::env::var_os("HOME") {
                    for ext in ["yaml", "yml"] {
                        let named = PathBuf::from(&home).join(".config/keel/profiles").join(format!("{ref_}.{ext}"));
                        if named.is_file() {
                            v.push(named);
                        }
                    }
                }
            }
        }
        if v.is_empty() {
            if let Some(home) = std::env::var_os("HOME") {
                let dflt = PathBuf::from(&home).join(".config/keel/profile.yaml");
                if dflt.is_file() {
                    v.push(dflt);
                }
            }
        }
        v
    };
    candidates
        .into_iter()
        .next()
        .context("no profile found — pass --profile <path-or-name>, or create one with `keel suggest > profile.yaml`")
}

/// Load and parse the profile.
pub fn load(explicit: Option<&str>) -> Result<(PathBuf, Profile)> {
    let path = resolve_path(explicit)?;
    let text = std::fs::read_to_string(&path)
        .with_context(|| format!("reading profile {}", path.display()))?;
    let profile: Profile =
        serde_yaml::from_str(&text).with_context(|| format!("parsing profile {}", path.display()))?;
    Ok((path, profile))
}

/// Load an `env_file` (KEY=VALUE lines) into the process environment.
/// Returns the key names (never the values) for logging.
pub fn load_env_file(path: &Path) -> Result<Vec<String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading env file {}", path.display()))?;
    let mut keys = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (k, v) = line
            .split_once('=')
            .with_context(|| format!("bad line in env file {}: {line:?}", path.display()))?;
        let k = k.trim();
        if !k.is_empty() {
            let mut v = v.trim();
            if (v.starts_with('"') && v.ends_with('"')) || (v.starts_with('\'') && v.ends_with('\'')) {
                v = &v[1..v.len() - 1];
            }
            std::env::set_var(k, v);
            keys.push(k.to_string());
        }
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_profile_still_parses() {
        let yaml = r#"
name: legacy
destination:
  mount_point: /mnt/x
sources:
  - path: /
    category: os
    name: rootfs
engines:
  zstd_level: 6
retention:
  keep: 4
"#;
        let p: Profile = serde_yaml::from_str(yaml).unwrap();
        assert!(p.methods.borg.is_none());
        assert!(p.sources[0].methods.is_none());
        assert_eq!(p.engines.zstd_level, 6);
    }

    #[test]
    fn multi_method_profile_parses() {
        let yaml = r#"
name: multi
destination:
  mount_point: /mnt/x
sources:
  - path: /home
    category: home
    name: home
    methods: [btrfs, borg]
    excludes:
      - re:(?i).*/[^/]*cache[^/]*(/|$)
methods:
  borg:
    repo: /mnt/x/borg-backups
    passphrase_file: /root/bp
retention:
  keep: 2
"#;
        let p: Profile = serde_yaml::from_str(yaml).unwrap();
        let m = p.sources[0].methods.as_ref().unwrap();
        assert_eq!(m, &vec!["btrfs".to_string(), "borg".to_string()]);
        let bc = p.methods.borg.as_ref().unwrap();
        assert_eq!(bc.compression, "zstd,6");
        assert_eq!(bc.keep_daily, 2);
        assert_eq!(bc.keep_weekly, 4);
        assert_eq!(bc.keep_monthly, 12);
        assert_eq!(p.sources[0].excludes.len(), 1);
    }
}
