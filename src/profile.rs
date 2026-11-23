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
    /// Exclusion globs relative to the source (engines that support them).
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
