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
    pub sources: Vec<SourceSpec>,
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

fn default_category() -> String {
    "extra".into()
}

/// `podman commit <container> <tag>` before the save (live quadlet state).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct PodmanCommit {
    pub container: String,
    pub tag: String,
}

/// One entry under `sources:` — what to back up and, for trees, how.
///
/// Kinds:
/// - `tree` (the default when `path:` is given — legacy profiles without
///   `kind:` still parse): a subvolume or directory, read by its methods.
/// - `pg_dump`: a PostgreSQL database, `pg_dump -Fc` custom-format dump.
/// - `podman_save`: container images to a tar; optionally `commit` a live
///   container first (the quadlet case: commit takes a consistent CoW state).
/// - `systemd_state`: `systemctl list-unit-files` snapshot for bare-metal
///   restore (re-enable the same units on a fresh install).
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum SourceSpec {
    Tree {
        /// Absolute path of the subvolume (btrfs) or directory to back up.
        path: PathBuf,
        /// Category used for the day-dir subdirectory: `os`, `home`, or `extra`.
        #[serde(default = "default_category")]
        category: String,
        /// Human-readable label for the manifest (defaults to the path).
        #[serde(default)]
        name: Option<String>,
        /// Which methods back up this source, in order.
        /// Valid names: `btrfs` (snapshot stream + zstd), `borg` (deduplicated
        /// archive into the configured repo), `tar` (whole-tree tar+zstd
        /// archive; works on any filesystem), `squashfs` (read-only
        /// `.squashfs` image of the tree).
        ///
        /// Omitted (or `null`) → subvolumes use `btrfs`, other trees use `tar`.
        /// A source can use any number of methods — e.g. `[btrfs, borg]`
        /// produces both a fast stream and an incremental, deduplicated copy
        /// from the *same* consistent snapshot.
        #[serde(default)]
        methods: Option<Vec<String>>,
        /// Exclusion patterns — one portable list, applied by tar, borg and
        /// squashfs (keel adapts each engine's pattern language; the portable
        /// form is `**/name`, verified against GNU tar, borg 1.4.5 and
        /// squashfs-tools 4.7.4). btrfs streams carry the whole subvolume —
        /// excludes do not apply to them.
        #[serde(default)]
        excludes: Vec<String>,
    },
    PgDump {
        /// Database name to dump.
        database: String,
        /// DB user (`pg_dump -U`).
        #[serde(default)]
        user: Option<String>,
        /// Server host (`pg_dump -h`); omitted = local socket.
        #[serde(default)]
        host: Option<String>,
        /// Server port (`pg_dump -p`).
        #[serde(default)]
        port: Option<u16>,
        /// OS user to run `pg_dump` as (e.g. `postgres`) via `runuser`;
        /// keel itself runs as root.
        #[serde(default)]
        run_as: Option<String>,
        #[serde(default = "default_category")]
        category: String,
        #[serde(default)]
        name: Option<String>,
    },
    PodmanSave {
        /// Image refs to `podman save` (e.g. `localhost/devenv:44`).
        /// Required unless `commit` is the only source of an image.
        #[serde(default)]
        images: Vec<String>,
        /// First `podman commit <container> <tag>` (consistent CoW state of a
        /// live container, like the legacy script's quadlet save), then save
        /// that tag too.
        #[serde(default)]
        commit: Option<PodmanCommit>,
        /// OS user to run podman as (e.g. `dns`) via `runuser`.
        #[serde(default)]
        run_as: Option<String>,
        #[serde(default = "default_category")]
        category: String,
        #[serde(default)]
        name: Option<String>,
    },
    SystemdState {
        #[serde(default = "default_category")]
        category: String,
        #[serde(default)]
        name: Option<String>,
    },
}

impl SourceSpec {
    pub fn category(&self) -> String {
        match self {
            SourceSpec::Tree { category, .. } => category.clone(),
            SourceSpec::PgDump { category, .. } => category.clone(),
            SourceSpec::PodmanSave { category, .. } => category.clone(),
            SourceSpec::SystemdState { category, .. } => category.clone(),
        }
    }
    pub fn name(&self) -> Option<String> {
        match self {
            SourceSpec::Tree { name, .. } => name.clone(),
            SourceSpec::PgDump { name, .. } => name.clone(),
            SourceSpec::PodmanSave { name, .. } => name.clone(),
            SourceSpec::SystemdState { name, .. } => name.clone(),
        }
    }
}

impl<'de> Deserialize<'de> for SourceSpec {
    /// Accepts the legacy form (a tree source with no `kind:` key) plus the
    /// four explicit kinds. Unknown kinds and unknown fields are rejected.
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error as _;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            #[serde(default)]
            kind: Option<String>,
            // tree
            #[serde(default)]
            path: Option<PathBuf>,
            #[serde(default)]
            methods: Option<Vec<String>>,
            #[serde(default)]
            excludes: Option<Vec<String>>,
            // shared
            #[serde(default)]
            category: Option<String>,
            #[serde(default)]
            name: Option<String>,
            // pg_dump
            #[serde(default)]
            database: Option<String>,
            #[serde(default)]
            user: Option<String>,
            #[serde(default)]
            host: Option<String>,
            #[serde(default)]
            port: Option<u16>,
            #[serde(default)]
            run_as: Option<String>,
            // podman_save
            #[serde(default)]
            images: Option<Vec<String>>,
            #[serde(default)]
            commit: Option<PodmanCommit>,
        }
        let r: Raw = Raw::deserialize(d)?;
        let cat = r.category.unwrap_or_else(default_category);
        let inf = |what: &str| {
            D::Error::custom(format!(
                "source has a {what} field but no matching kind — set kind: {} explicitly",
                what
            ))
        };
        let kind = match r.kind {
            Some(k) => k,
            // No explicit kind: infer from the identifying fields (legacy
            // profiles use `path:` with no kind at all).
            None => {
                if r.path.is_some() && (r.database.is_some() || r.images.is_some() || r.commit.is_some()) {
                    return Err(inf("conflicting"));
                }
                if r.path.is_some() {
                    "tree".into()
                } else if r.database.is_some() {
                    "pg_dump".into()
                } else if r.images.is_some() || r.commit.is_some() {
                    "podman_save".into()
                } else {
                    "systemd_state".into()
                }
            }
        };
        match kind.as_str() {
            "tree" => {
                let path = r.path.ok_or_else(|| D::Error::custom("kind: tree needs `path`"))?;
                Ok(SourceSpec::Tree {
                    path,
                    category: cat,
                    name: r.name,
                    methods: r.methods,
                    excludes: r.excludes.unwrap_or_default(),
                })
            }
            "pg_dump" => {
                let database = r
                    .database
                    .ok_or_else(|| D::Error::custom("kind: pg_dump needs `database`"))?;
                if r.path.is_some() {
                    return Err(inf("tree/`path`"));
                }
                Ok(SourceSpec::PgDump {
                    database,
                    user: r.user,
                    host: r.host,
                    port: r.port,
                    run_as: r.run_as,
                    category: cat,
                    name: r.name,
                })
            }
            "podman_save" => {
                if r.path.is_some() || r.database.is_some() {
                    return Err(inf("tree/`path` or pg_dump/`database`"));
                }
                Ok(SourceSpec::PodmanSave {
                    images: r.images.unwrap_or_default(),
                    commit: r.commit,
                    run_as: r.run_as,
                    category: cat,
                    name: r.name,
                })
            }
            "systemd_state" => {
                if r.path.is_some() || r.database.is_some() || r.images.is_some() || r.commit.is_some() {
                    return Err(inf("tree/pg_dump/podman_save"));
                }
                Ok(SourceSpec::SystemdState { category: cat, name: r.name })
            }
            other => Err(D::Error::custom(format!(
                "unknown source kind {other:?} (valid: tree, pg_dump, podman_save, systemd_state)"
            ))),
        }
    }
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
    /// Created automatically on first run with `repokey`
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
        match &p.sources[0] {
            crate::profile::SourceSpec::Tree { methods, .. } => assert!(methods.is_none()),
            _ => panic!("legacy `path:` source must parse as a tree"),
        }
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
        let (m, ex) = match &p.sources[0] {
            crate::profile::SourceSpec::Tree { methods, excludes, .. } => (methods.clone(), excludes.clone()),
            _ => panic!("multi-method source must parse as a tree"),
        };
        assert_eq!(m, Some(vec!["btrfs".to_string(), "borg".to_string()]));
        let bc = p.methods.borg.as_ref().unwrap();
        assert_eq!(bc.compression, "zstd,6");
        assert_eq!(bc.keep_daily, 2);
        assert_eq!(bc.keep_weekly, 4);
        assert_eq!(bc.keep_monthly, 12);
        assert_eq!(ex.len(), 1);
    }

    #[test]
    fn new_source_kinds_parse() {
        let yaml = r#"
name: kinds
destination:
  mount_point: /mnt/x
sources:
  - kind: pg_dump
    database: weather
    run_as: postgres
    category: extra
  - kind: podman_save
    images: [localhost/devenv:44]
    commit:
      container: dns
      tag: localhost/pihole-kr:backup-1
    run_as: dns
  - kind: systemd_state
  - kind: tree
    path: /boot
    methods: [squashfs]
"#;
        let p: Profile = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(p.sources.len(), 4);
        match &p.sources[0] {
            crate::profile::SourceSpec::PgDump { database, run_as, .. } => {
                assert_eq!(database, "weather");
                assert_eq!(run_as.as_deref(), Some("postgres"));
            }
            _ => panic!("sources[0] must be pg_dump"),
        }
        match &p.sources[1] {
            crate::profile::SourceSpec::PodmanSave { images, commit, run_as, .. } => {
                assert_eq!(images, &vec!["localhost/devenv:44".to_string()]);
                let c = commit.as_ref().unwrap();
                assert_eq!(c.container, "dns");
                assert_eq!(run_as.as_deref(), Some("dns"));
            }
            _ => panic!("sources[1] must be podman_save"),
        }
        match &p.sources[2] {
            crate::profile::SourceSpec::SystemdState { .. } => {}
            _ => panic!("sources[2] must be systemd_state"),
        }
        match &p.sources[3] {
            crate::profile::SourceSpec::Tree { path, methods, .. } => {
                assert_eq!(path.to_str().unwrap(), "/boot");
                assert_eq!(methods.as_deref(), Some(["squashfs".to_string()].as_slice()));
            }
            _ => panic!("sources[3] must be tree"),
        }
    }

    #[test]
    fn unknown_kind_and_fields_rejected() {
        let bad_kind = r#"
name: x
destination:
  mount_point: /m
sources:
  - kind: floppy
    path: /x
"#;
        assert!(serde_yaml::from_str::<Profile>(bad_kind).is_err());
        let bad_field = r#"
name: x
destination:
  mount_point: /m
sources:
  - path: /x
    btrfs_only: true
"#;
        assert!(serde_yaml::from_str::<Profile>(bad_field).is_err());
    }
}
