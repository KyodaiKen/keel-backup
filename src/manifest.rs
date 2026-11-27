//! Run manifest: the on-disk receipt of one backup day.
//!
//! The manifest is generic by design — no machine-specific facts except what
//! the profile and runtime detection produced. It is what restore tooling
//! and human readers rely on.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::detect::hostname;
use crate::engine;
use crate::plan::Plan;
use crate::profile::Profile;

/// One method result for a source (btrfs stream or borg archive).
#[derive(Debug, Clone, Serialize)]
pub struct MethodEntry {
    /// Engine used: `btrfs-send` or `borg`.
    pub engine: String,
    /// Whether this method succeeded.
    pub ok: bool,
    /// Error text when `ok` is false.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Snapshot the method read from (shared across the source's methods).
    pub snapshot: String,
    // --- btrfs-send ------------------------------------------------------
    /// Stream file, relative to the day dir (e.g. `os/rootfs.zst`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_rel: Option<String>,
    /// Raw bytes streamed by the engine.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_bytes: Option<u64>,
    /// Compressed file size in bytes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    // --- borg ------------------------------------------------------------
    /// Archive name (e.g. `feline-home-2026-11-23-21-58-00`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub archive: Option<String>,
    /// Repo the archive lives in.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repo: Option<String>,
}

/// One backed-up source.
#[derive(Debug, Clone, Serialize)]
pub struct SourceEntry {
    /// Stream name (e.g. `rootfs`, `home`).
    pub name: String,
    /// Source subvolume/directory on the live system.
    pub source: String,
    /// Category: `os` | `home` | `extra` (day-dir subdirectory).
    pub category: String,
    /// Per-method results, in run order.
    pub methods: Vec<MethodEntry>,
}

/// The full receipt for one run.
#[derive(Debug, Serialize)]
pub struct RunManifest {
    /// Manifest format id.
    pub format: String,
    /// Hostname (short).
    pub host: String,
    /// UTC timestamp of completion.
    pub completed_utc: String,
    /// Profile file used.
    pub profile: String,
    /// Profile name.
    pub profile_name: String,
    /// Destination description.
    pub destination: String,
    /// Day directory (absolute).
    pub day_dir: String,
    /// True when every method of every source succeeded.
    pub complete: bool,
    /// Backed-up sources.
    pub entries: Vec<SourceEntry>,
    /// Restore hints (one per engine family present).
    pub restore_hint: String,
}

impl RunManifest {
    pub fn new(plan: &Plan, _profile: &Profile) -> Self {
        let has_borg = plan.items.iter().any(|i| i.methods.iter().any(|m| m.kind == crate::plan::MethodKind::Borg));
        Self {
            format: "keel/2".into(),
            host: hostname(),
            completed_utc: engine::ts_utc(),
            profile: plan.profile_path.display().to_string(),
            profile_name: plan.profile_name.clone(),
            destination: plan.destination.clone(),
            day_dir: plan.day_dir.display().to_string(),
            complete: false,
            entries: Vec::new(),
            restore_hint: if has_borg {
                "btrfs streams: `zstd -dc < day/.../os/rootfs.zst | btrfs receive /target` (target on a btrfs fs; creates /target/<source>); borg archives: `cd /target && borg extract <repo>::<archive>` (extracts into the current directory; paths stored root-relative, e.g. home/kyo/...) or `borg mount <repo> /mnt/point` (read-only, all archives)".into()
            } else {
                "btrfs send/receive streams: `zstd -dc < day/.../os/rootfs.zst | btrfs receive /target` (target on a btrfs fs; creates /target/<source>; Phase 1 ships a generated live-media restore script)".into()
            },
        }
    }

    pub fn write(&self, path: &Path) -> Result<()> {
        let text = serde_yaml::to_string(self)
            .context("serializing manifest to YAML")?;
        std::fs::write(path, text)
            .with_context(|| format!("writing manifest {}", path.display()))?;
        Ok(())
    }
}
