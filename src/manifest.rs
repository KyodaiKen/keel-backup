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

/// One backed-up source.
#[derive(Debug, Clone, Serialize)]
pub struct SourceEntry {
    /// Stream name (e.g. `rootfs`, `home`).
    pub name: String,
    /// Source subvolume/directory on the live system.
    pub source: String,
    /// Category: `os` | `home` | `extra` (day-dir subdirectory).
    pub category: String,
    /// Engine used (e.g. `btrfs-send`).
    pub engine: String,
    /// Snapshot the stream was created from.
    pub snapshot: String,
    /// Stream file, relative to the day dir (e.g. `os/rootfs.zst`).
    pub stream_rel: String,
    /// Raw bytes streamed by the engine.
    pub raw_bytes: u64,
    /// Compressed file size in bytes.
    pub size_bytes: u64,
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
    /// True when every listed source succeeded.
    pub complete: bool,
    /// Backed-up sources.
    pub entries: Vec<SourceEntry>,
    /// Phase 0 restore hint (generic; the generated live-media restore
    /// script arrives in Phase 1).
    pub restore_hint: String,
}

impl RunManifest {
    pub fn new(plan: &Plan, _profile: &Profile) -> Self {
        Self {
            format: "keel/1".into(),
            host: hostname(),
            completed_utc: engine::ts_utc(),
            profile: plan.profile_path.display().to_string(),
            profile_name: plan.profile_name.clone(),
            destination: plan.destination.clone(),
            day_dir: plan.day_dir.display().to_string(),
            complete: false,
            entries: Vec::new(),
            restore_hint: "btrfs send/receive streams: `zstd -d < day/.../os/rootfs.zst | btrfs receive -p /target` (Phase 1 ships a generated live-media restore script)".into(),
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
