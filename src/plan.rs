//! Run pipeline: destination handling → plan → engines → manifest → retention.
//!
//! Shared by the CLI (`keel run`) and the TUI (`keel`). Side effects only
//! happen after the destination is write-verified. `dry_run` verifies
//! everything (destination, plan, root) but writes no streams.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::detect::{euid, free_bytes, hostname, parse_size};
use crate::engine::{self};
use crate::profile::{Destination, DestinationCandidate, Profile};

/// Events emitted by the pipeline (CLI prints + bars, TUI renders).
#[derive(Debug, Clone)]
pub enum RunEvent {
    /// Free-form info line.
    Note(String),
    /// Destination verdict.
    Destination(String),
    /// Day directory in use.
    DayDir(String),
    /// One source is starting.
    SourceStart {
        idx: usize,
        label: String,
        engine: String,
        out_rel: String,
    },
    /// Progress for a source: raw bytes streamed, compressed bytes written.
    Progress { idx: usize, raw: u64, written: u64 },
    /// One source finished OK.
    SourceOk { idx: usize, raw: u64, size: u64 },
    /// One source failed.
    SourceFail { idx: usize, err: String },
    /// Manifest file written.
    Manifest(String),
    /// Retention applied.
    Retained(String),
    /// Pipeline finished (success).
    Done,
}

/// Shared, thread-safe event sink (CLI handler / TUI state updater).
pub type EventArc = Arc<Mutex<dyn FnMut(&RunEvent) + Send>>;

/// Emit to the sink; a locked/hung UI must never wedge the backup.
pub fn emit(ev: &EventArc, e: &RunEvent) {
    if let Ok(mut f) = ev.lock() {
        f(e);
    }
}

#[derive(Debug, Clone)]
pub struct PlanItem {
    pub idx: usize,
    pub name: String,
    pub source: PathBuf,
    pub category: String,
    pub engine: String,
    /// Relative to the day dir, e.g. `os/rootfs.zst`.
    pub stream_rel: String,
}

#[derive(Debug, Clone)]
pub struct Plan {
    pub profile_path: PathBuf,
    pub profile_name: String,
    pub destination: String,
    pub day_dir: PathBuf,
    pub items: Vec<PlanItem>,
}

/// Build the plan. No side effects (reads profile + /proc only).
pub fn build_plan(profile_path: &Path, profile: &Profile) -> Result<Plan> {
    if profile.sources.is_empty() {
        bail!(
            "profile '{}' lists no sources — nothing to back up (add entries under `sources:`)",
            profile.name
        );
    }
    let host = hostname();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let date = engine::date_utc(now);
    let backups_dir = if profile.destination.backups_dir.is_empty() {
        "backups"
    } else {
        &profile.destination.backups_dir
    };
    let day_dir = profile
        .destination
        .mount_point
        .join(backups_dir)
        .join(&date)
        .join(&host);

    let mut items = Vec::new();
    for (i, src) in profile.sources.iter().enumerate() {
        if !src.path.is_absolute() {
            bail!("source {} is not an absolute path", src.path.display());
        }
        if !src.path.exists() {
            bail!("source {} does not exist", src.path.display());
        }
        let fs = crate::detect::fs_type(&src.path).unwrap_or_default();
        if fs != "btrfs" {
            bail!(
                "source {} is on '{}' — Phase 0 only handles btrfs subvolumes \
                 (tar+zstd / squashfs engines land in Phase 1)",
                src.path.display(),
                fs
            );
        }
        let name = src.name.clone().unwrap_or_else(|| engine::name_for(&src.path));
        let stream_rel = format!("{}/{}.zst", src.category, name);
        items.push(PlanItem {
            idx: i,
            name,
            source: src.path.clone(),
            category: if src.category.is_empty() {
                "extra".into()
            } else {
                src.category.clone()
            },
            engine: "btrfs-send".into(),
            stream_rel,
        });
    }

    Ok(Plan {
        profile_path: profile_path.to_path_buf(),
        profile_name: profile.name.clone(),
        destination: describe_destination(&profile.destination),
        day_dir,
        items,
    })
}

fn describe_destination(d: &Destination) -> String {
    let cands: Vec<String> = d
        .candidates
        .iter()
        .map(|c| match c {
            DestinationCandidate::Uuid { uuid } => format!("uuid {uuid}"),
            DestinationCandidate::Path { path } => format!("path {}", path.display()),
            DestinationCandidate::Device { device } => format!("device {}", device.display()),
            DestinationCandidate::Label { label } => format!("label {label}"),
        })
        .collect();
    format!(
        "{} → {}{}",
        if cands.is_empty() {
            "no candidates (using mount point as-is)".to_string()
        } else {
            cands.join(" | ")
        },
        d.mount_point.display(),
        d.min_free
            .as_deref()
            .map(|m| format!(" (min free {m})"))
            .unwrap_or_default()
    )
}

/// Run the whole pipeline. `ev` receives events as they happen.
pub fn run(plan: &Plan, profile: &Profile, ev: &EventArc, dry_run: bool) -> Result<()> {
    if euid() != 0 {
        bail!(
            "keel needs root (snapshots, mounting, umount) — re-run as: sudo keel run{}{}",
            if dry_run { " --dry-run" } else { "" },
            ""
        );
    }

    // Secrets from the locked-away env file, if configured. Values are never
    // logged; only key count and file path.
    if let Some(envf) = &profile.env_file {
        let p = expand_home(envf);
        let keys = crate::profile::load_env_file(&p)?;
        emit(
            ev,
            &RunEvent::Note(format!(
                "env file loaded: {} ({} secret key{})",
                p.display(),
                keys.len(),
                if keys.len() == 1 { "" } else { "s" }
            )),
        );
    }

    // ---- destination ---------------------------------------------------
    let mounted = mount_destination(&profile.destination, ev)?;
    let mp = profile.destination.mount_point.clone();
    if !mp.is_dir() {
        bail!(
            "destination {} does not exist (mount point missing and no candidate mounted)",
            mp.display()
        );
    }
    std::fs::create_dir_all(&mp)
        .with_context(|| format!("creating mount point {}", mp.display()))?;

    // Write test: prove writability BEFORE any snapshot exists.
    let probe = mp.join(format!(".keel-write-test-{}", std::process::id()));
    std::fs::write(&probe, b"keel").with_context(|| {
        format!(
            "destination {} is not writable (drive unplugged, asleep, or read-only)",
            mp.display()
        )
    })?;
    let _ = std::fs::remove_file(&probe);

    if let Some(min) = &profile.destination.min_free {
        let need = parse_size(min)
            .with_context(|| format!("bad min_free value {min:?} (expected e.g. 50G)"))?;
        let have = free_bytes(&mp).context("statvfs of destination failed")?;
        if have < need {
            bail!(
                "destination has only {} free, profile requires {}",
                engine::human(have),
                engine::human(need)
            );
        }
    }

    let free = free_bytes(&mp).map(engine::human).unwrap_or_else(|| "?".into());
    emit(
        ev,
        &RunEvent::Destination(format!(
            "destination ready: {} (write-verified, {} free)",
            plan.destination, free
        )),
    );

    if dry_run {
        emit(ev, &RunEvent::DayDir(plan.day_dir.display().to_string()));
        for it in &plan.items {
            emit(
                ev,
                &RunEvent::SourceStart {
                    idx: it.idx,
                    label: it.source.display().to_string(),
                    engine: it.engine.clone(),
                    out_rel: it.stream_rel.clone(),
                },
            );
        }
        emit(ev, &RunEvent::Note(format!(
            "dry run: {} source(s) planned — nothing written",
            plan.items.len()
        )));
        if let Some(_w) = mounted {
            let _ = std::process::Command::new("umount").arg(&mp).output();
            emit(ev, &RunEvent::Note(format!("unmounted {}", mp.display())));
        }
        return Ok(());
    }

    // ---- day dir --------------------------------------------------------
    let day = plan.day_dir.clone();
    std::fs::create_dir_all(&day)
        .with_context(|| format!("creating day dir {}", day.display()))?;
    emit(ev, &RunEvent::DayDir(day.display().to_string()));

    // ---- sources --------------------------------------------------------
    let mut manifest = crate::manifest::RunManifest::new(plan, profile);
    let mut any_fail = false;
    for it in &plan.items {
        let out = day.join(&it.stream_rel);
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?;
        }
        emit(
            ev,
            &RunEvent::SourceStart {
                idx: it.idx,
                label: it.source.display().to_string(),
                engine: it.engine.clone(),
                out_rel: it.stream_rel.clone(),
            },
        );

        let out_c = out.clone();
        let res = (|| -> anyhow::Result<engine::SentStream> {
            engine::send(&engine::SendConfig {
                source: &it.source,
                out: &out_c,
                zstd_level: profile.engines.zstd_level,
                zstd_threads: profile.engines.zstd_threads,
                on_progress: &|raw, written| {
                    emit(ev, &RunEvent::Progress { idx: it.idx, raw, written });
                },
            })
        })();

        match res {
            Ok(s) => {
                manifest.entries.push(crate::manifest::SourceEntry {
                    name: s.name.clone(),
                    source: it.source.display().to_string(),
                    category: it.category.clone(),
                    engine: it.engine.clone(),
                    snapshot: s.snapshot.display().to_string(),
                    stream_rel: it.stream_rel.clone(),
                    raw_bytes: s.raw_bytes,
                    size_bytes: s.size_bytes,
                });
                emit(
                    ev,
                    &RunEvent::SourceOk { idx: it.idx, raw: s.raw_bytes, size: s.size_bytes },
                );
            }
            Err(e) => {
                any_fail = true;
                emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
            }
        }
    }

    // ---- manifest ---------------------------------------------------------
    let mf = day.join("manifest.yaml");
    manifest.complete = !any_fail && !manifest.entries.is_empty();
    manifest.write(&mf)?;
    emit(ev, &RunEvent::Manifest(mf.display().to_string()));

    // ---- retention ---------------------------------------------------------
    if let Some(keep) = profile.retention.keep {
        if let Some(msg) = prune_old(plan, keep) {
            emit(ev, &RunEvent::Retained(msg));
        }
    }

    // ---- unmount what we mounted ---------------------------------------------
    if let Some(_w) = mounted {
        let _ = std::process::Command::new("umount").arg(&mp).output();
        emit(ev, &RunEvent::Note(format!("unmounted {}", mp.display())));
    }

    emit(ev, &RunEvent::Done);

    if any_fail {
        bail!("one or more sources failed — see log (manifest marked incomplete)");
    }
    Ok(())
}

fn expand_home(p: &Path) -> PathBuf {
    if let Some(rest) = p.to_str().and_then(|s| s.strip_prefix("~/")) {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    p.to_path_buf()
}

/// Mount the first candidate that works. Returns `Some(())` when keel
/// mounted something (and must unmount it later), `None` when we used an
/// already-mounted `path` candidate (or no candidates at all).
fn mount_destination(dest: &Destination, ev: &EventArc) -> Result<Option<()>> {
    if dest.candidates.is_empty() {
        return Ok(None);
    }
    let mp = dest.mount_point.clone();
    let mut last_err = String::from("no candidates");
    for c in &dest.candidates {
        let spec_desc: Option<(String, String)> = match c {
            DestinationCandidate::Path { path } => {
                if path.is_dir() {
                    emit(ev, &RunEvent::Destination(format!("using pre-mounted path {}", path.display())));
                    return Ok(None);
                }
                last_err = format!("path {} not found", path.display());
                None
            }
            DestinationCandidate::Uuid { uuid } => Some((format!("UUID={uuid}"), format!("uuid {uuid}"))),
            DestinationCandidate::Label { label } => Some((format!("LABEL={label}"), format!("label {label}"))),
            DestinationCandidate::Device { device } => {
                Some((device.to_string_lossy().to_string(), format!("device {}", device.display())))
            }
        };
        let Some((spec, desc)) = spec_desc else { continue };
        match std::process::Command::new("mount").arg(&spec).arg(&mp).output() {
            Ok(o) if o.status.success() => {
                emit(ev, &RunEvent::Destination(format!("mounted {desc} → {}", mp.display())));
                return Ok(Some(()));
            }
            Ok(o) => last_err = format!("{desc}: {}", String::from_utf8_lossy(&o.stderr).trim()),
            Err(e) => last_err = format!("{desc}: {e}"),
        }
    }
    bail!("could not mount any destination candidate — last error: {last_err}")
}

/// Prune old day directories for THIS host, keeping the newest `keep`.
fn prune_old(plan: &Plan, keep: u32) -> Option<String> {
    // day_dir = <backups_root>/YYYY-MM-DD/<host>
    let Some(date_dir) = plan.day_dir.parent() else {
        return None;
    };
    let Some(backups_root) = date_dir.parent() else {
        return None;
    };
    let Ok(rd) = std::fs::read_dir(backups_root) else {
        return None;
    };
    let host = hostname();
    let mut dates: Vec<PathBuf> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .map(|n| {
                    let b: Vec<char> = n.to_string_lossy().chars().collect();
                    b.len() == 10
                        && b[4] == '-'
                        && b[7] == '-'
                        && b[0].is_ascii_digit()
                        && b[5].is_ascii_digit()
                })
                .unwrap_or(false)
        })
        .filter(|p| p.join(&host).is_dir())
        .collect();
    dates.sort();
    let drop_n = dates.len().saturating_sub(keep as usize);
    let drop: Vec<PathBuf> = dates.into_iter().take(drop_n).collect();
    let mut removed = Vec::new();
    for d in &drop {
        let hostdir = d.join(&host);
        let _ = std::fs::remove_dir_all(&hostdir);
        let _ = std::fs::remove_dir(d);
        removed.push(hostdir.display().to_string());
    }
    if removed.is_empty() {
        None
    } else {
        Some(format!(
            "retention: pruned {} old backup(s), e.g. {}",
            removed.len(),
            removed.first().map(String::as_str).unwrap_or("")
        ))
    }
}
