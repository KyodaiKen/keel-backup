//! Run pipeline: destination handling → plan → engines → manifest → retention.
//!
//! Shared by the CLI (`keel run`) and the TUI (`keel`). Side effects only
//! happen after the destination is write-verified. `dry_run` verifies
//! everything (destination, plan, root) but writes no streams.
//!
//! Multi-method: each source declares its methods (e.g. `[btrfs, borg]`).
//! One consistent snapshot is taken per source and every method reads from
//! that same moment — the snapshot is deleted only after the last method has
//! finished (success or failure), per-source failures are isolated.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};

use crate::detect::{euid, free_bytes, hostname, parse_size, which};
use crate::engine::{self, SnapGuard};
use crate::profile::{Destination, DestinationCandidate, Profile, SourceSpec};

/// Events emitted by the pipeline (CLI prints + bars, TUI renders).
#[derive(Debug, Clone)]
pub enum RunEvent {
    /// Free-form info line.
    Note(String),
    /// Destination verdict.
    Destination(String),
    /// Day directory in use.
    DayDir(String),
    /// One source/method is starting.
    SourceStart {
        idx: usize,
        label: String,
        engine: String,
        out_rel: String,
    },
    /// Progress for a stream method: raw bytes streamed, compressed written.
    Progress { idx: usize, raw: u64, written: u64 },
    /// One method finished OK.
    SourceOk { idx: usize, engine: String, note: String },
    /// One source/method failed.
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

/// One method for one source.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum MethodKind {
    Btrfs,
    Borg,
    Tar,
    Squashfs,
    PgDump,
    PodmanSave,
    SystemdState,
}

#[derive(Debug, Clone)]
pub struct Method {
    pub kind: MethodKind,
    /// btrfs:    stream path relative to the day dir (`os/rootfs.zst`);
    /// tar:      tree archive relative to the day dir (`boot/boot.tar.zst`);
    /// squashfs: image relative to the day dir (`home/home.squashfs`);
    /// pg-dump:  dump file (`extra/pg-weather.dump`);
    /// podman:   image tar (`extra/podman.tar`);
    /// systemd:  unit-state yaml (`extra/systemd-state.yaml`);
    /// borg:     the archive name (no `::`).
    pub artifact: String,
}

#[derive(Debug, Clone)]
pub struct PlanItem {
    pub idx: usize,
    pub name: String,
    /// Display string (`/path`, `pg_dump database x`, `podman: img1, img2`, …).
    pub source: String,
    /// The tree path when the source is a tree (snapshot + tree engines read it);
    /// None for pg_dump / podman_save / systemd_state.
    pub tree: Option<PathBuf>,
    /// Exclude patterns (tree sources only).
    pub excludes: Vec<String>,
    /// The profile source spec (pg/podman params at run time).
    pub spec: SourceSpec,
    pub category: String,
    pub methods: Vec<Method>,
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

    let borg_cfg = profile.methods.borg.as_ref();
    let borg_bin = which("borg");

    let mut items = Vec::new();
    for (i, spec) in profile.sources.iter().enumerate() {
        let category = if spec.category().is_empty() {
            "extra".into()
        } else {
            spec.category()
        };
        match spec {
            SourceSpec::Tree { path, methods, excludes, name, .. } => {
                if !path.is_absolute() {
                    bail!("sources[{}].path must be absolute (got {})", i, path.display());
                }
                if !path.exists() {
                    bail!("sources[{}].path does not exist: {}", i, path.display());
                }
                let fs = crate::detect::fs_type(path).unwrap_or_default();

                // Resolve this source's methods.
                let kind_list: Vec<MethodKind> = match methods {
                    None => {
                        // Default: subvolumes get the fast stream, other trees
                        // get a whole-tree archive (tar+zstd).
                        if fs == "btrfs" {
                            vec![MethodKind::Btrfs]
                        } else {
                            vec![MethodKind::Tar]
                        }
                    }
                    Some(names) => {
                        if names.is_empty() {
                            bail!(
                                "sources[{}]: `methods: []` would back the tree up with nothing",
                                i
                            );
                        }
                        names
                            .iter()
                            .map(|n| match n.trim() {
                                "btrfs" | "btrfs-send" => Ok(MethodKind::Btrfs),
                                "borg" => Ok(MethodKind::Borg),
                                "tar" | "tar-zstd" => Ok(MethodKind::Tar),
                                "squashfs" | "squash" => Ok(MethodKind::Squashfs),
                                other => bail!(
                                    "sources[{}]: unknown method {other:?} (valid: btrfs, borg, tar, squashfs)",
                                    i
                                ),
                            })
                            .collect::<Result<_>>()?
                    }
                };

                if kind_list.contains(&MethodKind::Btrfs) && fs != "btrfs" {
                    bail!(
                        "sources[{}]: {} is on '{fs}' — method `btrfs` needs a btrfs subvolume \n\nfor non-btrfs trees use methods: [tar], [borg] and/or [squashfs]",
                        i, path.display()
                    );
                }

                if kind_list.contains(&MethodKind::Tar) {
                    if which("tar").is_none() {
                        bail!("sources[{}]: method `tar` but no `tar` binary on PATH", i);
                    }
                    if which("zstd").is_none() {
                        bail!("sources[{}]: method `tar` but no `zstd` binary on PATH", i);
                    }
                }
                if kind_list.contains(&MethodKind::Squashfs) {
                    if which("mksquashfs").is_none() {
                        bail!(
                            "sources[{}]: method `squashfs` but no `mksquashfs` binary on PATH (squashfs-tools)",
                            i
                        );
                    }
                }
                if kind_list.contains(&MethodKind::Borg) {
                    let bc = match borg_cfg.as_ref() {
                        Some(bc) => bc,
                        None => bail!(
                            "sources[{}]: method `borg` but the profile has no `methods.borg` block \n(repo + passphrase_file are required)",
                            i
                        ),
                    };
                    if borg_bin.is_none() {
                        bail!("sources[{}]: method `borg` but no `borg` binary on PATH", i);
                    }
                    let _ = bc; // settings validated at run time
                }

                let name = name.clone().unwrap_or_else(|| engine::name_for(path));
                let methods_art: Vec<Method> = kind_list
                    .iter()
                    .map(|k| match k {
                        MethodKind::Btrfs => Method {
                            kind: MethodKind::Btrfs,
                            artifact: format!("{category}/{name}.zst"),
                        },
                        MethodKind::Borg => Method {
                            kind: MethodKind::Borg,
                            artifact: crate::borg::archive_name(&name),
                        },
                        MethodKind::Tar => Method {
                            kind: MethodKind::Tar,
                            artifact: format!("{category}/{name}.tar.zst"),
                        },
                        MethodKind::Squashfs => Method {
                            kind: MethodKind::Squashfs,
                            artifact: format!("{category}/{name}.squashfs"),
                        },
                        _ => unreachable!("tree source with a non-tree method"),
                    })
                    .collect();

                items.push(PlanItem {
                    idx: i,
                    name,
                    source: path.display().to_string(),
                    tree: Some(path.clone()),
                    excludes: excludes.clone(),
                    spec: spec.clone(),
                    category,
                    methods: methods_art,
                });
            }
            SourceSpec::PgDump { database, host, .. } => {
                if database.trim().is_empty() {
                    bail!("sources[{}]: pg_dump source needs a non-empty `database`", i);
                }
                if which("pg_dump").is_none() {
                    bail!(
                        "sources[{}]: pg_dump source but no `pg_dump` binary on PATH (postgresql client tools)",
                        i
                    );
                }
                let name = spec.name().unwrap_or_else(|| format!("pg-{database}"));
                items.push(PlanItem {
                    idx: i,
                    name: name.clone(),
                    source: format!(
                        "pg_dump database {}{}",
                        database,
                        host.as_ref().map(|h| format!(" (host {h})")).unwrap_or_default()
                    ),
                    tree: None,
                    excludes: Vec::new(),
                    spec: spec.clone(),
                    category: category.clone(),
                    methods: vec![Method {
                        kind: MethodKind::PgDump,
                        artifact: format!("{category}/{name}.dump"),
                    }],
                });
            }
            SourceSpec::PodmanSave { images, commit, .. } => {
                if images.is_empty() && commit.is_none() {
                    bail!(
                        "sources[{}]: podman_save source needs at least one `images:` entry or a `commit:`",
                        i
                    );
                }
                if which("podman").is_none() {
                    bail!("sources[{}]: podman_save source but no `podman` binary on PATH", i);
                }
                let name = spec.name().unwrap_or_else(|| "podman".into());
                items.push(PlanItem {
                    idx: i,
                    name: name.clone(),
                    source: format!(
                        "podman: {}{}",
                        images.join(", "),
                        commit
                            .as_ref()
                            .map(|c| format!(" (+ commit {} → {})", c.container, c.tag))
                            .unwrap_or_default()
                    ),
                    tree: None,
                    excludes: Vec::new(),
                    spec: spec.clone(),
                    category: category.clone(),
                    methods: vec![Method {
                        kind: MethodKind::PodmanSave,
                        artifact: format!("{category}/{name}.tar"),
                    }],
                });
            }
            SourceSpec::SystemdState { .. } => {
                if which("systemctl").is_none() {
                    bail!("sources[{}]: systemd_state source but no `systemctl` binary on PATH", i);
                }
                let name = spec.name().unwrap_or_else(|| "systemd-state".into());
                items.push(PlanItem {
                    idx: i,
                    name: name.clone(),
                    source: "systemd unit files (list-unit-files)".into(),
                    tree: None,
                    excludes: Vec::new(),
                    spec: spec.clone(),
                    category: category.clone(),
                    methods: vec![Method {
                        kind: MethodKind::SystemdState,
                        artifact: format!("{category}/{name}.yaml"),
                    }],
                });
            }
        }
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
/// keel runs as root, but `run_as` engines exec as the target user and must
/// be able to create their artifact file. A root `create_dir_all` leaves the
/// dir root-owned 0755, so the user process can't write into it — and the
/// failure can be masked (machinectl shell exits 0 even when the inner
/// command fails), surfacing only as an empty/missing archive. Chown the
/// artifact dir to the user; on unknown user or failure the engine's own
/// error will surface instead. (Verified on FELINE, 2026-11-27.)
fn chown_dir_for_user(path: &Path, user: &str) {
    use std::os::unix::fs::chown;
    if let Some((uid, gid)) = crate::detect::user_ids(user) {
        let _ = chown(path, Some(uid), Some(gid));
    }
}

pub fn run(plan: &Plan, profile: &Profile, ev: &EventArc, dry_run: bool) -> Result<()> {
    if euid() != 0 {
        bail!(
            "keel needs root (snapshots, mounting, umount) — re-run as: sudo keel run{}",
            if dry_run { " --dry-run" } else { "" }
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
            for m in &it.methods {
                emit(
                    ev,
                    &RunEvent::SourceStart {
                        idx: it.idx,
                        label: format!("{} [{}]", it.source, describe_method(m, profile)),
                        engine: engine_label(m.kind),
                        out_rel: m.artifact.clone(),
                    },
                );
            }
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
    let mut any_borg_ok = false;
    let mut borg_pass = String::new();
    let mut borg_bootstrapped = false;

    for it in &plan.items {
        // One consistent moment for the whole source, shared by all methods —
        // when the source is a btrfs subvolume. Non-subvolume trees (tar/borg
        // only) are read live; the snapshot is deleted only after the last
        // method has finished (success or failure).
        // One consistent moment for the whole source, shared by all tree
        // methods — when the source is a btrfs subvolume. Non-subvolume trees
        // are read live; non-tree sources (pg/podman/systemd) snapshot nothing.
        // The snapshot is deleted only after the last method has finished
        // (success or failure).
        let snap_res: Option<Result<PathBuf, anyhow::Error>> =
            it.tree.as_ref().map(|t| engine::snapshot(t));
        let snap_opt: Option<PathBuf> = match &snap_res {
            Some(Ok(s)) => Some(s.clone()),
            _ => None,
        };
        let mut guard = SnapGuard::new();
        if let Some(s) = &snap_opt {
            guard.add(s.clone());
        }
        let live_or_snap: PathBuf = match &snap_opt {
            Some(s) => s.clone(),
            None => it.tree.clone().unwrap_or_default(),
        };
        let mut m_entries: Vec<crate::manifest::MethodEntry> = Vec::new();

        for m in &it.methods {
            match m.kind {
                MethodKind::Btrfs => {
                    // btrfs-send requires the CoW snapshot.
                    let snap = match &snap_opt {
                        Some(s) => s,
                        None => {
                            let snap_err = match &snap_res {
                                Some(Err(e)) => e.to_string(),
                                Some(Ok(_)) => unreachable!("snapshot succeeded but not recorded"),
                                None => "no snapshot (source is not a btrfs subvolume?)".to_string(),
                            };
                            let err = format!("{} (method `btrfs` needs a btrfs subvolume)", snap_err);
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "btrfs-send".into(),
                                ok: false,
                                error: Some(err.clone()),
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err });
                            continue;
                        }
                    };
                    let out = day.join(&m.artifact);
                    if let Some(parent) = out.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "btrfs-send".into(),
                            out_rel: m.artifact.clone(),
                        },
                    );
                    let name = it.name.clone();
                    let out_c = out.clone();
                    let res = (|| -> anyhow::Result<engine::SentStream> {
                        engine::send_stream(&engine::SendStreamConfig {
                            name: &name,
                            snapshot: snap,
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
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "btrfs-send".into(),
                                ok: true,
                                error: None,
                                snapshot: s.snapshot.display().to_string(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: Some(s.raw_bytes),
                                size_bytes: Some(s.size_bytes),
                                archive: None,
                                repo: None,
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "btrfs-send".into(),
                                    note: format!(
                                        "{} raw → {} compressed ({})",
                                        engine::human(s.raw_bytes),
                                        engine::human(s.size_bytes),
                                        m.artifact
                                    ),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "btrfs-send".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: snap.display().to_string(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }

                }
                MethodKind::Tar => {
                    let out = day.join(&m.artifact);
                    if let Some(parent) = out.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "tar".into(),
                            out_rel: m.artifact.clone(),
                        },
                    );
                    let name = it.name.clone();
                    let out_c = out.clone();
                    let read_from = live_or_snap.clone();
                    let excludes: Vec<String> = it.excludes.clone();
                    let res = (|| -> anyhow::Result<engine::SentStream> {
                        engine::tar_stream(&engine::TarStreamConfig {
                            name: &name,
                            src: &read_from,
                            excludes: &excludes,
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
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "tar".into(),
                                ok: true,
                                error: None,
                                snapshot: s.snapshot.display().to_string(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: Some(s.raw_bytes),
                                size_bytes: Some(s.size_bytes),
                                archive: None,
                                repo: None,
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "tar".into(),
                                    note: format!(
                                        "{} raw → {} compressed ({})",
                                        engine::human(s.raw_bytes),
                                        engine::human(s.size_bytes),
                                        m.artifact
                                    ),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "tar".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: live_or_snap.display().to_string(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }
                }
                MethodKind::Borg => {
                    let bc = profile.methods.borg.as_ref().expect("validated in build_plan");
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "borg".into(),
                            out_rel: format!("{}::{}", bc.repo.display(), m.artifact),
                        },
                    );
                    let cfg = crate::borg::BorgCreateConfig {
                        repo: bc.repo.clone(),
                        passphrase_file: bc.passphrase_file.clone(),
                        snapshot: live_or_snap.clone(),
                        archive: m.artifact.clone(),
                        compression: bc.compression.clone(),
                        excludes: it.excludes.clone(),
                    };
                    match crate::borg::create(&cfg) {
                        Ok((pass, bootstrapped)) => {
                            any_borg_ok = true;
                            borg_pass = pass;
                            if bootstrapped {
                                borg_bootstrapped = true;
                                emit(
                                    ev,
                                    &RunEvent::Note(format!(
                                        "⚠ NEW borg passphrase generated at {} (0400) — it is the \
                                         only key to this repo alongside the drive; store it safely",
                                        bc.passphrase_file.display()
                                    )),
                                );
                            }
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "borg".into(),
                                ok: true,
                                error: None,
                                snapshot: live_or_snap.display().to_string(),
                                stream_rel: None,
                                raw_bytes: None,
                                size_bytes: None,
                                archive: Some(m.artifact.clone()),
                                repo: Some(bc.repo.display().to_string()),
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "borg".into(),
                                    note: format!("archive {} in {}", m.artifact, bc.repo.display()),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "borg".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: live_or_snap.display().to_string(),
                                stream_rel: None,
                                raw_bytes: None,
                                size_bytes: None,
                                archive: Some(m.artifact.clone()),
                                repo: Some(bc.repo.display().to_string()),
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }
                }
                MethodKind::Squashfs => {
                    let out = day.join(&m.artifact);
                    if let Some(parent) = out.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "squashfs".into(),
                            out_rel: m.artifact.clone(),
                        },
                    );
                    let name = it.name.clone();
                    let out_c = out.clone();
                    let read_from = live_or_snap.clone();
                    let ladders = engine::squashfs_excludes(&it.excludes);
                    let res = (|| -> anyhow::Result<engine::SentStream> {
                        engine::squashfs(&engine::SquashfsConfig {
                            name: &name,
                            src: &read_from,
                            excludes: &ladders,
                            out: &out_c,
                            zstd_level: profile.engines.zstd_level,
                            on_progress: &|raw, written| {
                                emit(ev, &RunEvent::Progress { idx: it.idx, raw, written });
                            },
                        })
                    })();
                    match res {
                        Ok(s) => {
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "squashfs".into(),
                                ok: true,
                                error: None,
                                snapshot: s.snapshot.display().to_string(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: Some(s.raw_bytes),
                                size_bytes: Some(s.size_bytes),
                                archive: None,
                                repo: None,
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "squashfs".into(),
                                    note: format!(
                                        "{} raw → {} imaged ({})",
                                        engine::human(s.raw_bytes),
                                        engine::human(s.size_bytes),
                                        m.artifact
                                    ),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "squashfs".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: read_from.display().to_string(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }
                }
                MethodKind::PgDump => {
                    let (database, user, host, port, run_as) = match &it.spec {
                        SourceSpec::PgDump { database, user, host, port, run_as, .. } => (
                            database.clone(),
                            user.clone(),
                            host.clone(),
                            *port,
                            run_as.clone(),
                        ),
                        _ => unreachable!("pg_dump method without a pg_dump source"),
                    };
                    let out = day.join(&m.artifact);
                    if let Some(parent) = out.parent() {
                        std::fs::create_dir_all(parent)?;
                        if let Some(u) = run_as.as_deref() {
                            chown_dir_for_user(parent, u);
                        }
                    }
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "pg-dump".into(),
                            out_rel: m.artifact.clone(),
                        },
                    );
                    let res = engine::pg_dump(&engine::PgDumpConfig {
                        database: &database,
                        user: user.as_deref(),
                        host: host.as_deref(),
                        port,
                        run_as: run_as.as_deref(),
                        out: &out,
                    });
                    match res {
                        Ok(size) => {
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "pg-dump".into(),
                                ok: true,
                                error: None,
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: Some(size),
                                archive: None,
                                repo: None,
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "pg-dump".into(),
                                    note: format!(
                                        "dump of database {} → {} ({})",
                                        database,
                                        engine::human(size),
                                        m.artifact
                                    ),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "pg-dump".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }
                }
                MethodKind::PodmanSave => {
                    let (images, commit, run_as) = match &it.spec {
                        SourceSpec::PodmanSave { images, commit, run_as, .. } => (
                            images.clone(),
                            commit
                                .as_ref()
                                .map(|c| engine::PodmanCommitSpec {
                                    container: c.container.clone(),
                                    tag: c.tag.clone(),
                                }),
                            run_as.clone(),
                        ),
                        _ => unreachable!("podman_save method without a podman_save source"),
                    };
                    let out = day.join(&m.artifact);
                    if let Some(parent) = out.parent() {
                        std::fs::create_dir_all(parent)?;
                        if let Some(u) = run_as.as_deref() {
                            chown_dir_for_user(parent, u);
                        }
                    }
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "podman-save".into(),
                            out_rel: m.artifact.clone(),
                        },
                    );
                    let res = engine::podman_save(&engine::PodmanSaveConfig {
                        images: &images,
                        commit: commit.as_ref(),
                        run_as: run_as.as_deref(),
                        out: &out,
                    });
                    match res {
                        Ok(size) => {
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "podman-save".into(),
                                ok: true,
                                error: None,
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: Some(size),
                                archive: None,
                                repo: None,
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "podman-save".into(),
                                    note: format!(
                                        "saved image(s) {} → {} ({})",
                                        images.join(", "),
                                        engine::human(size),
                                        m.artifact
                                    ),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "podman-save".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }
                }
                MethodKind::SystemdState => {
                    let out = day.join(&m.artifact);
                    if let Some(parent) = out.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    emit(
                        ev,
                        &RunEvent::SourceStart {
                            idx: it.idx,
                            label: it.source.clone(),
                            engine: "systemd-state".into(),
                            out_rel: m.artifact.clone(),
                        },
                    );
                    let res = engine::systemd_state(&out);
                    match res {
                        Ok(size) => {
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "systemd-state".into(),
                                ok: true,
                                error: None,
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: Some(size),
                                archive: None,
                                repo: None,
                            });
                            emit(
                                ev,
                                &RunEvent::SourceOk {
                                    idx: it.idx,
                                    engine: "systemd-state".into(),
                                    note: format!(
                                        "systemd unit states captured → {} ({})",
                                        engine::human(size),
                                        m.artifact
                                    ),
                                },
                            );
                        }
                        Err(e) => {
                            any_fail = true;
                            m_entries.push(crate::manifest::MethodEntry {
                                engine: "systemd-state".into(),
                                ok: false,
                                error: Some(format!("{e:#}")),
                                snapshot: String::new(),
                                stream_rel: Some(m.artifact.clone()),
                                raw_bytes: None,
                                size_bytes: None,
                                archive: None,
                                repo: None,
                            });
                            emit(ev, &RunEvent::SourceFail { idx: it.idx, err: format!("{e:#}") });
                        }
                    }
                }
            }
        }

        // All methods for this source are done (ok or failed): release the
        // shared anchor snapshot now (or the guard's Drop will on panic).
        guard.consume();
        drop(guard);

        manifest.entries.push(crate::manifest::SourceEntry {
            name: it.name.clone(),
            source: it.source.clone(),
            category: it.category.clone(),
            methods: m_entries,
        });
    }

    // ---- manifest ---------------------------------------------------------
    let mf = day.join("manifest.yaml");
    let all_ok = !manifest.entries.is_empty()
        && manifest
            .entries
            .iter()
            .flat_map(|e| e.methods.iter())
            .all(|m| m.ok);
    manifest.complete = all_ok && !any_fail;
    manifest.write(&mf)?;
    emit(ev, &RunEvent::Manifest(mf.display().to_string()));

    // ---- borg retention (whole shared repo, once per run) ----------------
    if any_borg_ok {
        if let Some(bc) = &profile.methods.borg {
            match crate::borg::prune(
                &bc.repo,
                bc.keep_daily,
                bc.keep_weekly,
                bc.keep_monthly,
                &borg_pass,
            ) {
                Ok(()) => emit(
                    ev,
                    &RunEvent::Retained(format!(
                        "borg prune applied to {}: keep daily {}/weekly {}/monthly {}",
                        bc.repo.display(),
                        bc.keep_daily,
                        bc.keep_weekly,
                        bc.keep_monthly
                    )),
                ),
                Err(e) => emit(
                    ev,
                    &RunEvent::Note(format!(
                        "⚠ borg prune FAILED (archives kept, retention NOT applied): {e:#}"
                    )),
                ),
            }
        }
    }

    // ---- streams retention -------------------------------------------------
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

    if borg_bootstrapped {
        // Re-emit the safety notice at the end, where it is most visible.
        emit(ev, &RunEvent::Note("⚠ this run generated a NEW borg passphrase — see the notice above; the old one (if any) no longer exists".to_string()));
    }

    emit(ev, &RunEvent::Done);

    if any_fail {
        bail!("one or more methods failed — see log (manifest marked incomplete)");
    }
    Ok(())
}

fn engine_label(k: MethodKind) -> String {
    match k {
        MethodKind::Btrfs => "btrfs-send".into(),
        MethodKind::Borg => "borg".into(),
        MethodKind::Tar => "tar".into(),
        MethodKind::Squashfs => "squashfs".into(),
        MethodKind::PgDump => "pg-dump".into(),
        MethodKind::PodmanSave => "podman-save".into(),
        MethodKind::SystemdState => "systemd-state".into(),
    }
}

fn describe_method(m: &Method, profile: &Profile) -> String {
    match m.kind {
        MethodKind::Btrfs => "stream".into(),
        MethodKind::Borg => {
            if let Some(bc) = &profile.methods.borg {
                format!("borg {}", bc.repo.display())
            } else {
                "borg".into()
            }
        }
        MethodKind::Tar => "tar+zstd (whole tree, excludes applied)".into(),
        MethodKind::Squashfs => "squashfs image (zstd, excludes expanded per depth)".into(),
        MethodKind::PgDump => "pg_dump -Fc (custom-format dump)".into(),
        MethodKind::PodmanSave => "podman save (commit first if configured)".into(),
        MethodKind::SystemdState => "systemctl list-unit-files snapshot".into(),
    }
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
