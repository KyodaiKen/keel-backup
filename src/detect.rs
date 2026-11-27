//! Capability registry: what THIS system can back up, and why.
//!
//! Every entry carries an applicability verdict plus a human-readable reason
//! and evidence. The TUI dropdown shows only the applicable ones; the "i"
//! button reveals the rest with their reasons. The CLI does the same via
//! `keel doctor`.

use std::path::Path;

/// One backup capability as seen by the current system.
#[derive(Debug, Clone)]
pub struct Capability {
    /// Stable id, e.g. `btrfs-send`.
    pub id: &'static str,
    /// Human title.
    pub title: String,
    /// True when this system could use this capability.
    pub applicable: bool,
    /// Why it is applicable / hidden (one line, shown in the "i" view).
    pub reason: String,
    /// Concrete evidence (fstypes, binary paths, mounts).
    pub evidence: Vec<String>,
    /// True when the keel engine for it is implemented in this version.
    /// Applicable-but-unimplemented engines are visible but not selectable.
    pub implemented: bool,
}

/// Filesystem type of the mount containing `path` (from /proc/mounts).
pub fn fs_type(path: &Path) -> Option<String> {
    let mounts = std::fs::read_to_string("/proc/mounts").ok()?;
    let mut best: Option<(usize, String)> = None;
    let path_str = path.to_str()?;
    for line in mounts.lines() {
        let cols: Vec<&str> = line.split_whitespace().collect();
        if cols.len() < 3 {
            continue;
        }
        let mnt = cols[1];
        let contained = if mnt == "/" {
            true
        } else {
            path_str == mnt || path_str.starts_with(&format!("{mnt}/"))
        };
        if !contained {
            continue;
        }
        let cand = (mnt.len(), cols[2].to_string());
        match &best {
            Some((len, _)) if *len >= cand.0 => {}
            _ => best = Some(cand),
        }
    }
    best.map(|(_, fs)| fs)
}

/// True when `path` is its own mount point (a separate partition/subvolume).
pub fn is_mountpoint(path: &str) -> bool {
    let mounts = match std::fs::read_to_string("/proc/mounts") {
        Ok(m) => m,
        Err(_) => return false,
    };
    mounts
        .lines()
        .any(|l| l.split_whitespace().nth(1) == Some(path))
}

/// Locate a binary on PATH.
pub fn which(bin: &str) -> Option<String> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let cand = dir.join(bin);
        if cand.is_file() {
            return Some(cand.to_string_lossy().to_string());
        }
    }
    None
}

/// The effective UID (from /proc/self/status; Linux).
pub fn euid() -> u32 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("Uid:") {
            if let Some(t) = rest.split_whitespace().next() {
                if let Ok(v) = t.parse() {
                    return v;
                }
            }
        }
    }
    u32::MAX
}

/// Hostname (short).
pub fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::process::Command::new("hostname")
                .arg("-s")
                .output()
                .ok()
                .filter(|o| o.status.success())
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        })
        .unwrap_or_else(|| "unknown-host".into())
}

/// Free bytes on the filesystem containing `path` (df, bytes).
pub fn free_bytes(path: &Path) -> Option<u64> {
    let out = std::process::Command::new("df")
        .args(["-B1", "--output=avail"])
        .arg(path)
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout)
        .lines()
        .nth(1)?
        .trim()
        .parse()
        .ok()
}

/// Parse a human size like `"50G"`, `"1T"`, `"512M"`, `"2gib"`, or plain bytes.
pub fn parse_size(s: &str) -> Option<u64> {
    let s = s.trim().trim_end_matches(['b', 'B']).trim_end_matches(['i', 'I']);
    let (num_part, mult) = match s.chars().last() {
        Some(c) if c.is_ascii_alphabetic() => {
            let m = match c {
                'K' | 'k' => 1 << 10,
                'M' | 'm' => 1 << 20,
                'G' | 'g' => 1 << 30,
                'T' | 't' => 1 << 40,
                'P' | 'p' => 1 << 50,
                _ => return None,
            };
            (&s[..s.len() - c.len_utf8()], m)
        }
        _ => (s, 1),
    };
    let n: u64 = num_part.parse().ok()?;
    Some(n * mult)
}

/// The full capability verdict for the current system.
pub fn registry() -> Vec<Capability> {
    let mut caps = Vec::new();

    let root_fs = fs_type(Path::new("/"));
    let home_fs = fs_type(Path::new("/home"));
    let btrfs_bin = which("btrfs");
    let zstd_bin = which("zstd");
    let tar_bin = which("tar");
    let squash_bin = which("mksquashfs");
    let borg_bin = which("borg");
    let pg_bin = which("pg_dump");
    let podman_bin = which("podman");
    let sfdisk_bin = which("sfdisk");
    let root = euid() == 0;

    let any_btrfs = matches!(root_fs.as_deref(), Some("btrfs")) || matches!(home_fs.as_deref(), Some("btrfs"));

    caps.push(Capability {
        id: "btrfs-send",
        title: "btrfs send/receive — subvolume streams".into(),
        applicable: any_btrfs && btrfs_bin.is_some() && zstd_bin.is_some() && root,
        reason: if any_btrfs && btrfs_bin.is_some() && zstd_bin.is_some() && root {
            format!("btrfs on /: {}, /home: {}; tools present",
                    matches!(root_fs.as_deref(), Some("btrfs")),
                    matches!(home_fs.as_deref(), Some("btrfs")))
        } else if !any_btrfs {
            "no btrfs mounts on this system".into()
        } else if !root {
            "btrfs found but keel needs root to snapshot (re-run via sudo)".into()
        } else {
            "btrfs tools or zstd missing".into()
        },
        evidence: vec![
            format!("/ fstype = {:?}", root_fs),
            format!("/home fstype = {:?}", home_fs),
            format!("btrfs = {:?}", btrfs_bin),
            format!("zstd = {:?}", zstd_bin),
            format!("euid = {}", euid()),
        ],
        implemented: true,
    });

    caps.push(Capability {
        id: "tar-zstd",
        title: "tar + zstd — whole-tree archive".into(),
        applicable: tar_bin.is_some() && zstd_bin.is_some(),
        reason: if tar_bin.is_some() && zstd_bin.is_some() {
            "fallback engine for non-COW filesystems (ext4, xfs without reflinks…)".into()
        } else {
            "tar or zstd missing".into()
        },
        evidence: vec![format!("tar = {:?}", tar_bin), format!("zstd = {:?}", zstd_bin)],
        implemented: true,
    });

    caps.push(Capability {
        id: "squashfs",
        title: "mksquashfs — read-only image".into(),
        applicable: squash_bin.is_some(),
        reason: if squash_bin.is_some() {
            "image engine for home/extra payloads".into()
        } else {
            "mksquashfs missing".into()
        },
        evidence: vec![format!("mksquashfs = {:?}", squash_bin)],
        implemented: false,
    });

    caps.push(Capability {
        id: "borg",
        title: "borg — deduplicated, incremental archives".into(),
        applicable: borg_bin.is_some(),
        reason: if borg_bin.is_some() {
            "repo + passphrase file configured per profile (methods.borg); passphrase is never hardcoded".into()
        } else {
            "borg binary missing".into()
        },
        evidence: vec![format!("borg = {:?}", borg_bin)],
        implemented: true,
    });

    caps.push(Capability {
        id: "pg-dump",
        title: "pg_dump -Fc — PostgreSQL".into(),
        applicable: pg_bin.is_some(),
        reason: if pg_bin.is_some() {
            "databases are listed in the profile".into()
        } else {
            "pg_dump missing (no PostgreSQL tools found)".into()
        },
        evidence: vec![format!("pg_dump = {:?}", pg_bin)],
        implemented: false,
    });

    caps.push(Capability {
        id: "podman-save",
        title: "podman commit+save — container state".into(),
        applicable: podman_bin.is_some(),
        reason: if podman_bin.is_some() {
            "containers are listed in the profile".into()
        } else {
            "podman missing".into()
        },
        evidence: vec![format!("podman = {:?}", podman_bin)],
        implemented: false,
    });

    caps.push(Capability {
        id: "gpt",
        title: "GPT / bare-metal layout dump".into(),
        applicable: sfdisk_bin.is_some(),
        reason: if sfdisk_bin.is_some() {
            "partition table + drive metadata for bare-metal restore".into()
        } else {
            "sfdisk missing".into()
        },
        evidence: vec![format!("sfdisk = {:?}", sfdisk_bin)],
        implemented: false,
    });

    caps.push(Capability {
        id: "efi",
        title: "UEFI / boot partition archive".into(),
        applicable: Path::new("/boot/efi").is_dir() || Path::new("/boot").is_dir(),
        reason: if Path::new("/boot/efi").is_dir() {
            "/boot/efi present".into()
        } else if Path::new("/boot").is_dir() {
            "/boot present (no /boot/efi found)".into()
        } else {
            "no /boot directory".into()
        },
        evidence: vec![
            format!("/boot/efi = {:?}", Path::new("/boot/efi").is_dir()),
            format!("/boot = {:?}", Path::new("/boot").is_dir()),
        ],
        implemented: false,
    });

    caps.push(Capability {
        id: "creds",
        title: "credentials snapshot (shadow, sudoers.d, machine-id)".into(),
        applicable: Path::new("/etc/shadow").exists(),
        reason: if Path::new("/etc/shadow").exists() {
            "/etc/shadow present (root can read it)".into()
        } else {
            "no /etc/shadow found".into()
        },
        evidence: vec![format!("/etc/shadow = {:?}", Path::new("/etc/shadow").exists())],
        implemented: false,
    });

    caps
}

/// Btrfs subvolumes under `roots` discovered by inode (stable across
/// btrfs-progs versions, unlike parsing `btrfs subvolume list`).
pub fn find_nested_subvolumes(roots: &[&str]) -> Vec<String> {
    let mut out = std::process::Command::new("find");
    let mut args: Vec<String> = Vec::new();
    for r in roots {
        args.push(r.to_string());
    }
    args.extend(["-xdev", "-mindepth", "1", "-type", "d", "-inum", "256"].iter().map(|s| s.to_string()));
    out.args(&args);
    let Ok(o) = out.output() else {
        return Vec::new();
    };
    // NOTE: find exits non-zero on unreadable subdirs (normal, e.g. as non-root);
    // stdout still contains the found subvolumes, so we ignore the status.
    String::from_utf8_lossy(&o.stdout)
        .lines()
        .map(|l| l.trim().to_string())
        .filter(|l| !l.is_empty())
        .filter(|l| {
            let name = l.rsplit('/').next().unwrap_or(l);
            !name.starts_with(".keel-") && !name.starts_with(".btrbk-") && !name.contains("backup_snapshot")
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_sizes() {
        assert_eq!(parse_size("50G"), Some(50 * (1 << 30)));
        assert_eq!(parse_size("1T"), Some(1 << 40));
        assert_eq!(parse_size("512M"), Some(512 * (1 << 20)));
        assert_eq!(parse_size("4096"), Some(4096));
        assert_eq!(parse_size("2gib"), Some(2 * (1 << 30)));
        assert_eq!(parse_size("bogus"), None);
    }

    #[test]
    fn fs_type_root() {
        assert!(fs_type(Path::new("/")).is_some());
        assert!(fs_type(Path::new("/definitely/not/mounted")).is_some());
    }
}
