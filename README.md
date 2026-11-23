# keel

**Profile-driven, distribution-agnostic, bare-metal-capable system backups — written in Rust.**

`keel` detects what your system *can* be backed up with (btrfs subvolumes today,
tar+zstd / squashfs / borg / pg-dump / podman-save in later phases), plans a
consistent snapshot set from your **profile YAML**, and streams compressed,
verified backups to a second drive — with real progress feedback in a TUI
or in the terminal.

No machine-specific facts live in the binary: paths, UUIDs, users, services
come exclusively from your profile file and runtime detection.

## Why the name

The keel is the load-bearing spine of a ship — the thing that keeps it upright
when the sea comes over the side. A bare-metal backup is exactly that for a
system.

## Phase 0 (current)

- Capability registry with per-capability *why* (visible in TUI + `keel doctor`)
- Profile YAML: destination candidates, sources, engines, retention, env file
- Destination auto-mount (uuid / label / device / pre-mounted path) + write test
- btrfs send → zstd streams with **real** byte-counting progress (no `pv`)
- Consistent CoW snapshots, cleaned on every exit path (even Ctrl-C)
- `manifest.yaml` receipt per backup day
- Retention: keep newest N day directories per host
- TUI: capabilities / plan / profile / live log — start a run with Enter
- CLI: `keel run` with `indicatif` counters, `--dry-run`, `-y`

## Later phases

1. tar+zstd & squashfs engines (non-btrfs trees), exclude globs
2. borg (env-file passphrase), pg-dump, podman-save, systemd state
3. GPT/EFI/creds bare-metal dump + **generated live-media restore script**
4. `musl` static binary, packaging

## Quick start

```sh
# 1. See what this system can back up — and why some options are hidden
keel doctor

# 2. Generate a starting profile (btrfs sources pre-filled on this host)
keel suggest > ~/.config/keel/profile.yaml
#    → edit: set destination candidates (uuid/label/device), min_free, retention

# 3. Optional: secrets go in a locked-away env file, never in the profile
echo 'BORG_PASSPHRASE=...' > ~/secrets/keel.env && chmod 600 ~/secrets/keel.env
#    → reference it:  env_file: ~/secrets/keel.env

# 4. Inspect the plan (no writes)
keel plan

# 5. TUI: browse capabilities (press i to see hidden ones + why), plan, profile —
#    then start the run from the log tab with Enter
sudo keel

# 6. …or plain CLI
sudo keel run --dry-run
sudo keel run
```

## Profile sketch

```yaml
name: myhost
# env_file: ~/secrets/keel.env
destination:
  candidates:
    - kind: uuid
      uuid: "1234-abcd-…"        # tried in order until one mounts
  mount_point: /mnt/backup
  backups_dir: backups
  min_free: 50G
sources:
  - path: /
    category: os
    name: rootfs
  - path: /home
    category: home
    name: home
engines:
  zstd_level: 6
retention:
  keep: 4
```

Layout on the drive: `backups/YYYY-MM-DD/<host>/<category>/<name>.zst` + `manifest.yaml`.

## Building

```sh
cargo build --release     # gnu (fastest)
# portable one-file:
rustup target add x86_64-unknown-linux-musl
cargo build --release --target x86_64-unknown-linux-musl
```

Requires at runtime: `btrfs-progs`, `zstd`, `coreutils` (df, find).
Runs as root (snapshots, mounting).

## License

MIT — see [LICENSE](LICENSE).
