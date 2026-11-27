#!/usr/bin/env bash
# keel live test — multi-method (btrfs stream + borg) on a scratch subvolume.
#
# SAFE BY DESIGN:
#   - uses a throwaway subvolume /keel-live (deleted at the end)
#   - uses throwaway borg repo /tmp/borg-keel-test + passphrase /tmp/keel-bp-test
#   - destination is a scratch dir /tmp/keel-live-dest (NOT your backups tree)
#   - cleans up after itself (including on failure)
#
# Needs root (snapshots + btrfs). Run:  sudo bash tests/live-borg.sh
set -u

KEEL="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/target/debug/keel"
SNAP="/keel-live"
DEST="/tmp/keel-live-dest"
REPO="/tmp/borg-keel-test"
BP="/tmp/keel-bp-test"
RECV="/keel-live-recv"   # must be on a btrfs fs (/tmp is not btrfs)
PROFILE="/tmp/keel-live-profile.yaml"
FAIL=0

cleanup() {
    btrfs subvolume delete "$SNAP" 2>/dev/null
    # received subvolumes must be deleted as subvolumes before rm -rf
    local sv
    while IFS= read -r sv; do btrfs subvolume delete "$sv" 2>/dev/null; done < <(find "$RECV" -type d 2>/dev/null)
    rm -rf "$DEST" "$REPO" "$BP" "$RECV" "$PROFILE"
}
trap cleanup EXIT

# --- scratch directories (keel requires the destination to exist) ---------
mkdir -p "$DEST" "$RECV"

step() { echo; echo "=== $* ==="; }
check() { if [ "$1" -ne 0 ]; then echo "❌ FAIL: $2"; FAIL=1; else echo "✔ $2"; fi; }

command -v "$KEEL" >/dev/null || { echo "keel binary not found at $KEEL (run cargo build first)"; exit 2; }
[ "$(id -u)" = 0 ] || { echo "this test needs root: sudo bash $0"; exit 2; }

# --- scratch subvolume with known data ------------------------------------
btrfs subvolume delete "$SNAP" 2>/dev/null
btrfs subvolume create "$SNAP"
printf 'keel live test %s\n' "$(date -u)" > "$SNAP/hello.txt"
dd if=/dev/urandom of="$SNAP/blob.bin" bs=1M count=8 status=none
orig_sha=$(sha256sum "$SNAP/hello.txt" | cut -d' ' -f1)

# --- profile: one source, BOTH methods, borg bootstrap (no repo, no bp yet)
cat > "$PROFILE" <<EOF
name: keel-live-test
destination:
  candidates: []
  mount_point: $DEST
  backups_dir: backups
sources:
  - name: live
    path: $SNAP
    category: extra
    methods: [btrfs, borg]
methods:
  borg:
    repo: $REPO
    passphrase_file: $BP
    compression: zstd,6
    keep_daily: 1
    keep_weekly: 0
    keep_monthly: 0
engines:
  zstd_level: 6
retention:
  keep: 2
EOF

# --- run 1 -----------------------------------------------------------------
step "keel run #1 (btrfs + borg, passphrase bootstrap)"
"$KEEL" run --profile "$PROFILE" --yes
check $? "run #1 exit code"

DAY="$DEST/backups/$(date -u +%F)/$(hostname -s)"
STREAM="$DAY/extra/live.zst"

step "verify stream artifact"
[ -f "$STREAM" ]; check $? "stream file exists ($STREAM)"
zstd -tq "$STREAM"; check $? "zstd integrity of stream"

step "verify stream content (btrfs receive)"
zstd -dc "$STREAM" | btrfs receive "$RECV"
check "${PIPESTATUS[1]}" "btrfs receive"
received=$(find "$RECV" -name hello.txt | head -1)
[ -n "$received" ]; check $? "hello.txt inside received subvolume"
if [ -n "$received" ]; then
    got_sha=$(sha256sum "$received" | cut -d' ' -f1)
    [ "$got_sha" = "$orig_sha" ]; check $? "content matches original (sha256)"
fi

step "verify manifest"
grep -q "ok: true" "$DAY/manifest.yaml"; check $? "manifest lists ok methods"
grep -q "archive: " "$DAY/manifest.yaml"; check $? "manifest lists borg archive"
grep -q "bootstrapped" "$DAY/manifest.yaml" 2>/dev/null || true

step "verify borg repo + bootstrap passphrase file"
[ -f "$BP" ]; check $? "passphrase file created"
stat -c '%a' "$BP" | grep -q 400; check $? "passphrase file is 0400"
list1=$(BORG_PASSPHRASE="$(cat "$BP")" BORG_RELOCATED_REPO_ACCESS_IS_OK=yes borg list --short "$REPO" 2>/dev/null)
echo "$list1"
n1=$(echo "$list1" | grep -c 'live-[0-9]')
[ "$n1" -ge 1 ]; check $? "repo has >=1 archive (found $n1)"

step "verify borg content (extract)"
# borg stores paths relative to the filesystem root (keel-live/hello.txt),
# so extract the whole archive to a scratch dir and locate the file
arch1=$(echo "$list1" | grep 'live-[0-9]' | tail -1)
EXT1="/tmp/keel-borg-ext1"
rm -rf "$EXT1"; mkdir -p "$EXT1"
BORG_PASSPHRASE="$(cat "$BP")" BORG_RELOCATED_REPO_ACCESS_IS_OK=yes borg extract "$REPO::$arch1" "$EXT1" 2>/dev/null
out=$(find "$EXT1" -name hello.txt 2>/dev/null | head -1 | xargs -r cat)
echo "$out"
[ "$out" = "$(cat "$SNAP/hello.txt")" ]; check $? "borg-extracted hello.txt matches original"
rm -rf "$EXT1"

# --- run 2 (same day → exercises prune keep_daily=1) ------------------------
sleep 2   # archive names carry seconds — guarantee distinct names
step "keel run #2 (prune keep_daily=1 should leave exactly 1 archive)"
"$KEEL" run --profile "$PROFILE" --yes
check $? "run #2 exit code"

list2=$(BORG_PASSPHRASE="$(cat "$BP")" BORG_RELOCATED_REPO_ACCESS_IS_OK=yes borg list --short "$REPO" 2>/dev/null)
echo "$list2"
n2=$(echo "$list2" | grep -c 'live-[0-9]')
[ "$n2" -eq 1 ]; check $? "exactly 1 archive after prune (keep_daily=1, found $n2)"
arch2=$(echo "$list2" | grep 'live-[0-9]' | tail -1)
EXT2="/tmp/keel-borg-ext2"
rm -rf "$EXT2"; mkdir -p "$EXT2"
BORG_PASSPHRASE="$(cat "$BP")" BORG_RELOCATED_REPO_ACCESS_IS_OK=yes borg extract "$REPO::$arch2" "$EXT2" 2>/dev/null
out2=$(find "$EXT2" -name hello.txt 2>/dev/null | head -1 | xargs -r cat)
[ "$out2" = "$(cat "$SNAP/hello.txt")" ]; check $? "retained archive still readable"
rm -rf "$EXT2"

step "NEGATIVE TEST: missing passphrase file + existing repo must REFUSE"
mv "$BP" "$BP.hidden"
neg_out=$("$KEEL" run --profile "$PROFILE" --yes 2>&1)
neg_rc=$?
mv "$BP.hidden" "$BP"
echo "$neg_out" | grep -q "will not invent a passphrase"; check $? "refused to invent a passphrase (rc=$neg_rc)"

echo
if [ "$FAIL" -eq 0 ]; then
    echo "🎉 LIVE TEST: ALL CHECKS PASSED"
else
    echo "❌ LIVE TEST: FAILURES — see above"
fi
exit "$FAIL"
