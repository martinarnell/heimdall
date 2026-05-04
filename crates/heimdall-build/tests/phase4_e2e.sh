#!/bin/bash
# Phase 4 end-to-end test (TODO_REBUILD_MODES Phase 4).
#
# Builds Luxembourg (smallest country with both OSM + Photon ≈ 125 MB total)
# and verifies the per-step diff planner:
#
#   1. Fresh build  → all 4 sentinels written, all source phases RAN
#   2. Re-run as-is → planner reports all SKIP, no work done
#   3. Bump photon.md5 in rebuild-state.json → planner reports
#      photon RUN + cascade to enrich/pack; extract/national stay SKIP
#
# Designed to run inside the Dockerfile.test-phase4 image but works
# anywhere with the heimdall-build binary on PATH and write access to
# /work (or $HEIMDALL_TEST_ROOT).

set -euo pipefail

ROOT="${HEIMDALL_TEST_ROOT:-/work}"
COUNTRY="${HEIMDALL_TEST_COUNTRY:-lu}"
CONFIG="$ROOT/data/sources.toml"
STATE="$ROOT/data/rebuild-state.json"
INDEX="$ROOT/data/index-$COUNTRY"

cd "$ROOT"

# ANSI helpers — use stderr so they don't pollute the planner table that
# we grep below.
log()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*" >&2; }
ok()   { printf '\033[1;32m  ok\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m  FAIL\033[0m %s\n' "$*" >&2; exit 1; }

# ── 0. Clean slate ─────────────────────────────────────────────────────
# Anything left over from a prior run of this script poisons all three
# steps below — the photon.done sentinel might already be patched, the
# state file might point at a stale local file, etc. Always start clean.
log "Step 0: wipe prior test artefacts in $ROOT"
rm -rf "$INDEX" "$ROOT/data/index-${COUNTRY}-tmp" "$STATE" \
       "$ROOT/data/downloads/$COUNTRY" \
       "$ROOT/data/global" "$ROOT/data"/rebuild-report-*.log
ok "test root reset"

# ── 1. Fresh build ─────────────────────────────────────────────────────
log "Step 1: fresh rebuild for $COUNTRY"
heimdall-build rebuild \
    --config "$CONFIG" \
    --state-file "$STATE" \
    --country "$COUNTRY" \
    --max-ram 1G --max-disk 5G

# Sanity: index built, all sentinels in place.
test -f "$INDEX/meta.json" || fail "no meta.json"
test -f "$INDEX/checkpoints/extract.done" || fail "no extract.done"
test -f "$INDEX/checkpoints/photon.done" || fail "no photon.done"
test -f "$INDEX/checkpoints/pack.done" || fail "no pack.done"
ok "all expected sentinels present after fresh build"

# Verify every Phase-4 sentinel carries the format marker.
for s in extract photon pack; do
    fmt=$(jq -r '.inputs._format // "missing"' "$INDEX/checkpoints/$s.done")
    [ "$fmt" = "v4" ] || fail "$s.done missing _format=v4 marker (got: $fmt)"
done
ok "all sentinels carry _format=v4"

# Photon sentinel must record the photon.md5 we just downloaded.
photon_md5_in_ckpt=$(jq -r '.inputs["photon.md5"] // "missing"' "$INDEX/checkpoints/photon.done")
[ "$photon_md5_in_ckpt" != "missing" ] && [ -n "$photon_md5_in_ckpt" ] \
    || fail "photon.done has no photon.md5 input"
ok "photon.done records photon.md5=$photon_md5_in_ckpt"

# ── 2. Re-run with no source change → all SKIP ─────────────────────────
log "Step 2: re-run, expecting --show-plan all SKIP"
plan_out=$(heimdall-build rebuild \
    --config "$CONFIG" --state-file "$STATE" \
    --country "$COUNTRY" --show-plan 2>&1 || true)
# Strip ANSI escapes + tracing prefix so plain grep sees the plan table.
plan_clean=$(echo "$plan_out" | sed -E $'s/\\x1b\\[[0-9;]*m//g')
echo "$plan_clean" >&2

# Each plan line looks like:
#   2026-05-04T...Z INFO heimdall_build::rebuild:     extract        SKIP  ...
# After ANSI stripping we can match on "  <phase>   RUN|SKIP".
phase_line() {
    echo "$plan_clean" | grep -E "^\S+\s+INFO\s+\S+:\s+$1\s" || true
}
phase_decision() {
    phase_line "$1" | sed -E 's/.*: +[a-z_]+ +([A-Z]+) +.*/\1/'
}

ran_count=$(echo "$plan_clean" | grep -cE ': +(extract|national|places_source|photon|enrich|pack) +RUN +' || true)
if [ "$ran_count" -ne 0 ]; then
    fail "expected 0 RUN phases on idempotent re-run, found $ran_count"
fi
ok "all phases SKIP on idempotent re-run"

# ── 3. Mutate the sentinel → only photon (+ enrich + pack) RUN ────────
# We'd LIKE to mutate rebuild-state.json (mirroring what happens when the
# server-side photon.md5 changes), but check_country_changed re-fetches
# the live md5 and overwrites our patched value. So instead simulate the
# inverse: rewind photon.done's recorded md5 to a stale sentinel. That
# represents "the index was built against an older md5; the world has
# moved on" — exactly what the planner is supposed to detect.
log "Step 3: rewind photon.done's photon.md5, expect photon RUN"
ckpt="$INDEX/checkpoints/photon.done"
test -f "$ckpt" || fail "no $ckpt to mutate"
jq '.inputs["photon.md5"] = "STALE_MD5_FROM_PRIOR_BUILD"' "$ckpt" \
    > "$ckpt.tmp" && mv "$ckpt.tmp" "$ckpt"
ok "photon.done rewound to stale md5"

plan_out=$(heimdall-build rebuild \
    --config "$CONFIG" --state-file "$STATE" \
    --country "$COUNTRY" --show-plan 2>&1 || true)
plan_clean=$(echo "$plan_out" | sed -E $'s/\\x1b\\[[0-9;]*m//g')
echo "$plan_clean" >&2

phase_line() {
    echo "$plan_clean" | grep -E "^\S+\s+INFO\s+\S+:\s+$1\s" || true
}
phase_decision() {
    phase_line "$1" | sed -E 's/.*: +[a-z_]+ +([A-Z]+) +.*/\1/'
}

extract_dec=$(phase_decision extract)
national_dec=$(phase_decision national)
photon_dec=$(phase_decision photon)
enrich_dec=$(phase_decision enrich)
pack_dec=$(phase_decision pack)

[ "$extract_dec" = "SKIP" ]  || fail "extract should SKIP, got '$extract_dec'"
[ "$national_dec" = "SKIP" ] || fail "national should SKIP (LU has no national), got '$national_dec'"
[ "$photon_dec" = "RUN" ]    || fail "photon should RUN (md5 changed), got '$photon_dec'"
[ "$enrich_dec" = "RUN" ]    || fail "enrich should RUN (cascade), got '$enrich_dec'"
[ "$pack_dec" = "RUN" ]      || fail "pack should RUN (cascade), got '$pack_dec'"

ok "plan table: extract SKIP, photon RUN, enrich+pack RUN (cascade)"

# Verify the reason mentions photon.md5 specifically.
reason=$(phase_line photon | sed -E 's/.*: +photon +RUN +(.*)/\1/')
echo "    photon RUN reason: $reason" >&2
[[ "$reason" == *"photon.md5"* ]] || fail "photon RUN reason should cite photon.md5; got: $reason"
ok "photon RUN reason cites the changed input"

log "Phase 4 e2e: ALL PASSED"
