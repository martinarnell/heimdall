#!/bin/bash
# Phase 0 end-to-end test (TODO_NOMINATIM_PARITY Phase 0).
#
# Proves that place_id is stable across a country rebuild.
#
#   1. Build LU from scratch
#   2. Start the API, hit /search?q=Luxembourg → capture top result's place_id
#   3. Hit /lookup?place_ids={id} → assert resolves to the same record
#   4. Stop the API, wipe the index + checkpoints, rebuild from scratch
#   5. Restart the API, hit /lookup?place_ids={original_id} → assert STILL
#      resolves to the same record (place_id stability across rebuild)
#   6. Re-run /search?q=Luxembourg → assert the top result has the same
#      place_id as step 2 (same record gets the same id deterministically)
#   7. Sanity: an unknown place_id returns an empty array, not an error
#
# Designed to run inside Dockerfile.test-phase0 but works anywhere with
# heimdall-build + heimdall on PATH and write access to $HEIMDALL_TEST_ROOT.

set -euo pipefail

ROOT="${HEIMDALL_TEST_ROOT:-/work}"
COUNTRY="${HEIMDALL_TEST_COUNTRY:-lu}"
CONFIG="$ROOT/data/sources.toml"
STATE="$ROOT/data/rebuild-state.json"
INDEX="$ROOT/data/index-$COUNTRY"
PORT="${HEIMDALL_TEST_PORT:-2400}"
BASE="http://127.0.0.1:$PORT"
QUERY="${HEIMDALL_TEST_QUERY:-Luxembourg}"

cd "$ROOT"

# ANSI helpers — stderr so they don't pollute captured JSON.
log()  { printf '\n\033[1;36m== %s ==\033[0m\n' "$*" >&2; }
ok()   { printf '\033[1;32m  ok\033[0m %s\n' "$*" >&2; }
fail() { printf '\033[1;31m  FAIL\033[0m %s\n' "$*" >&2; cleanup_server; exit 1; }

SERVER_PID=""
cleanup_server() {
    if [ -n "$SERVER_PID" ] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
    fi
    SERVER_PID=""
}
trap cleanup_server EXIT

start_server() {
    log "starting heimdall serve on :$PORT against $INDEX"
    heimdall serve --bind "127.0.0.1:$PORT" --index "$INDEX" \
        > "$ROOT/heimdall-serve.log" 2>&1 &
    SERVER_PID=$!
    # Health-check loop. heimdall does cold-init on first request so a
    # /status check is enough to know the index is loaded.
    for i in $(seq 1 60); do
        if curl -fsS "$BASE/status" >/dev/null 2>&1; then
            ok "server up after ${i}s"
            return
        fi
        sleep 1
    done
    fail "server did not come up within 60s; tail of log:\n$(tail -30 "$ROOT/heimdall-serve.log")"
}

stop_server() {
    log "stopping heimdall serve (pid $SERVER_PID)"
    cleanup_server
    ok "server stopped"
}

# Hit /search?q=$1 and emit the top result's place_id, osm_id, display_name
# as a TSV line. Fails the test if the response has zero results.
top_search_result() {
    local q="$1" file="$ROOT/search.json"
    curl -fsS --get --data-urlencode "q=$q" "$BASE/search" -o "$file"
    local n
    n=$(jq 'length' "$file")
    [ "$n" -ge 1 ] || fail "expected ≥1 search result for '$q', got $n; payload:\n$(cat "$file")"
    jq -r '.[0] | [.place_id, .osm_id, .osm_type, .display_name] | @tsv' "$file"
}

# Hit /lookup?place_ids=$1 and emit the first result's place_id, osm_id,
# display_name as TSV. Fails the test if the response is empty.
lookup_by_place_id() {
    local pid="$1" file="$ROOT/lookup.json"
    curl -fsS --get --data-urlencode "place_ids=$pid" "$BASE/lookup" -o "$file"
    local n
    n=$(jq 'length' "$file")
    [ "$n" -ge 1 ] || fail "expected ≥1 lookup result for place_id=$pid, got $n; payload:\n$(cat "$file")"
    jq -r '.[0] | [.place_id, .osm_id, .osm_type, .display_name] | @tsv' "$file"
}

# Wipe build artefacts so the next rebuild really starts from scratch.
# Phase 4's input-fingerprint planner would otherwise SKIP every phase.
wipe_index() {
    log "wiping $INDEX (full rebuild requested)"
    rm -rf "$INDEX"
    ok "index dir gone"
}

run_rebuild() {
    log "rebuild --country $COUNTRY"
    heimdall-build rebuild \
        --config "$CONFIG" \
        --state-file "$STATE" \
        --country "$COUNTRY" \
        --max-ram 1G --max-disk 5G
    test -f "$INDEX/meta.json" || fail "rebuild finished without meta.json"
    ok "rebuild done; meta.json present"
}

# ── Step 0: clean slate ───────────────────────────────────────────────
log "Step 0: wipe prior test artefacts in $ROOT"
rm -rf "$INDEX" "$ROOT/data/index-${COUNTRY}-tmp" "$STATE" \
       "$ROOT/data/global" "$ROOT/data"/rebuild-report-*.log
ok "test root reset"

# ── Step 1: first build ───────────────────────────────────────────────
log "Step 1: first rebuild"
run_rebuild
start_server

# ── Step 2: capture place_id for $QUERY ───────────────────────────────
log "Step 2: capture top search result for q=$QUERY"
read -r ORIG_PID ORIG_OSM ORIG_OSM_TYPE ORIG_DISPLAY < <(top_search_result "$QUERY")
[ -n "$ORIG_PID" ] && [ "$ORIG_PID" != "0" ] \
    || fail "first /search returned place_id=$ORIG_PID (expected non-zero stable hash); display=$ORIG_DISPLAY"
ok "captured place_id=$ORIG_PID osm_id=$ORIG_OSM osm_type=$ORIG_OSM_TYPE"
ok "  display=\"$ORIG_DISPLAY\""

# ── Step 3: round-trip via /lookup before any rebuild ─────────────────
log "Step 3: /lookup?place_ids=$ORIG_PID resolves to same record"
read -r LOOKUP_PID LOOKUP_OSM LOOKUP_OSM_TYPE LOOKUP_DISPLAY < <(lookup_by_place_id "$ORIG_PID")
[ "$LOOKUP_PID" = "$ORIG_PID" ]               || fail "lookup place_id mismatch: $LOOKUP_PID != $ORIG_PID"
[ "$LOOKUP_OSM" = "$ORIG_OSM" ]               || fail "lookup osm_id mismatch: $LOOKUP_OSM != $ORIG_OSM"
[ "$LOOKUP_OSM_TYPE" = "$ORIG_OSM_TYPE" ]     || fail "lookup osm_type mismatch: $LOOKUP_OSM_TYPE != $ORIG_OSM_TYPE"
[ "$LOOKUP_DISPLAY" = "$ORIG_DISPLAY" ]       || fail "lookup display_name mismatch: '$LOOKUP_DISPLAY' != '$ORIG_DISPLAY'"
ok "round-trip preserves osm_id, osm_type, display_name"

# ── Step 4: stop, wipe, rebuild ───────────────────────────────────────
stop_server
wipe_index
log "Step 4: second rebuild — record positions will be recomputed"
run_rebuild
start_server

# ── Step 5: same place_id still resolves ──────────────────────────────
log "Step 5: /lookup?place_ids=$ORIG_PID after rebuild"
read -r LOOKUP2_PID LOOKUP2_OSM LOOKUP2_OSM_TYPE LOOKUP2_DISPLAY < <(lookup_by_place_id "$ORIG_PID")
[ "$LOOKUP2_PID" = "$ORIG_PID" ]            || fail "post-rebuild place_id mismatch: $LOOKUP2_PID != $ORIG_PID"
[ "$LOOKUP2_OSM" = "$ORIG_OSM" ]            || fail "post-rebuild osm_id mismatch: $LOOKUP2_OSM != $ORIG_OSM"
[ "$LOOKUP2_OSM_TYPE" = "$ORIG_OSM_TYPE" ]  || fail "post-rebuild osm_type mismatch: $LOOKUP2_OSM_TYPE != $ORIG_OSM_TYPE"
[ "$LOOKUP2_DISPLAY" = "$ORIG_DISPLAY" ]    || fail "post-rebuild display mismatch: '$LOOKUP2_DISPLAY' != '$ORIG_DISPLAY'"
ok "place_id $ORIG_PID survives full rebuild — STABILITY VERIFIED"

# ── Step 6: same query gives same place_id ────────────────────────────
log "Step 6: re-run /search?q=$QUERY — top result must have place_id=$ORIG_PID"
read -r NEW_PID NEW_OSM NEW_OSM_TYPE NEW_DISPLAY < <(top_search_result "$QUERY")
[ "$NEW_PID" = "$ORIG_PID" ] || fail "post-rebuild /search top place_id changed: $NEW_PID != $ORIG_PID"
ok "deterministic /search → /search: same record gets place_id=$ORIG_PID"

# ── Step 7: unknown place_id returns empty (not error) ────────────────
log "Step 7: /lookup with unknown place_id returns empty array"
unknown_id=999999999999999999
unknown_file="$ROOT/lookup-unknown.json"
curl -fsS --get --data-urlencode "place_ids=$unknown_id" "$BASE/lookup" -o "$unknown_file"
n=$(jq 'length' "$unknown_file")
[ "$n" = "0" ] || fail "unknown place_id should return empty array, got $n entries"
ok "unknown place_id → empty array"

stop_server
log "Phase 0 e2e: ALL PASSED"
