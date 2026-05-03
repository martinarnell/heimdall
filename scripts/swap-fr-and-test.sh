#!/usr/bin/env bash
# Stop dev API, switch data-dev/index-fr to point to the freshly rebuilt
# /root/heimdall/dev/data/index-fr, restart dev API on :2400, run France
# test suite.
set -u

DEV=/root/heimdall/dev
NEW_INDEX="$DEV/data/index-fr"

if [[ ! -f "$NEW_INDEX/meta.json" ]]; then
  echo "FAIL: rebuild incomplete — $NEW_INDEX/meta.json missing"
  ls "$NEW_INDEX/" 2>/dev/null
  exit 1
fi

echo "== rebuilt index files =="
ls -la "$NEW_INDEX/" | head -20

# Stop old API
tmux kill-session -t heimdall 2>/dev/null
sleep 1

# Update the symlink
ln -sfn "$NEW_INDEX" "$DEV/data/data-dev/index-fr"
ls -la "$DEV/data/data-dev/" | grep index-fr

# Restart API
tmux new-session -d -s heimdall -c "$DEV" "$DEV/target/release/heimdall serve --bind 127.0.0.1:2400 --index $DEV/data/data-dev/index-fr --index $DEV/data/data-dev/index-de --index $DEV/data/data-dev/index-gb 2>&1 | tee /tmp/api-dev-fr.log"
sleep 7
echo
echo "== API boot log =="
tail -20 /tmp/api-dev-fr.log

echo
echo "== test suite =="
"$DEV/scripts/test-france.sh" 2>&1 | tail -60
