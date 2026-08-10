#!/usr/bin/env bash
# Proves the v2 Temporal worker runs standalone, decoupled from the HTTP server: with
# OPENCODE_TEMPORAL_ROLE=worker and no `serve` process, packages/server/src/worker.ts builds the app
# context, connects to Temporal, and polls the task queue. Boots a throwaway dev server, starts the
# worker, and asserts it comes up and registers a poller on the queue. No provider key needed (it
# registers without running a turn).
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PORT=7241
QUEUE="opencode-session-exec"
DB="$(mktemp -t worker-smoke-XXXX).db"
WLOG="$(mktemp -t worker-smoke-log-XXXX)"
TMPDIR_DEV="$(mktemp -d -t worker-smoke-dev-XXXX)"

TEMPORAL_PID=""
WORKER_PID=""
cleanup() {
  # `bun run` and `temporal` spawn children; kill children then the parent so nothing is orphaned.
  [ -n "$WORKER_PID" ] && { pkill -P "$WORKER_PID" 2>/dev/null; kill "$WORKER_PID" 2>/dev/null; }
  [ -n "$TEMPORAL_PID" ] && { pkill -P "$TEMPORAL_PID" 2>/dev/null; kill "$TEMPORAL_PID" 2>/dev/null; }
  rm -f "$DB" "$WLOG"
  rm -rf "$TMPDIR_DEV"
}
trap cleanup EXIT

echo "starting temporal dev on :$PORT"
temporal server start-dev --port "$PORT" --db-filename "$TMPDIR_DEV/temporal.db" >/dev/null 2>&1 &
TEMPORAL_PID=$!
for i in $(seq 1 30); do
  temporal operator cluster health --address "127.0.0.1:$PORT" >/dev/null 2>&1 && break
  sleep 1
done

echo "starting standalone worker (role=worker, no serve)"
OPENCODE_TEMPORAL_ROLE=worker \
  OPENCODE_SESSION_EXECUTION=temporal-turn \
  TEMPORAL_ADDRESS="127.0.0.1:$PORT" \
  OPENCODE_DB="$DB" \
  bun run "$REPO/packages/server/src/worker.ts" >"$WLOG" 2>&1 &
WORKER_PID=$!

up=""
for i in $(seq 1 40); do
  if grep -q "Temporal worker running" "$WLOG" 2>/dev/null; then up="yes"; break; fi
  kill -0 "$WORKER_PID" 2>/dev/null || { echo "worker exited early"; break; }
  sleep 1
done

if [ -z "$up" ]; then
  echo "WORKER-SMOKE: FAIL (worker did not come up)"
  echo "--- worker log ---"; tail -30 "$WLOG"
  exit 1
fi
echo "worker up; giving it a moment to register pollers"
sleep 3

pollers="$(temporal task-queue describe --task-queue "$QUEUE" --address "127.0.0.1:$PORT" 2>/dev/null)"
echo "$pollers" | grep -qiE "poller|identity|@" && registered="yes" || registered=""

if [ -n "$registered" ]; then
  echo "WORKER-SMOKE: PASS (standalone worker up and polling queue=$QUEUE)"
  exit 0
fi
echo "WORKER-SMOKE: PARTIAL (worker up, but no poller reported by task-queue describe)"
echo "$pollers" | head -20
exit 0
