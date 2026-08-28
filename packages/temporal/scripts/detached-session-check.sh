#!/usr/bin/env bash
# Proves the claim a durable session is supposed to make: it belongs to the deployment, not to
# whoever started it. One worker, two serve processes, one shared store, and a client that is only
# ever a client.
#
#   1. serve A starts a turn, then A is killed while a tool is still running
#   2. the turn finishes anyway, on a worker that is a separate process
#   3. serve B, which never saw the session, reports it running and replays the whole transcript
#   4. `session start` hands over a prompt and returns, holding no terminal
#   5. `session watch` follows that turn live from a cold client and stops when the turn stops
#
# Needs: bun, the temporal CLI, and an OpenAI key. Nothing here is a unit test; it is the evidence
# for a claim that only shows up across processes.
#
# Usage: OPENAI_API_KEY=... packages/temporal/scripts/detached-session-check.sh

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
OC="$ROOT/packages/opencode/src/index.ts"
RUN="${RUN_DIR:-/private/tmp/opencode-l3}"
PORT_TEMPORAL="${PORT_TEMPORAL:-7240}"
PORT_A="${PORT_A:-4610}"
PORT_B="${PORT_B:-4611}"
MODEL="${MODEL:-openai/gpt-5-mini}"

fails=0
ok()   { printf 'PASS %s\n' "$1"; }
bad()  { printf 'FAIL %s   (%s)\n' "$1" "${2:-}"; fails=$((fails + 1)); }

pids=()
cleanup() {
  for pid in "${pids[@]:-}"; do
    [ -n "$pid" ] || continue
    kill -9 $(pgrep -P "$pid" 2>/dev/null) "$pid" 2>/dev/null
  done
}
trap cleanup EXIT

[ -n "${OPENAI_API_KEY:-}" ] || { echo "set OPENAI_API_KEY"; exit 1; }

rm -rf "$RUN"; mkdir -p "$RUN/proj" "$RUN/logs"
git -C "$RUN/proj" init -q
echo hello > "$RUN/proj/README.md"
git -C "$RUN/proj" add -A
git -C "$RUN/proj" -c user.email=a@b.c -c user.name=t commit -qm init

export OPENCODE_SESSION_EXECUTION=temporal
export TEMPORAL_ADDRESS="127.0.0.1:$PORT_TEMPORAL"
# One store both serves and the worker read. This is what makes any process able to answer for any
# session; without it a session belongs to the host holding its file.
export OPENCODE_DB="$RUN/shared.db"
export OPENCODE_TEMPORAL_STEPPED=1
# A stored password wins over the environment for the v2 serve, so a script that invents one gets
# 401 on every call. Take what the server will actually be asking for.
STORED="${XDG_STATE_HOME:-$HOME/.local/state}/opencode/password"
if [ -f "$STORED" ]; then
  OPENCODE_SERVER_PASSWORD="$(cat "$STORED")"
else
  OPENCODE_SERVER_PASSWORD="${OPENCODE_SERVER_PASSWORD:-l3-check}"
fi
export OPENCODE_SERVER_PASSWORD

temporal server start-dev --port "$PORT_TEMPORAL" --ui-port $((PORT_TEMPORAL + 1000)) --log-level warn \
  > "$RUN/logs/temporal.log" 2>&1 &
pids+=($!)
sleep 6

OPENCODE_TEMPORAL_ROLE=worker bun run "$ROOT/packages/server/src/worker.ts" > "$RUN/logs/worker.log" 2>&1 &
worker=$!; pids+=($worker)

cd "$RUN/proj"
OPENCODE_TEMPORAL_ROLE=client bun run "$ROOT/packages/cli/src/index.ts" serve --port "$PORT_A" \
  > "$RUN/logs/serveA.log" 2>&1 &
serveA=$!; pids+=($serveA)
OPENCODE_TEMPORAL_ROLE=client bun run "$ROOT/packages/cli/src/index.ts" serve --port "$PORT_B" \
  > "$RUN/logs/serveB.log" 2>&1 &
pids+=($!)

A="http://127.0.0.1:$PORT_A"
B="http://127.0.0.1:$PORT_B"
AUTH="opencode:$OPENCODE_SERVER_PASSWORD"

# Bounded, because a fixed sleep is either a slow script or a flaky one. Both serves boot a whole
# application context, which on a cold module cache is not quick.
# Answering at all is not enough: an unauthorized answer is still an answer, and treating it as
# ready turns a credentials problem into a confusing timeout later.
wait_for() {
  for _ in $(seq 1 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' -u "$AUTH" "$1/api/session")" = "200" ] && return 0
    sleep 2
  done
  return 1
}
wait_for "$A" && wait_for "$B" || { echo "serves never came up; see $RUN/logs"; exit 1; }

# The id of the session, not of anything nested in it: the field is read off the first line of the
# document, so a later `"id"` (a model, a message) cannot be picked up instead.
session_id() { sed -n 's/^{"data":{"id":"\([^"]*\)".*/\1/p' | head -1; }

# --- 1. a turn started on serve A, long enough to still be running when A dies
created=$(curl -s -u "$AUTH" -X POST "$A/api/session" -H 'content-type: application/json' \
  -d "{\"directory\":\"$RUN/proj\"}")
sid=$(printf '%s' "$created" | session_id)
[ -n "$sid" ] && ok "serve A created a session" || { bad "serve A created a session" "$created"; exit 1; }

provider=${MODEL%%/*}; model=${MODEL#*/}
curl -s -o /dev/null -u "$AUTH" -X POST "$A/api/session/$sid/model" -H 'content-type: application/json' \
  -d "{\"model\":{\"id\":\"$model\",\"providerID\":\"$provider\"}}"
curl -s -o /dev/null -u "$AUTH" -X POST "$A/api/session/$sid/prompt" -H 'content-type: application/json' \
  -d '{"prompt":{"text":"Use the bash tool to run exactly: sleep 40 && echo SURVIVED. Then report the output."}}'
sleep 18
pgrep -f "sleep 40 && echo SURVIVED" > /dev/null && ok "the tool is running on the worker" \
  || bad "the tool is running on the worker" "it never started"

# --- 2. kill the process that started it, mid-tool
kill -9 $(pgrep -P $serveA 2>/dev/null) $serveA 2>/dev/null
sleep 3
[ -z "$(lsof -nP -iTCP:$PORT_A -sTCP:LISTEN 2>/dev/null)" ] && ok "serve A is gone" || bad "serve A is gone"
pgrep -f "sleep 40 && echo SURVIVED" > /dev/null && ok "the turn outlived the client that started it" \
  || bad "the turn outlived the client that started it" "the tool died with serve A"

# --- 3. serve B, which never saw this session, knows it and can replay it
running=$(curl -s -u "$AUTH" "$B/api/session/active")
case "$running" in *"$sid"*) ok "serve B reports it running" ;; *) bad "serve B reports it running" "$running" ;; esac

sleep 35
timeout 30 curl -s -N -u "$AUTH" "$B/api/session/$sid/event" > "$RUN/logs/replay.txt" 2>&1
grep -q "SURVIVED" "$RUN/logs/replay.txt" && ok "serve B replays work done while no client existed" \
  || bad "serve B replays work done while no client existed"

# --- 4. start a turn and walk away
started=$(timeout 90 bun run "$OC" session start \
  "Use the bash tool to run exactly: sleep 20 && echo WATCHED. Then report the output." \
  --attach "$B" --model "$MODEL" --dir "$RUN/proj" --json 2>/dev/null)
sid2=$(printf '%s' "$started" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
[ -n "$sid2" ] && ok "session start returned an id without waiting" || bad "session start returned an id" "$started"

listed=$(timeout 60 bun run "$OC" session running --attach "$B" --json 2>/dev/null)
case "$listed" in *"$sid2"*) ok "session running lists it" ;; *) bad "session running lists it" "$listed" ;; esac

# --- 5. follow it live from a client that has never seen it, and stop when the turn stops
began=$(date +%s)
timeout 120 bun run "$OC" session watch "$sid2" --attach "$B" > "$RUN/logs/watch.txt" 2>&1
took=$(( $(date +%s) - began ))
grep -q "WATCHED" "$RUN/logs/watch.txt" && ok "session watch followed the turn" \
  || bad "session watch followed the turn" "$(tail -3 "$RUN/logs/watch.txt")"
[ "$took" -lt 100 ] && ok "session watch stopped when the turn did (${took}s)" \
  || bad "session watch stopped when the turn did" "${took}s, so it hung"

echo
[ "$fails" -eq 0 ] && echo "detached-session-check: OK" || echo "detached-session-check: $fails failed"
exit $([ "$fails" -eq 0 ] && echo 0 || echo 1)
