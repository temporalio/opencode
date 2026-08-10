#!/bin/bash
# Engine-level crash recovery for the v2 Temporal SessionExecution (Phase 2).
#
# Kills the whole v2 server (which co-hosts the embedded Temporal worker) mid-turn, restarts it,
# and shows the turn still completes: Temporal re-drives the runContinuation activity, and
# SessionRunner.run re-reads the durable event log and continues from where it stopped.
#
# Prereqs: a Temporal dev server on :7237, an OpenAI key at $OPENCODE_KEY_FILE (default
# ~/.config/ai363/llm.key), and the v2 server run with OPENCODE_SESSION_EXECUTION=temporal.
set -u
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
KEY_FILE=${OPENCODE_KEY_FILE:-$HOME/.config/ai363/llm.key}
PORT=${OPENCODE_PORT:-4601}
B=http://127.0.0.1:$PORT/api
AUTH=$(printf 'opencode:%s' "$(cat "$HOME/.local/state/opencode/password")" | base64)
H="Authorization: Basic $AUTH"

boot() {
  cd "$REPO"
  nohup env OPENAI_API_KEY="$(cat "$KEY_FILE")" OPENCODE_SESSION_EXECUTION=temporal TEMPORAL_ADDRESS=127.0.0.1:7237 \
    bun run --cwd packages/cli src/index.ts serve --port "$PORT" --hostname 127.0.0.1 >/tmp/oc-v2-temporal.log 2>&1 &
  until lsof -ti tcp:$PORT >/dev/null 2>&1 && grep -q "SessionExecutionTemporal ready" /tmp/oc-v2-temporal.log 2>/dev/null; do sleep 1; done
}
killserver() { lsof -ti tcp:$PORT 2>/dev/null | xargs -r kill -9 2>/dev/null; pkill -9 -f "packages/cli src/index.ts serve" 2>/dev/null; sleep 1; }

echo "[1] ensure server up"; { grep -q "SessionExecutionTemporal ready" /tmp/oc-v2-temporal.log 2>/dev/null && lsof -ti tcp:$PORT >/dev/null 2>&1; } || boot

echo "[2] create + prompt a multi-step task"
SID=$(curl -sS -m10 -X POST $B/session -H "$H" -H 'content-type: application/json' -d '{"model":{"providerID":"openai","id":"gpt-5-mini"}}' | python3 -c 'import sys,json;print((json.load(sys.stdin).get("data") or {}).get("id",""))')
echo "    SID=$SID"
curl -sS -m10 -o /dev/null -w '    prompt HTTP %{http_code}\n' -X POST $B/session/$SID/prompt -H "$H" -H 'content-type: application/json' \
  -d '{"prompt":{"text":"Do these strictly in order using your tools, one per step: (1) write a file a.txt containing STEP_A; (2) read a.txt; (3) write a file result.txt containing exactly the token CRASH_RECOVERED; (4) read result.txt and reply with only its contents."}}'

echo "[3] let a few steps record, then KILL the whole server mid-turn"
sleep 7
PRE=$(curl -sS -m8 $B/session/$SID/history -H "$H" | python3 -c 'import sys,json;d=json.load(sys.stdin).get("data",[]);print(len(d), "seen="+str("CRASH_RECOVERED" in json.dumps(d)))')
echo "    events before crash: $PRE"
# The token is written by step 3 of the task; if it already exists the kill landed too late and the
# run proves nothing about recovery.
[[ "$PRE" == *seen=True* ]] && { echo "RESULT: INVALID (task finished before the crash; rerun)"; exit 2; }
killserver; echo "    server killed"

echo "[4] restart server (embedded worker re-registers; Temporal re-drives)"
sleep 3; boot; echo "    server back up"

echo "[5] await turn completion post-recovery"
DONE=no
for i in $(seq 1 40); do
  sleep 3
  r=$(curl -sS -m8 $B/session/$SID/history -H "$H" | python3 -c '
import sys,json
items=json.load(sys.stdin).get("data",[])
types=[e.get("type","") for e in items]
print("ENDED" if any(t.endswith("step.ended") for t in types) else "pending", "seen="+str("CRASH_RECOVERED" in json.dumps(items)))' 2>/dev/null)
  # Pass needs the post-crash work to have actually happened (the token is only written by a step
  # that runs after the kill), not just any pre-crash step.ended in the history.
  echo "    poll $i: $r"; [[ "$r" == "ENDED seen=True" ]] && { DONE=yes; break; }
done

echo "[6] evidence from Temporal"
temporal workflow show --address 127.0.0.1:7237 --workflow-id "session-exec-$SID" --output json 2>/dev/null > /tmp/v2wf.json
python3 - <<'PY'
import json
ev=json.load(open("/tmp/v2wf.json")).get("events",[])
attempts=[int(e["activityTaskStartedEventAttributes"].get("attempt",1)) for e in ev if e.get("activityTaskStartedEventAttributes")]
print("    runContinuation attempts:", attempts, "| max:", max(attempts) if attempts else 0)
PY
echo "RESULT: turn completed post-crash = $DONE"
[[ "$DONE" == yes ]] || exit 1
