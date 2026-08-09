#!/bin/bash
# Any-worker resume on a shared store, shown deterministically via cross-worker continuity:
# worker A handles turn 1 (a code word), A is killed entirely, then a FRESH worker B (same task
# queue, same shared store, never saw the session) handles turn 2 and can only answer by loading
# turn 1 from the shared store. This is the fleet-durability story: Temporal (cross-worker
# execution) + a shared store (cross-worker state) = any worker resumes any session.
# (Execution re-drive after a crash is covered separately by v2-crash-test.sh.)
#
# Prereqs: a Temporal dev server on :7237, an OpenAI key, bun.
set -u
REPO=$(cd "$(dirname "$0")/../../.." && pwd)
KEY_FILE=${OPENCODE_KEY_FILE:-$HOME/.config/ai363/llm.key}
SHARED=${OPENCODE_SHARED_DB:-/tmp/oc-shared/opencode.db}
AUTH=$(printf 'opencode:%s' "$(cat "$HOME/.local/state/opencode/password" 2>/dev/null)" | base64)
H="Authorization: Basic $AUTH"

boot() { # boot <port>
  cd "$REPO"
  nohup env OPENAI_API_KEY="$(cat "$KEY_FILE")" OPENCODE_SESSION_EXECUTION=temporal TEMPORAL_ADDRESS=127.0.0.1:7237 \
    OPENCODE_DB="$SHARED" \
    bun run --cwd packages/cli src/index.ts serve --port "$1" --hostname 127.0.0.1 >"/tmp/oc-worker-$1.log" 2>&1 &
  until lsof -ti tcp:"$1" >/dev/null 2>&1 && grep -q "SessionExecutionTemporal ready" "/tmp/oc-worker-$1.log" 2>/dev/null; do sleep 1; done
}
kill_port() { lsof -ti tcp:"$1" 2>/dev/null | xargs -r kill -9 2>/dev/null; }

# turn <base> <sid> <text>; waits for a NEW step.ended (past the pre-prompt baseline); echoes the
# event count that existed BEFORE this turn (so callers can slice out just this turn's events).
turn() {
  local base=$1 sid=$2 text=$3 pre
  pre=$(curl -sS -m8 "$base/session/$sid/history" -H "$H" | python3 -c 'import sys,json;print(len(json.load(sys.stdin).get("data",[])))' 2>/dev/null)
  pre=${pre:-0}
  curl -sS -m10 -o /dev/null -X POST "$base/session/$sid/prompt" -H "$H" -H 'content-type: application/json' \
    -d "$(python3 -c 'import json,sys;print(json.dumps({"prompt":{"text":sys.argv[1]}}))' "$text")"
  for _ in $(seq 1 30); do
    sleep 2
    local done
    done=$(curl -sS -m8 "$base/session/$sid/history" -H "$H" | PRE=$pre python3 -c '
import sys,json,os
items=json.load(sys.stdin).get("data",[])
new=items[int(os.environ["PRE"]):]
print("yes" if any(e.get("type","").endswith("step.ended") for e in new) else "no")' 2>/dev/null)
    [[ "$done" == "yes" ]] && { echo "$pre"; return 0; }
  done
  echo "$pre"; return 1
}

mkdir -p "$(dirname "$SHARED")"; rm -f "$SHARED"*
echo "[1] boot worker A :4601"; boot 4601
BA=http://127.0.0.1:4601/api
SID=$(curl -sS -m10 -X POST $BA/session -H "$H" -H 'content-type: application/json' -d '{"model":{"providerID":"openai","id":"gpt-5-mini"}}' | python3 -c 'import sys,json;print((json.load(sys.stdin).get("data") or {}).get("id",""))')
echo "    SID=$SID"

echo "[2] turn 1 on A: set a code word"
N1=$(turn "$BA" "$SID" "Remember this code word for later: BANANA47. Just reply OK.")
echo "    turn 1 done (events=$N1)"

echo "[3] KILL worker A entirely"; kill_port 4601; sleep 2

echo "[4] boot a FRESH worker B :4602 (same queue + shared store, never saw this session)"; boot 4602
BB=http://127.0.0.1:4602/api

echo "[5] turn 2 on B: recall the code word (only possible by loading turn 1 from the shared store)"
N2=$(turn "$BB" "$SID" "What was the code word I asked you to remember? Reply with only that word.")
echo "    turn 2 done (events=$N2)"

echo "[6] verify B's turn-2 reply used shared state, and ran on B"
python3 - "$BB" "$SID" "$N2" "$AUTH" <<'PY'
import sys,json,urllib.request
base,sid,n2,auth=sys.argv[1],sys.argv[2],int(sys.argv[3] or 0),sys.argv[4]
req=urllib.request.Request(f"{base}/session/{sid}/history",headers={"Authorization":"Basic "+auth})
items=json.load(urllib.request.urlopen(req,timeout=8)).get("data",[])
# only turn-2 events (from turn 2's baseline); pull assistant text
new=items[n2:]
text=" ".join(json.dumps(e.get("data",{})) for e in new)
ok = "BANANA47" in text
print("    turn-2 reply recalled the code word from the shared store:", ok)
PY
echo "[7] evidence: A was dead during turn 2; worker that ran it"
temporal workflow show --address 127.0.0.1:7237 --workflow-id "session-exec-$SID" --output json 2>/dev/null > /tmp/failover-wf.json
python3 - <<'PY'
import json
ev=json.load(open("/tmp/failover-wf.json")).get("events",[])
ids=sorted(set(e["activityTaskStartedEventAttributes"].get("identity") for e in ev if e.get("activityTaskStartedEventAttributes")))
print("    runContinuation ran on worker identities:", ids)
PY
kill_port 4602