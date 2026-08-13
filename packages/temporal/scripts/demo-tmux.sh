#!/usr/bin/env bash
# One-command demo of the Temporal-backed v2 SessionExecution, in one tmux session:
#   left pane   = Temporal dev server (reused when one already answers on the port)
#   right-top   = opencode v2 serve with OPENCODE_SESSION_EXECUTION=temporal
#   right-bottom= a driver that creates a session, prompts it, and prints the reply plus the
#                 workflow evidence, then leaves copy-paste commands for more poking
#
# Env overrides: TEMPORAL_PORT (7237), OPENCODE_PORT (4601), OPENCODE_DB
# (/tmp/opencode-temporal-demo.db), OPENCODE_KEY_FILE (~/.config/ai363/llm.key),
# DEMO_TMUX_SESSION (opencode-temporal).
set -uo pipefail

REPO=$(cd "$(dirname "$0")/../../.." && pwd)
SESSION=${DEMO_TMUX_SESSION:-opencode-temporal}
TPORT=${TEMPORAL_PORT:-7237}
PORT=${OPENCODE_PORT:-4601}
DB=${OPENCODE_DB:-/tmp/opencode-temporal-demo.db}
KEY_FILE=${OPENCODE_KEY_FILE:-$HOME/.config/ai363/llm.key}
B="http://127.0.0.1:$PORT/api"

# The driver body, run inside the third pane via `--drive`.
drive() {
  echo "waiting for serve on :$PORT (first boot bundles the workflow, about a minute)"
  until curl -s -o /dev/null --max-time 2 "$B/session"; do sleep 1; done
  AUTH=$(printf 'opencode:%s' "$(cat "$HOME/.local/state/opencode/password")" | base64)
  SID=$(curl -s -X POST "$B/session" -H "Authorization: Basic $AUTH" \
    -H 'content-type: application/json' \
    -d '{"model":{"providerID":"openai","id":"gpt-5-mini"}}' |
    python3 -c 'import json,sys;print(json.load(sys.stdin)["data"]["id"])')
  echo "session: $SID"
  curl -s -o /dev/null -X POST "$B/session/$SID/prompt" -H "Authorization: Basic $AUTH" \
    -H 'content-type: application/json' \
    -d '{"prompt":{"text":"Reply with the single word PONG."}}'
  echo "prompted; waiting for the turn to settle"
  OUT="RUNNING|"
  for _ in $(seq 1 90); do
    OUT=$(curl -s "$B/session/$SID/history" -H "Authorization: Basic $AUTH" | python3 -c '
import json, sys
d = json.load(sys.stdin)
texts = []
ended = False
for e in d.get("data") or []:
    t = e.get("type", "")
    if "step.ended" in t: ended = True
    if "text.ended" in t: texts.append(e.get("data", {}).get("text", ""))
print(("ENDED" if ended else "RUNNING") + "|" + " ".join(texts))
' 2>/dev/null || echo "RUNNING|")
    case "$OUT" in ENDED*) break ;; esac
    sleep 2
  done
  case "$OUT" in
    ENDED*) echo "reply: ${OUT#ENDED|}" ;;
    *) echo "turn did not settle in time; check the serve pane" ;;
  esac
  echo
  echo "the workflow behind it:"
  temporal workflow list --address "127.0.0.1:$TPORT" | head -5
  echo
  echo "poke further (copy-paste):"
  echo "  temporal workflow show --address 127.0.0.1:$TPORT --workflow-id session-exec-$SID"
  echo "  curl -s $B/session/$SID/history -H 'Authorization: Basic $AUTH'"
  # start-dev puts the UI on the server port + 1000.
  echo "  UI: http://localhost:$((TPORT + 1000))"
  echo
  echo "driver done. serve and the Temporal server keep running in the other panes; the session"
  echo "still accepts prompts. This pane is a normal shell now."
}

[ "${1:-}" = "--drive" ] && { drive; exit 0; }

command -v tmux >/dev/null || { echo "tmux is required: brew install tmux"; exit 1; }
command -v temporal >/dev/null || { echo "temporal CLI is required: brew install temporal"; exit 1; }
[ -f "$KEY_FILE" ] || { echo "no provider key at $KEY_FILE (set OPENCODE_KEY_FILE)"; exit 1; }

tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$REPO" -x 220 -y 50
P0=$(tmux display-message -p -t "$SESSION" '#{pane_id}')

if temporal operator cluster health --address "127.0.0.1:$TPORT" >/dev/null 2>&1; then
  tmux send-keys -t "$P0" "echo 'reusing the Temporal dev server already on :$TPORT'" C-m
else
  tmux send-keys -t "$P0" "temporal server start-dev --port $TPORT" C-m
fi

# serve waits for Temporal first: layer construction connects at startup. The key stays out of
# this script's expansion; the pane's shell reads it.
P1=$(tmux split-window -P -F '#{pane_id}' -t "$P0" -h -c "$REPO")
tmux send-keys -t "$P1" "until temporal operator cluster health --address 127.0.0.1:$TPORT >/dev/null 2>&1; do sleep 1; done; OPENAI_API_KEY=\$(cat $KEY_FILE) OPENCODE_SESSION_EXECUTION=temporal TEMPORAL_ADDRESS=127.0.0.1:$TPORT OPENCODE_DB=$DB bun run --cwd packages/cli src/index.ts serve --port $PORT" C-m

P2=$(tmux split-window -P -F '#{pane_id}' -t "$P1" -v -c "$REPO")
tmux send-keys -t "$P2" "TEMPORAL_PORT=$TPORT OPENCODE_PORT=$PORT bash packages/temporal/scripts/demo-tmux.sh --drive" C-m

tmux select-pane -t "$P2"
if [ -t 0 ]; then
  if [ -n "${TMUX:-}" ]; then tmux switch-client -t "$SESSION"; else tmux attach -t "$SESSION"; fi
else
  echo "started tmux session '$SESSION'; attach with: tmux attach -t $SESSION"
fi
