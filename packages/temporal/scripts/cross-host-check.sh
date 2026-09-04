#!/usr/bin/env bash
# Any worker resumes any session, across machines rather than across processes.
#
# On one host the second worker already has the project on disk, so the interesting half of the
# claim is never exercised: the tree is there whether or not anything shipped it. Here worker B is a
# container with an empty project volume, so a session that moves to it has to bring its worktree
# along, out of the snapshot packs in the shared store.
#
# Usage: OPENAI_API_KEY=... packages/temporal/scripts/cross-host-check.sh
#
# Not covered: one libSQL server, so this shows a shared store over a network rather than one that
# survives losing a node.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
COMPOSE="docker compose -f packages/temporal/docker/compose.yml"
MODEL_ID="${MODEL_ID:-gpt-5-mini}"

fails=0
ok()  { printf 'PASS %s\n' "$1"; }
bad() { printf 'FAIL %s   (%s)\n' "$1" "${2:-}"; fails=$((fails + 1)); }

# KEEP=1 leaves the stack up, which is the difference between reading a failure and guessing at it.
cleanup() { [ -n "${KEEP:-}" ] || $COMPOSE down -v >/dev/null 2>&1; }
trap cleanup EXIT

[ -n "${OPENAI_API_KEY:-}" ] || { echo "set OPENAI_API_KEY"; exit 1; }

$COMPOSE down -v >/dev/null 2>&1
# Only when the image is missing. The compose file mounts the engine's source over the image, so a
# code change does not need a new one, and the dependency install is most of the build.
if ! docker image inspect opencode-temporal:l3 >/dev/null 2>&1; then
  docker build -f packages/temporal/docker/Dockerfile -t opencode-temporal:l3 . >/dev/null \
    || { echo "build failed"; exit 1; }
fi
$COMPOSE up -d temporal sqld serve worker-a >/dev/null 2>&1 || { echo "stack failed"; exit 1; }

api() { curl -s -u "opencode:$PW" "$@"; }

# The serve generates its own password on first boot and prefers it over the environment, so ask it
# rather than tell it.
PW=""
for _ in $(seq 1 60); do
  PW=$($COMPOSE exec -T serve sh -c 'cat /root/.local/state/opencode/password 2>/dev/null' 2>/dev/null | tr -d '\r\n')
  [ -n "$PW" ] && break
  sleep 3
done
[ -n "$PW" ] && ok "serve is up" || { bad "serve never came up"; exit 1; }

hostA=$($COMPOSE exec -T worker-a hostname 2>/dev/null | tr -d '\r')
[ -n "$hostA" ] && ok "worker A is a host of its own ($hostA)" || bad "worker A came up"

# A project only worker A and serve can see.
$COMPOSE exec -T serve sh -c \
  'cd /project && git init -q 2>/dev/null; echo hello > README.md; git add -A; git commit -qm init' \
  >/dev/null 2>&1

new_session() {
  api -X POST http://127.0.0.1:4096/api/session -H 'content-type: application/json' \
    -d '{"directory":"/project"}' | sed -n 's/^{"data":{"id":"\([^"]*\)".*/\1/p'
}
prompt() {
  api -o /dev/null -X POST "http://127.0.0.1:4096/api/session/$1/prompt" \
    -H 'content-type: application/json' -d "{\"prompt\":{\"text\":$2}}"
}
# A turn is over when a step of it ends on "stop", which is not the same as the session leaving the
# running set: the supervisor stays open for its idle timeout with nothing left to do. Counted
# rather than matched, because the history of a second turn still contains the first one's ending,
# and matching would call every later turn finished before it started.
stops() {
  local body
  body=$(api "http://127.0.0.1:4096/api/session/$1/history?limit=100" 2>/dev/null)
  case "$body" in *InvalidRequestError*) echo "  history rejected: $body" >&2; echo -1; return ;; esac
  printf '%s' "$body" | grep -o '"finish":"stop"' | wc -l | tr -d ' '
}
await_turn() {
  local before=$2
  for _ in $(seq 1 90); do
    [ "$(stops "$1")" -gt "$before" ] && return 0
    sleep 4
  done
  return 1
}

sid=$(new_session)
[ -n "$sid" ] && ok "a session was created ($sid)" || { bad "no session"; exit 1; }
api -o /dev/null -X POST "http://127.0.0.1:4096/api/session/$sid/model" \
  -H 'content-type: application/json' -d "{\"model\":{\"id\":\"$MODEL_ID\",\"providerID\":\"openai\"}}"

# --- turn 1 on worker A: writes a file, so a snapshot of the tree is captured and shipped
before=$(stops "$sid")
prompt "$sid" '"Use the bash tool to run exactly: echo TRAVELLED > /project/note.txt && cat /project/note.txt. Report the output."'
await_turn "$sid" "$before" && ok "turn 1 finished on worker A" || bad "turn 1 never finished"

packs=$(curl -s http://127.0.0.1:8081/v2/pipeline -H 'content-type: application/json' \
  -d '{"requests":[{"type":"execute","stmt":{"sql":"select count(*) from snapshot_pack"}},{"type":"close"}]}' \
  2>/dev/null | grep -o '"value":"[0-9]*"' | head -1 | grep -o '[0-9]*')
[ "${packs:-0}" -gt 0 ] && ok "the tree was shipped to the shared store ($packs packs)" \
  || bad "no snapshot packs reached the store" "$packs"

# --- worker A's host goes away, and a host that has never seen this project takes over
docker kill "$($COMPOSE ps -q worker-a)" >/dev/null 2>&1
sleep 2
[ -z "$($COMPOSE ps -q --status running worker-a)" ] && ok "worker A's host is gone" || bad "worker A's host is gone"

$COMPOSE up -d worker-b >/dev/null 2>&1
sleep 8
hostB=$($COMPOSE exec -T worker-b hostname 2>/dev/null | tr -d '\r')
[ "$hostB" != "$hostA" ] && ok "worker B is a different host ($hostB)" || bad "worker B is a different host"
empty=$($COMPOSE exec -T worker-b sh -c 'ls -A /project | wc -l' 2>/dev/null | tr -d '\r ')
[ "${empty:-1}" = "0" ] && ok "worker B's project is empty before the turn" || bad "worker B's project was not empty" "$empty"

# --- turn 2 on worker B: the file only exists there if the worktree travelled
before=$(stops "$sid")
prompt "$sid" '"Use the bash tool to run exactly: cat /project/note.txt. Report exactly what it printed."'
await_turn "$sid" "$before" && ok "turn 2 finished on worker B" || bad "turn 2 never finished"

# Asked of worker B's own disk rather than of the transcript. The transcript still holds turn 1,
# where the file did exist, so anything matched across the whole of it proves nothing about B.
landed=$($COMPOSE exec -T worker-b sh -c 'cat /project/note.txt 2>&1' 2>/dev/null | tr -d '\r')
case "$landed" in
  TRAVELLED*) ok "the worktree travelled to worker B" ;;
  *) bad "the worktree travelled to worker B" "$landed" ;;
esac

echo
[ "$fails" -eq 0 ] && echo "cross-host-check: OK" || echo "cross-host-check: $fails failed"
exit $([ "$fails" -eq 0 ] && echo 0 || echo 1)
