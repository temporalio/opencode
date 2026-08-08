# @opencode-ai/temporal

A Temporal durable-execution layer for opencode. It makes an opencode **session** a durable
Temporal workflow, so a coding session survives worker loss, can run detached or in the
background, and can be driven from anywhere by signal. It is a drop-in layer: opencode's loop,
tools, model, storage, and HTTP API are untouched.

There are two phases.

## Phase 1 (this directory): wrap the shipping `opencode serve`

A workflow owns one session. It creates the session, then drains a queue of prompts, running each
turn as an activity that drives the shipping opencode server over its HTTP API. The conversation,
the prompt queue, and which turns have completed all live in workflow state, so the session is
durable and resumable. The turn itself runs inside opencode (`prompt_async` forks it server-side),
so it keeps running even while the Temporal worker is down; recovery re-attaches by reading the
recorded messages.

- `src/opencode.ts` — a thin `fetch` client for the opencode HTTP API.
- `src/activities.ts` — `createSession`, `runTurn` (idempotent on the user-message count, so a
  retry never double-sends), `abortTurn`.
- `src/workflows.ts` — `durableSession`: create session, drain the prompt queue, one turn per
  activity; signals `submitPrompt` / `abortSession` / `closeSession`; query `getState`.
- `src/worker.ts` — the Temporal worker (runs on Node via `tsx`).
- `src/demo.ts` — drive a two-turn session end to end.
- `src/crash-demo.ts` — kill the worker mid-turn and show the session still completes.

### Run it

```bash
# 1. a Temporal dev server
temporal server start-dev --port 7237

# 2. opencode serve with a provider key (any provider opencode supports)
OPENAI_API_KEY=... bun run --cwd packages/opencode --conditions=browser src/index.ts serve --port 4599

# 3. the durable-session worker
TEMPORAL_ADDRESS=127.0.0.1:7237 OPENCODE_BASE_URL=http://127.0.0.1:4599 bun run --cwd packages/temporal worker

# 4. drive a session
TEMPORAL_ADDRESS=127.0.0.1:7237 bun run --cwd packages/temporal demo
```

Env: `TEMPORAL_ADDRESS` (default `127.0.0.1:7237`), `TEMPORAL_TASK_QUEUE` (`opencode-durable`),
`OPENCODE_BASE_URL` (`http://127.0.0.1:4599`), `OPENCODE_PROVIDER` (`openai`), `OPENCODE_MODEL`
(`gpt-5-mini`).

### What it proves

`bun run --cwd packages/temporal crash-demo` starts a turn, `SIGKILL`s the worker mid-turn, starts
a fresh worker, and checks the outcome. A passing run shows:

```
reply: "RECOVERED"
runTurn attempts (max): 2 | started-event attempts: [ 1, 2 ]
user messages in session: 1
CRASH-RECOVERY: PASS
```

That is: the turn completed after the crash, Temporal re-drove the activity on a new worker
(attempt 2), and idempotency kept it to a single prompt (no double-send).

### Honest limits of Phase 1

- It makes the **orchestration** durable (the session, the queue, turn re-drive), not opencode's
  in-process turn. If the opencode **server** dies mid-turn, the turn's host side effects can be
  partial; re-driving re-attaches to whatever the server recorded.
- The worker talks to the opencode server over unauthenticated HTTP by default. Run them together
  or set `OPENCODE_SERVER_PASSWORD` and pass the header.

## Phase 2 (built): a durable `SessionExecution` on the v2 engine

opencode's v2 engine (`packages/core` + `packages/server`) is already event-sourced per session and
exposes a substitutable `SessionExecution` service (`active` / `resume` / `wake` / `interrupt`)
whose local impl comments "Future remote placement belongs here." Phase 2 provides a Temporal-backed
`SessionExecution` in `packages/core/src/session/execution/`:

- `temporal-workflow.ts` — the pure per-session workflow (the Temporal equivalent of
  `SessionRunCoordinator`: `wake`/`force` drive one drain, wakes coalesce, quiescent runs end).
- `temporal-activities.ts` — the `runContinuation` activity (heartbeats; forwards cancellation).
- `temporal.ts` — the `SessionExecution` layer + node: `wake` → `signalWithStart`, `resume` →
  forced `signalWithStart`, `interrupt` → cancel signal; the drain is the local coordinator's body
  (`SessionRunner.run`) run in the activity against the durable event log. The Temporal client and
  an embedded worker are co-hosted in the server process (both run under bun).

Wiring is one binding in `packages/server/src/routes.ts`, opt-in via
`OPENCODE_SESSION_EXECUTION=temporal`. Because turn state lives in the event log, the workflow stays
thin and recovery is engine-level: a run re-reads recorded history and continues, it does not
re-attach.

### Run it

```bash
temporal server start-dev --port 7237
OPENAI_API_KEY=... OPENCODE_SESSION_EXECUTION=temporal TEMPORAL_ADDRESS=127.0.0.1:7237 \
  bun run --cwd packages/cli src/index.ts serve --port 4601
```

Create a session and prompt it against `POST /api/session` and `POST /api/session/:id/prompt`; each
session runs as a Temporal workflow `session-exec-<sessionID>`.

### Verified

- With Temporal execution on, prompting a v2 session drove a full turn to completion (`step.ended`,
  `TEMPORAL_V2_OK`), recorded as a completed per-session workflow.
- Engine-level crash recovery (`scripts/v2-crash-test.sh`): killing the whole server (with its
  embedded worker) mid-turn, then restarting, still completes the turn. Temporal re-drives
  `runContinuation` (attempt 2), the run continues from the event log, and the workflow completes.

### Known limits

- `resume` drives a forced run but does not yet carry the typed `RunError` back to the caller (a
  follow-up: a Temporal update). `active` reports sessions this process started (process-local, like
  the local coordinator), not a durable-visibility query. Layer construction connects to Temporal at
  server startup, so the server needs Temporal reachable when `OPENCODE_SESSION_EXECUTION=temporal`.
