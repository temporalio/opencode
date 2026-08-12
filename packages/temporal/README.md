# @opencode-ai/temporal

A Temporal durable-execution layer for opencode. It makes an opencode **session** a durable
Temporal workflow, so a coding session survives worker loss, can run detached or in the
background, and can be driven from anywhere by signal. It is a drop-in layer: opencode's loop,
tools, model, storage, and HTTP API are untouched.

This package wraps the **shipping** `opencode serve`: the agent stays a black box behind its HTTP
API, and durability is added from the outside. It is the pattern for an agent you can't or won't
modify. The deeper integration (a Temporal-backed `SessionExecution` inside the v2 engine, with
per-step activities and engine-level crash resume) is a separate change on the
`2026/08/opencode-temporal` branch.

## How it works

A workflow owns one session. It creates the session, then drains a queue of prompts, running each
turn as an activity that drives the shipping opencode server over its HTTP API. The conversation,
the prompt queue, and which turns have completed all live in workflow state, so the session is
durable and resumable. The turn itself runs inside opencode (`prompt_async` forks it server-side),
so it keeps running even while the Temporal worker is down; recovery re-attaches by reading the
recorded messages.

- `src/opencode.ts`: a thin `fetch` client for the opencode HTTP API.
- `src/activities.ts`: `createSession`, `runTurn` (idempotent on the user-message count, so a
  retry never double-sends), `abortTurn`.
- `src/workflows.ts`: `durableSession`: create session, drain the prompt queue, one turn per
  activity; signals `submitPrompt` / `abortSession` / `closeSession`; query `getState`.
- `src/worker.ts`: the Temporal worker (runs on Node via `tsx`).
- `src/demo.ts`: drive a two-turn session end to end.
- `src/crash-demo.ts`: kill the worker mid-turn and show the session still completes.

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

### Honest limits

- It makes the **orchestration** durable (the session, the queue, turn re-drive), not opencode's
  in-process turn. If the opencode **server** dies mid-turn, the turn's host side effects can be
  partial; re-driving re-attaches to whatever the server recorded. Closing that requires the
  engine-level integration on the `2026/08/opencode-temporal` branch.
- The worker talks to the opencode server over unauthenticated HTTP by default. Run them together
  or set `OPENCODE_SERVER_PASSWORD` and pass the header.
