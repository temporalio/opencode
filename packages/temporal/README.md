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

### Two modes, one supervisor

The factory has exactly two modes. `OPENCODE_SESSION_EXECUTION=temporal` runs the session
supervisor on a Temporal worker; anything else (the default) runs the SAME supervisor in-process
with the micro-driver (`workflow-core.ts` + `local-driver.ts`): no server, no worker, no ports,
durability from the event log. Both modes drive the turn one **step** at a time: the `sessionTurn`
supervisor loops a `runTurnStep` drain, so in temporal mode each step (one provider attempt + its
tools) is its own activity with its own retry/timeout/visibility. It reuses `SessionRunner.runStep`
(one iteration of `run`'s loop), so the turn semantics are unchanged. Verified: a
create-then-read-then-reply turn recorded three `runTurnStep` activities under a `sessionTurn`
workflow and completed. (Earlier whole-turn-per-activity and stock-coordinator modes were folded
away; the whole-turn workflow stays exported for executions already running.)

A per-step re-drive resumes from the durable event log rather than re-running work. `runStep` closes
any tool left dangling by an interrupted attempt on every entry, not just the first. Without that, a
mid-turn retry (`first=false`) re-streamed a request carrying a `tool_use` with no `tool_result`,
which the provider rejects, a retry poison loop. And if the crashed step had already dispatched tools
it is finalized from the log: completed tool results are kept, still-unsettled tools are failed, and
a synthesized `Step.Ended` closes the step without re-calling the model. A tool caught in flight at
the crash is handled by declared idempotency: a side-effect-free tool (`read`/`glob`/`grep`, marked
`idempotent: true`) is re-run for a real result, while a side-effecting tool is marked interrupted
and left for the model to redo. The harness cannot know whether the side-effecting one already ran
and must not re-run `git push`, so the default is non-idempotent; the blanket case (idempotency keys
against an external system) is per-integration and out of scope. Finer granularity (the model call
and each tool as separate Temporal activities) would un-fuse the eager tool dispatch and is left for
later. Verified by `packages/core/test/session-runner-resume.test.ts`.

### Notes

- `resume` awaits the forced run (via Update-with-Start) and surfaces its failure as the **exact
  tagged `RunError`**: the activity encodes the error through a `Schema.Union` of every RunError
  member (`run-error-codec.ts`) into the failure details, and the layer reconstructs it, falling
  back to a `ContextSnapshotDecodeError` carrying the text only if decoding fails.
- `active` queries Temporal for the open per-session workflows, so it reflects durable state and
  survives a restart (not a process-local set).
- The per-session workflow is long-lived and self-terminates after an idle period. Layer
  construction connects to Temporal at startup, so the server needs Temporal reachable when
  `OPENCODE_SESSION_EXECUTION=temporal`.
- Activity bounds: the 10s heartbeat is the liveness bound (worker death re-drives within seconds);
  `startToCloseTimeout` is a 12-hour backstop for a drain that hangs while its process stays alive.
  Known limit: when an attempt is retried while the previous one is still alive (a network
  partition, or the backstop firing), the old attempt keeps publishing for a few seconds until its
  heartbeat is rejected and the AbortSignal interrupts it; the projector's status guards make
  duplicate settlements no-ops in projection, but the overlap window is not fully fenced.

`scripts/resume-check.ts` verifies resume: it resolves on a healthy session and rejects on a failing
one with the original tagged error (`LLM.Error`) reconstructed across the boundary.

### Running workers separately

By default the serve process hosts both the Temporal activity worker and the workflow client
(`OPENCODE_TEMPORAL_ROLE=both`). To scale workers independently of the HTTP server, run standalone
workers and point serve at client-only:

```bash
# serve drives workflows, hosts no worker
OPENCODE_TEMPORAL_ROLE=client OPENCODE_SESSION_EXECUTION=temporal ... serve --port 4601

# one or more standalone activity workers (no HTTP surface)
OPENCODE_TEMPORAL_ROLE=worker OPENCODE_SESSION_EXECUTION=temporal \
  TEMPORAL_ADDRESS=127.0.0.1:7237 OPENCODE_DB_URL=... \
  bun run packages/server/src/worker.ts
```

`packages/server/src/worker.ts` builds the same application context serve uses (`createWorkerLayer`)
without the HTTP API, so a worker resumes a session purely from the shared store.
`scripts/standalone-worker-smoke.sh` verifies a worker comes up with no serve process and registers
on the task queue. Caveat: file-touching tools run against the local working tree, so a worker must
have the session's worktree present (see "What resumes cross-host").

### Durable permission asks

A tool waiting for user approval used to park on an in-memory deferred: invisible outside the asking
process (a standalone worker's ask could never be answered) and gone on restart. A pending ask is now
also a row in the shared store (`permission_request`). The blocked `assert` races its local deferred
against a poll of the row, so a reply from ANY process sharing the store (the HTTP server answering
for a detached worker) unblocks it; `list`/`get`/`forSession` read the rows, so serve can show asks
raised elsewhere. Replies keep their semantics: `once`/`always` approve (an `always` rule
retro-approves other pending asks it covers), `reject` declines and cascades to the session's other
pending asks, a rejection message arrives as the typed `CorrectedError`. A user decline inside a
Temporal activity is now a non-retryable failure, so the workflow does not re-drive a turn the user
stopped. Tool-originated asks have deterministic ids (session + callID + action + resources), so a
re-driven activity adopts the same pending row instead of filing a duplicate, and an approval that
landed while the asker was dead short-circuits the retry (a one-time approve is honored across
re-drives without a saved rule). Graceful shutdown retires the process's pending asks as `expired`
and a revived attempt flips them back to pending; after a hard crash the pending row simply feeds
the retry. A pending ask whose session is abandoned lingers in the list until a reply retires it.
Verified by `packages/core/test/permission-durable.test.ts` (two independent stacks over one store).
The `question` tool still uses an in-process deferred and needs the same treatment.

## Shared, durable event store (any-worker resume)

The v2 engine event-sources each session to a SQLite store. By default that is a local file, so a
session can only be resumed on the host holding the file. Point every worker at one **shared** store
and any worker resumes any session: Temporal load-balances `runContinuation` across the fleet,
`active` is visibility-backed, and the resumed worker reads the session purely from the shared log.

- **Same host**: set `OPENCODE_DB` to one absolute path on all workers (WAL + `busy_timeout` allow
  multiple processes). No code change.
- **Across hosts**: set `OPENCODE_DB_URL` to a libSQL URL (self-hosted `sqld` or Turso), with
  `OPENCODE_DB_AUTH_TOKEN` if needed. `packages/core/src/database/sqlite.libsql.ts` provides a
  `SqlClient` over `@libsql/client`; it speaks the same SQLite dialect, so the schema and all
  migrations are unchanged. Selection is one env check in `database.ts`.

`scripts/shared-store-failover.sh` verifies it: worker A handles turn 1 (a code word), A is killed,
and a fresh worker B (same queue, same store, never saw the session) handles turn 2 and recalls the
code word, which it can only do by loading turn 1 from the shared store. The two turns run on two
distinct worker identities.

### Caveats

- Cold-start migrations serialize across processes (one `BEGIN IMMEDIATE` transaction wraps
  check-and-apply, so concurrent starts wait and then no-op). Running migrations once as a deploy
  step is still good practice for large fleets, and on a remote store it keeps cold starts from
  contending for the write lock.
- The PRAGMAs (`journal_mode` / `synchronous` / `busy_timeout` / `cache_size` / `wal_checkpoint`)
  are local-file semantics and are skipped for the shared/libSQL backend, which manages journaling
  itself.
- Multi-statement writes commit atomically on both backends. The libSQL client runs each statement
  as its own auto-commit request, so a `BEGIN`/`COMMIT` emitted as plain statements would not bind a
  transaction over a remote URL. The shared backend instead routes a transaction through a real
  interactive libSQL transaction (one pinned stream), which is all-or-nothing. Verified against an
  embedded (`file:`) store (`packages/core/test/database-libsql-transaction.test.ts`) AND against a
  live networked `sqld` over HTTP (`packages/core/test/database-libsql-remote.test.ts`: atomic
  commit, full rollback on a mid-transaction failure, and the schema migrations applied remotely).
  The remote suite needs a server, so it runs only when `OPENCODE_LIBSQL_TEST_URL` is set (e.g.
  `turso dev --port 8899`, then `OPENCODE_LIBSQL_TEST_URL=http://127.0.0.1:8899`); it skips
  otherwise.
- Known limit: every durable event is its own transaction, and the engine records text/reasoning
  deltas as events, so a streaming turn against a REMOTE store pays one interactive transaction per
  delta, serialized per process. Fine for a shared local file; expect reduced streaming throughput
  over a network URL until delta events are batched for the remote backend.

### What resumes cross-host, and what does not

The runner rebuilds a session's LLM context purely from the shared DB (`SessionHistory.entriesForRunner`
then `toLLMMessages`); it never reads local disk to reconstruct context. So the **conversation** resumes
on any worker: messages, tool results (the bounded preview and structured output that the model sees),
prompt attachments (stored inline as `data:` URIs in the prompt), and credentials (`CredentialTable`)
all ride the shared store.

Host-local state that does NOT ride the DB, so it is not reconstructed on a different host:

- **The project working tree.** File-touching tools (read/edit/bash) operate on the local worktree, so
  a turn that keeps editing files must resume on a worker that has that worktree. This is the one real
  cross-host correctness constraint. Three ways to satisfy it: co-locate a session's workers by worktree
  (session affinity via a per-worktree Temporal task queue), share the worktree (a networked
  filesystem), or reconstruct it from the last snapshot on resume (needs a shared snapshot store).
  [docs/worktree-portability.md](docs/worktree-portability.md) weighs the three; the recommendation is
  per-worktree task queues, with snapshot reconstruction as the long-term path.
- **The snapshot store (`${data}/snapshot`) and the retained full tool-output files
  (`${data}/tool-output`).** The runner never reads these to rebuild context: snapshot file-diffs are
  best-effort (`Effect.catch` to `undefined`), and the model sees the bounded tool-output preview, not
  the file. They only affect the diff/restore/revert features and full-output viewing. Point `${data}`
  (the XDG data dir) at shared storage to make them portable.
