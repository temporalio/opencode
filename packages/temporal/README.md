# @opencode-ai/temporal

The Temporal executor for opencode's `SessionExecution` seam, packaged as a plugin: core carries
the seam, the built-in local executor, and the executor-agnostic toolkit; this package is one
dependency that makes an opencode **session** a durable Temporal workflow. A coding session
survives worker loss, can run detached or in the background, and can be driven from anywhere by
signal. opencode's loop, tools, model, storage, and HTTP API are untouched, and nothing Temporal
exists in core.

A lighter increment exists as its own change: the `2026/08/opencode-temporal-http` branch wraps
the **shipping** `opencode serve` over its HTTP API, for the agent-as-black-box case. It makes the
orchestration durable but cannot recover a partial turn. This change is the deeper one: durability
inside the engine, so a crashed turn resumes mid-step instead of being re-attached to.

## How it fits together

One design decision carries the change: durability is a choice of executor behind the substitutable
`SessionExecution` service, and both executors drive the same `SessionRunner` over the same durable
event log. Everything else here is a consequence of taking at-least-once execution seriously.

One env var picks the executor. `temporal` runs each session as a per-session Temporal workflow
with one activity per step (this package: `executor.ts` wires the client and worker, `supervisor.ts`
is the loop, `workflow.ts` adapts it to the sandbox, `drain.ts` is the step body). The default runs
in-process on the proven `SessionRunCoordinator` (core's `execution/local.ts`), the same lifecycle
the v1 server uses, with no server and no ports (see [Two modes, one runner](#two-modes-one-runner)).
What an executor must do is defined executably: core's conformance suite
(`session/execution/conformance.ts`) runs the same wake/resume/interrupt scenarios against the local
executor in core's tests and against this package through real workflows.

That forces six things:

1. **State must be shareable.** Any worker resumes any session only if the event log is not
   host-local: the libSQL backend, atomic remote writes, busy retry, serialized migrations
   ([Shared, durable event store](#shared-durable-event-store-any-worker-resume)).
2. **Re-drives must be safe.** A retried step reuses completed tool results, re-runs only tools
   declared idempotent, and fails the rest for the model to redo. The step loop is bounded
   (`loop-guard.ts`: a step ceiling plus a repeated-identical-call detector), because a runaway
   turn would otherwise be a durable runaway turn
   ([Two modes, one runner](#two-modes-one-runner)).
3. **Two writers must be fenced.** A superseded attempt cannot keep appending to the log; each
   drain claims the log with an attempt token ([Notes](#notes)).
4. **The worktree must travel.** Snapshot trees ship as incremental git packs, and a worker
   without the project tree rebuilds it before the run
   ([What resumes cross-host](#what-resumes-cross-host-and-what-does-not)).
5. **Human-in-the-loop must be durable.** A pending permission ask is a row in the shared store,
   answerable from any process, adopted by re-drives, expired when abandoned
   ([Durable permission asks](#durable-permission-asks)).
6. **Errors must survive the boundary.** `resume` rejects with the exact tagged `RunError`
   reconstructed from the failure details, and a user decline is non-retryable ([Notes](#notes)).

Operationally, workers scale separately from the HTTP server
([Running workers separately](#running-workers-separately)), and `active` is backed by Temporal
visibility, so it survives restarts.

## A durable `SessionExecution` on the v2 engine

opencode's v2 engine (`packages/core` + `packages/server`) is already event-sourced per session and
exposes a substitutable `SessionExecution` service (`active` / `resume` / `wake` / `interrupt`)
whose local impl comments "Future remote placement belongs here." This change provides a
Temporal-backed `SessionExecution` in `packages/core/src/session/execution/`:

- `temporal-workflow.ts`: the pure per-session workflow (the Temporal equivalent of
  `SessionRunCoordinator`: `wake`/`force` drive one drain, wakes coalesce, quiescent runs end).
- `temporal-activities.ts`: the `runTurnStep` activity (heartbeats; forwards cancellation;
  injects the attempt's event-log owner token).
- `temporal.ts`: the `SessionExecution` layer + node: `wake` → `signalWithStart`, `resume` →
  forced `signalWithStart`, `interrupt` → cancel signal; each drain runs one step of the local
  coordinator's loop (`SessionRunner.runStep`) in an activity against the durable event log. The
  Temporal client and an embedded worker are co-hosted in the server process (both run under bun).

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
- Engine-level crash recovery: killing the whole server (with its
  embedded worker) mid-turn, then restarting, still completes the turn. Temporal re-drives
  the in-flight step activity (attempt 2), the run continues from the event log, and the workflow
  completes.

### A step as three activities (`OPENCODE_TEMPORAL_STEPPED=1`)

By default one step (a provider attempt plus every tool it asks for) is a single activity. That is
the smallest unit the runner used to expose, and it means nothing can sit between the model asking
for a tool and the tool running. `OPENCODE_TEMPORAL_STEPPED=1` splits a step into three kinds of
activity instead:

```
runModelCall  ->  runToolCall (one per call, concurrent)  ->  sealStep
```

`SessionRunner.runModelCall` performs the attempt, records each call as `Tool.Called`, and hands the
calls back rather than running them. `runToolCall` settles one call. `sealStep` takes the end
snapshot, diffs it against the start, and publishes `Step.Ended`. The loop between them is workflow
code, so a retry policy, a timeout, an approval or a budget can live where the model-to-tools handoff
used to be. Each activity also carries its own bounds: sealing does not inherit a turn-sized
backstop, and one tool waiting on a human no longer holds the attempt and its sibling tools under a
single timeout.

The supervisor is unchanged. Wake, interrupt, idle self-termination and continue-as-new only ever
called one `runTurnStep`, so the stepped mode supplies a different one. The mode rides the workflow
input, so a session that rolls over keeps it.

Two things are load-bearing and easy to get wrong:

- **One owner token per step, not per activity.** The event log fences a publish behind the current
  owner, so a step's writers have to share one. Only `runModelCall` claims; the tool and seal
  activities publish under the token it returns. A token per activity execution (right when the
  activity *is* the whole step) would make them fence each other out.
- **A retried call is not silently repeated.** Whether a side effect already happened is the
  activity's knowledge, not the workflow's, so `retry` comes from the Temporal attempt number. On a
  retry only a tool declaring `idempotent` runs again; anything else is reported to the model as an
  unknown outcome. This is the rule the crash-resume path already followed.

#### What it costs, measured

Two separate costs, and only one of them is usually real.

**The lost overlap.** A whole-step activity starts each tool the moment the model asks for it, while
the stream is still going. Here the attempt has to return before any tool starts, because a workflow
cannot consume a stream. `packages/core/test/step-overlap-bench.test.ts` dials a mock model's stream
tail and a sleeping tool, so the number is the overlap and nothing else:

| stream tail after 1st call | tool | whole step | split step | loss | `min(tail, tool)` |
|---:|---:|---:|---:|---:|---:|
| 1 ms | 1500 ms | 1534 ms | 1519 ms | **-15 ms** | 1 ms |
| 200 ms | 1500 ms | 1538 ms | 1734 ms | **196 ms** | 200 ms |
| 1500 ms | 200 ms | 1534 ms | 1743 ms | **209 ms** | 200 ms |
| 1500 ms | 1500 ms | 1536 ms | 3045 ms | **1509 ms** | 1500 ms |
| 3000 ms | 1500 ms | 3038 ms | 4542 ms | **1504 ms** | 1500 ms |

So the loss per step is `min(stream tail after the first tool call, tool duration)`, within about
10 ms every time. It is zero when the tool call is the last thing in the stream, and that turns out
to be what real providers do.

`packages/temporal/scripts/stream-tail-probe.ts` measures the tail against the live API, timed from
the point the runner actually forks: the `tool-call` event, which the protocol layer emits once a
call's arguments are complete and parsed, not when its id first appears. Six coding-agent-shaped
prompts, including ones asking for three and five tools at once and ones asking the model to narrate
around the call:

| model | median tail | max tail | text after the first call |
|---|---:|---:|---|
| `gpt-4o-mini` (chat) | 0 ms | 50 ms | none, in any probe |
| `gpt-5-mini` (responses) | 33 ms | 36 ms | none, in any probe |
| `gpt-5` (responses) | 34 ms | 77 ms | none, in any probe |

A single tool call *is* the end of the stream, so there is nothing to overlap. A tail appears only
when the model asks for several tools at once, and is then just the time to stream calls 2..N: tens
of milliseconds, one to three percent of the stream. No model emitted a single character of text
after asking for its first tool.

Taken with the hand-offs below, the whole cost of the split is roughly 40-110 ms per step against
model calls of two to eight seconds. The overlap is not a reason to avoid it.

**The extra round trips.** Three activities per step instead of one means two more hand-offs.
Measured from workflow history on a loopback dev server (mean of four, one turn):

```
done runModelCall -> sched runToolCall   10 ms
done runToolCall  -> sched sealStep       3 ms
done sealStep     -> sched runModelCall   4 ms
done runModelCall -> sched sealStep       3 ms
```

About 5 ms per hand-off, so ~10 ms per step, against model calls of 1.2 s and 3.2 s in the same run.
Per-step Temporal overhead tracks worker-to-namespace distance, so this is the floor: it grows with
placement, and a laptop driving a remote namespace pays it many times over. Put workers next to the
namespace and the split is close to free.

Wall-clock totals are deliberately not quoted here. Model latency dominates and varies more between
two runs of the same cell than the effect being measured.

#### Verified

Live against a dev server and `gpt-5-mini`, with `OPENCODE_SESSION_EXECUTION=temporal` and
`OPENCODE_TEMPORAL_STEPPED=1`:

- A turn using one tool recorded five activities: `runModelCall`, `runToolCall`, `sealStep` for the
  step that called the tool, then `runModelCall`, `sealStep` for the answer. The tool ran once.
- Crash mid-tool: a turn ran `echo ... >> counter.txt` in one step and `sleep 60` in the next, and
  the serve process (with its embedded worker) was killed with the sleep in flight. On a fresh
  worker the interrupted `runToolCall` came back as **attempt 2**, `counter.txt` still held one
  line (the settled tool was not re-run), the interrupted call reached the model as "The outcome of
  this tool call is unknown", and the turn ran on to its answer.

### Two modes, one runner

The factory has exactly two modes, both driving the same `SessionRunner` over the same durable event
log. `OPENCODE_SESSION_EXECUTION=temporal` runs each session as a per-session Temporal workflow: the
`sessionTurn` supervisor (`supervisor.ts`) loops a `runTurnStep` drain, so each step (one provider
attempt + its tools) is its own activity with its own retry/timeout/visibility, reusing
`SessionRunner.runStep` (one iteration of `run`'s loop). Anything else (the default) runs in-process
on the proven `SessionRunCoordinator` (`execution/local.ts`) -- no server, no worker, no ports -- which
drives whole turns with `SessionRunner.run` and owns the wake/resume/interrupt lifecycle. That
coordinator is the same one the v1 server uses and has direct lifecycle tests
(`session-run-coordinator.test.ts`), so the default path reuses well-exercised code rather than a
second hand-written loop. The local integration wiring is covered by
`session-execution-local.test.ts`, and Temporal crash recovery by the crash test (in the
stacked scripts PR). The same wake/resume/interrupt contract also runs against the Temporal driver
through real workflows via the opt-in suite (`session-execution-temporal-contract.test.ts`).
Verified:
a create-then-read-then-reply turn recorded three `runTurnStep` activities under a `sessionTurn`
workflow and completed. (Earlier whole-turn-per-activity, stock-coordinator, and shared-supervisor
local modes were folded away in favor of the coordinator for local.)

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
  When an attempt is retried while the previous one is still alive (a network partition, or the
  backstop firing), the old attempt could briefly keep publishing until its heartbeat is rejected
  and the AbortSignal interrupts it. That overlap is now fenced: each drain claims the event log
  with an attempt token (`event_sequence.owner_id` via `claim()`), and a live durable append dies
  if a newer attempt has since claimed the log (the check is in `event.ts`, gated by the
  `EventOwner` context the drain provides). The owner is set activity-side from the run id and
  attempt, so it stays out of the workflow's deterministic input; the local driver uses a
  per-instance token. The projector's status guards still make any duplicate settlement a no-op.

Resume is verified end to end: it resolves on a healthy session and rejects on a failing
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
Verified: a worker comes up with no serve process and registers
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
and a revived attempt flips them back to pending; after a hard crash the pending row feeds the
retry. A pending ask whose session is abandoned lingers in the list until a reply retires it.
Verified by `packages/core/test/permission-durable.test.ts` (two independent stacks over one store).
The `question` tool still uses an in-process deferred and needs the same treatment.

## Shared, durable event store (any-worker resume)

The v2 engine event-sources each session to a SQLite store. By default that is a local file, so a
session can only be resumed on the host holding the file. Point every worker at one **shared** store
and any worker resumes any session: Temporal load-balances `runTurnStep` across the fleet,
`active` is visibility-backed, and the resumed worker reads the session purely from the shared log.

- **Same host**: set `OPENCODE_DB` to one absolute path on all workers (WAL + `busy_timeout` allow
  multiple processes). No code change.
- **Across hosts**: set `OPENCODE_DB_URL` to a libSQL URL (self-hosted `sqld` or Turso), with
  `OPENCODE_DB_AUTH_TOKEN` if needed. `packages/core/src/database/sqlite.libsql.ts` provides a
  `SqlClient` over `@libsql/client`; it speaks the same SQLite dialect, so the schema and all
  migrations are unchanged. Selection is one env check in `database.ts`.

Verified: worker A handles turn 1 (a code word), A is killed,
and a fresh worker B (same queue, same store, never saw the session) handles turn 2 and recalls the
code word, which it can only do by loading turn 1 from the shared store. The two turns run on two
distinct worker identities.

### Caveats

- Cold-start migrations serialize across processes (one `BEGIN IMMEDIATE` transaction wraps
  check-and-apply, so concurrent starts wait and then no-op). Running migrations once as a deploy
  step is still good practice for large fleets, and on a remote store it keeps cold starts from
  contending for the write lock. A fresh remote store also builds the whole schema inside one
  write transaction; a slow link or a server-side transaction timeout can kill that, one more
  reason to migrate before starting workers.
- Foreign keys are enforced on the local backend and best-effort on the shared one (SQLite
  defaults them off; a remote server applies the pragma per stream at most). Deletes on the
  shared store rely on the application-side cascades.
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
- Every durable event is its own interactive transaction, but streaming text and reasoning deltas
  are live-only: they broadcast in memory and write no row (verified by `session-runner.test.ts`
  "broadcasts provider ... deltas without storing projection rewrites", which streams 32 chunks and
  asserts zero delta rows while the context still rebuilds from the log). So a streaming turn does
  NOT pay a transaction per token. The durable cost of one step is the handful of boundary and tool
  events (`Step.Started`, a `Text.Ended` per block, the `Tool.*` pair per call, `Step.Ended`), each
  recorded as it settles. Over a remote store that is a small, bounded number of round trips per
  step, not one per token. Coalescing them into a single commit at step end would cut round trips
  further, but a mid-step crash would then lose the completed-tool records the resume path reuses,
  so per-event durability is kept on purpose.

### What resumes cross-host, and what does not

The runner rebuilds a session's LLM context purely from the shared DB (`SessionHistory.entriesForRunner`
then `toLLMMessages`); it never reads local disk to reconstruct context. So the **conversation** resumes
on any worker: messages, tool results (the bounded preview and structured output that the model sees),
prompt attachments (stored inline as `data:` URIs in the prompt), and credentials (`CredentialTable`)
all ride the shared store.

**The project working tree** now rides the store too. After each step capture the runner ships
the snapshot tree as an incremental git pack (`snapshot-sync.ts`, `snapshot_pack` table). Before a
drain runs, a worker missing the session's directory rebuilds the worktree from those packs
(`session/execution/worktree.ts`): uncommitted edits and untracked files included, checked out at
the same absolute path it was captured at (a uniform fleet layout). Ignored files and dependencies
are not captured, so a rebuilt tree may need an install step before `bash` behaves identically.
Worker affinity or a shared volume skips the materialization latency on warm paths; the packs
are the portable baseline that works with neither.

Host-local state that does NOT ride the DB, so it is not reconstructed on a different host:

- **The snapshot store (`${data}/snapshot`) and the retained full tool-output files
  (`${data}/tool-output`).** The runner never reads these to rebuild context: snapshot file-diffs are
  best-effort (`Effect.catch` to `undefined`), and the model sees the bounded tool-output preview, not
  the file. They only affect the diff/restore/revert features and full-output viewing. Point `${data}`
  (the XDG data dir) at shared storage to make them portable.

## Porting this pattern

The shape transfers to any agent engine; Temporal is one executor behind a seam the engine owns.

1. Find the engine's coordination seam and name it: here, four verbs (`active`, `wake`, `resume`,
   `interrupt`) behind one substitutable service, with the in-process coordinator as the default.
2. Make the turn body an idempotent, fenced step function: claim the log with an owner token,
   reuse recorded results on re-drive, encode errors so they survive a process boundary.
3. Write the executor as a thin workflow that loops the step as activities; keep the loop free of
   engine imports so it stays deterministic and sandbox-safe.
4. Forward settings as one evolvable input record; read configuration at layer build, never at
   module load.
5. Hold every executor to one conformance suite. Parity between the default and the durable path
   is a test, not a promise.
