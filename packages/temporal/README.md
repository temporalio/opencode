# @opencode-ai/temporal

A Temporal executor for opencode's `SessionExecution` seam. With it, a coding session is a durable
Temporal workflow: it survives the loss of the worker running it, can run detached from any client,
and resumes on any worker that shares its store. opencode's loop, tools, model, storage and HTTP API
are untouched, and nothing Temporal lives in core.

## How it fits together

Durability is a choice of executor behind the substitutable `SessionExecution` service. Both
executors drive the same `SessionRunner` over the same durable event log; the difference is who
schedules the steps.

- The default executor runs turns in-process on `SessionRunCoordinator` (`core/src/session/execution/local.ts`).
- `OPENCODE_SESSION_EXECUTION=temporal` runs each session as a per-session workflow with one
  activity per step. `executor.ts` wires the client and the worker, `supervisor.ts` is the loop,
  `workflow.ts` adapts it to the Temporal sandbox, and `drain.ts` is the step body.

What an executor must do is defined executably: the conformance suite in
`packages/core/test/lib/execution-conformance.ts` runs the same wake, resume and interrupt scenarios
against the local executor in core's tests and against this package through real workflows.

Taking at-least-once execution seriously forces six things, all of which live in core because the
local executor benefits from them too:

1. **State is shareable.** Any worker can resume any session only if the event log is not
   host-local: a libSQL backend, atomic remote writes, a busy retry, serialized migrations.
2. **Re-drives are safe.** A retried step reuses completed tool results, re-runs only tools declared
   `idempotent`, and fails the rest for the model to redo. The step loop is bounded by
   `loop-guard.ts`: a step ceiling and a repeated-identical-call detector.
3. **Two writers are fenced.** Each drain claims the event log with an attempt token, and a
   superseded attempt's appends die.
4. **The worktree travels.** Snapshot trees ship as incremental git packs, and a worker without the
   project tree rebuilds it before the run.
5. **Human-in-the-loop is durable.** A pending permission or question is a row in the shared store,
   answerable from any process, adopted by re-drives and expired when abandoned.
6. **Errors survive the boundary.** `resume` rejects with the exact tagged `RunError` rebuilt from
   the failure details, and a user decline is non-retryable.

## Run it

```bash
temporal server start-dev --port 7237
OPENAI_API_KEY=... OPENCODE_SESSION_EXECUTION=temporal TEMPORAL_ADDRESS=127.0.0.1:7237 \
  bun run --cwd packages/cli src/index.ts serve --port 4601
```

Create a session with `POST /api/session` and prompt it with `POST /api/session/:id/prompt`. Each
session runs as the workflow `session-exec-<sessionID>`.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OPENCODE_SESSION_EXECUTION` | local | `temporal` selects this executor. |
| `TEMPORAL_ADDRESS` | `127.0.0.1:7237` | The Temporal frontend. |
| `TEMPORAL_NAMESPACE` | `default` | |
| `OPENCODE_TEMPORAL_TASK_QUEUE` | `opencode-session-exec` | |
| `OPENCODE_TEMPORAL_ROLE` | `both` | `both` hosts the worker and the client in one process. `client` drives workflows without a worker. `worker` runs activities with no HTTP surface. |
| `OPENCODE_SESSION_IDLE_TIMEOUT` | | How long an idle session's workflow stays open. Local mode honors the same variable. |
| `OPENCODE_TEMPORAL_STEPPED` | off | `1` runs each step as a model call, one activity per tool call, and a seal. |
| `OPENCODE_TEMPORAL_SERIAL_TOOLS` | derived | Run a step's tool calls one at a time. On by default only where two hosts could write one step's tree: a shared store. |
| `OPENCODE_EVENT_POLL_MS` | | How often a live subscriber re-reads the log for events another process appended. `0` turns the tick off. |
| `OPENCODE_DB` | | One absolute path shared by every process on a host. |
| `OPENCODE_DB_URL`, `OPENCODE_DB_AUTH_TOKEN` | | A libSQL URL for a store shared across hosts. Takes precedence over `OPENCODE_DB`. |

Settings are read when the executor's layer is built, never at module load, so an embedder or a
test can provide `TemporalConfig.Service` instead.

## One step per activity

The supervisor loops a `runTurnStep` activity. Each step is one provider attempt plus the tools it
asked for, with its own retry, timeout and visibility. `SessionRunner.runStep` is one iteration of
the same loop `run` uses, so the two executors do not diverge.

A re-drive resumes from the durable log rather than repeating work. `Tool.Called` is recorded before
a tool runs, so once a crashed step shows a dispatched tool, re-streaming the model could run that
side effect twice. The step is closed from the log instead: completed tool results are kept, tools
declared `idempotent` (`read`, `glob`, `grep`) are run again for a real result, and anything else
still open is failed for the model to redo, since the runner cannot know whether it already ran. A
step that crashed before dispatching anything is safe to stream again, and a dangling tool call left
by an interrupted attempt is closed on every step entry so no request ever carries a `tool_use`
without its `tool_result`.

Activity bounds: the heartbeat is the liveness bound, so a dead worker is re-driven within seconds.
`startToCloseTimeout` is only a backstop for a drain that hangs while its process stays alive, and
it is long on purpose, because a bound that kills a live turn opens a two-writer window. That
window is fenced regardless: a drain claims the event log with an attempt token, and a live append
under a superseded token dies.

`resume` awaits the forced run through an update-with-start and surfaces its failure as the exact
tagged `RunError`, encoded through a `Schema.Union` of every member in `run-error-codec.ts`.
`active` is the set of open per-session workflows, so it survives a restart of the serve process.
A user decline inside an activity is a non-retryable failure, so the workflow does not re-drive a
turn the user stopped.

## A step as three activities

`OPENCODE_TEMPORAL_STEPPED=1` splits a step into three activities instead of one:

```
runModelCall  ->  runToolCall (one per call, concurrent)  ->  sealStep
```

`SessionRunner.runModelCall` performs the attempt, records each call as `Tool.Called`, and hands the
calls back rather than running them. `runToolCall` settles one call. `sealStep` takes the end
snapshot, diffs it against the start, and publishes `Step.Ended`. The loop between them is workflow
code (`stepped-turn.ts`), so a retry policy, a timeout, an approval or a budget can sit between the
model asking for a tool and the tool running. Each activity carries its own bounds: sealing does not
inherit a turn-sized backstop, and one tool waiting on a human no longer holds the attempt and its
sibling tools under a single timeout.

The supervisor is the same. Wake, interrupt, idle self-termination and continue-as-new only ever
called one `runTurnStep`, so the stepped mode supplies a different one. The mode rides the workflow
input, so a session that rolls over keeps it.

Three rules carry the split:

- **One owner token per step, not per activity.** The event log fences a publish behind the current
  owner, so a step's writers share one token. Only `runModelCall` claims; the tool and seal
  activities publish under the token it returns.
- **A call that already started is not silently repeated.** `runToolCall` publishes `Tool.Called`
  before it runs the tool, so a call the log shows as running is one a dispatch was already inside.
  On a second dispatch only a tool declaring `idempotent` runs again; anything else reaches the model
  as an unknown outcome. The attempt number cannot stand in for this: it counts every way a dispatch
  can die, including the ones that never reached the tool.
- **A stop closes the calls it cut short, and a hand-off does not.** A dispatch closes its own call
  when the cancellation means the turn is over. When it means this attempt is being handed to another
  (a worker shutting down, a pause, a heartbeat timeout), the call is left as it was found, or the
  next attempt would read a closed call and never run the tool.

A declined permission crosses the activity boundary as its own error type, so the workflow tells a
refusal apart from a tool that failed. The whole-step path still raises it as an interrupt, which is
the behaviour its own tests pin, so that drain says so at the boundary.

Each tool call ships the tree from the host that ran it, because the seal can land on any worker and
a capture there would miss what the tool wrote. Where two hosts could run one step's tools, they run
one at a time (`OPENCODE_TEMPORAL_SERIAL_TOOLS`), or the second to ship would publish a tree without
the first's work.

What the split costs is the overlap between the model's stream and its own tools: a whole-step
activity starts each tool the moment the model asks for it, while here the attempt returns first.
The tools of one step still run concurrently with each other. Against live providers that tail is
under a tenth of a second, and no model tested emitted text after asking for its first tool.

## Running workers separately

By default the serve process hosts both the worker and the client. To scale workers apart from the
HTTP server, run standalone workers and point serve at the client role:

```bash
# serve drives workflows, hosts no worker
OPENCODE_TEMPORAL_ROLE=client OPENCODE_SESSION_EXECUTION=temporal ... serve --port 4601

# one or more standalone activity workers (no HTTP surface)
OPENCODE_TEMPORAL_ROLE=worker OPENCODE_SESSION_EXECUTION=temporal \
  TEMPORAL_ADDRESS=127.0.0.1:7237 OPENCODE_DB_URL=... \
  bun run packages/server/src/worker.ts
```

`packages/server/src/worker.ts` builds the same application context serve uses, without the HTTP
API, so a worker resumes a session purely from the shared store. A compiled binary cannot carry the
worker's bundler, so a packaged serve runs as `client` next to standalone workers.

## Durable asks

A pending permission or question is a row in the shared store (`permission_request`,
`question_request`), not only a deferred in the asking process's memory. The blocked tool races its
local deferred against a poll of the row, so a reply from any process sharing the store unblocks
it, and the list endpoints read the rows, so serve can show asks raised inside a standalone worker.

Tool-originated asks have deterministic ids (session, call and the ask's own fields), so a re-driven
activity adopts the same pending row instead of filing a duplicate, and an approval that landed while
the asker was dead short-circuits the retry. Graceful shutdown retires the process's pending asks as
`expired`, and a revived attempt flips them back. A row untouched for a day is treated as abandoned
and swept on read.

Replies keep their meaning: `once` and `always` approve, an `always` rule retro-approves other
pending asks it covers, and `reject` declines and cascades to the session's other pending asks.

## A shared store

By default the event store is a local SQLite file, so a session can only resume on the host that
holds it. Point every process at one store and any worker resumes any session:

- **Same host**: set `OPENCODE_DB` to one absolute path on all processes. WAL and `busy_timeout`
  allow several processes.
- **Across hosts**: set `OPENCODE_DB_URL` to a libSQL URL (self-hosted `sqld` or Turso), with
  `OPENCODE_DB_AUTH_TOKEN` if needed. `sqlite.libsql.ts` speaks the same SQLite dialect, so the
  schema and the migrations are unchanged.

Caveats of the shared backend:

- Cold-start migrations serialize across processes inside one `BEGIN IMMEDIATE` transaction, so
  concurrent starts wait and then no-op. Migrating once as a deploy step is still the better shape
  for a fleet, and on a remote store it keeps cold starts from contending for the write lock.
- Foreign keys are enforced on the local backend and best-effort on the shared one. Deletes on the
  shared store rely on the application-side cascades.
- The local-file PRAGMAs are skipped for the shared backend, which manages journaling itself.
- Multi-statement writes commit atomically on both backends. The libSQL client runs each statement
  as its own request, so a transaction is routed through one interactive libSQL transaction rather
  than emitted as `BEGIN` and `COMMIT` statements.
- Streaming text and reasoning deltas are live-only and write no row, so a streaming turn does not
  pay a transaction per token. The durable cost of a step is its handful of boundary and tool
  events, recorded as each settles. Coalescing them into one commit at step end would lose the
  completed-tool records the resume path reuses, so per-event durability is kept.

### Live events across processes

A commit publishes its wake in-process, so a subscriber on the serve process cannot see events a
standalone worker appended: a live tail would show the prompt admitted and then silence. A durable
tail can also re-read on a tick, which catches what the wake cannot see while in-process commits
still wake it instantly. The tick is on only in the durable executor's composition root
(`EventV2.pollingNode`), because a deployment that runs in one process wakes its own subscribers
and would pay a query per second per subscribed session for events that cannot exist. Token deltas
are live-only and never cross a process boundary either way; what does is block-level:
`step.started`, `tool.called`, `tool.success`, `step.ended`.

## What resumes cross-host, and what does not

The runner rebuilds a session's model context purely from the store, so the conversation resumes
on any worker: messages, tool results, prompt attachments and credentials all ride the shared store.

The project working tree rides it too. After each step capture the runner ships the snapshot tree
as an incremental git pack (`snapshot-sync.ts`, the `snapshot_pack` table), and a worker missing
the session's directory rebuilds it from those packs before a drain runs
(`session/execution/worktree.ts`), uncommitted edits and untracked files included, at the same
absolute path it was captured at. Ignored files and dependencies are not captured, so a rebuilt
tree may need an install step before `bash` behaves identically.

Host-local state that does not ride the store: the snapshot object store and the retained full
tool-output files under the data directory. The runner never reads these to rebuild context; they
only affect the diff, restore and full-output features. Point the data directory at shared storage
to make them portable.

One rule bounds what the rebuild may touch, because checking a stored tree out over the wrong one
destroys work: a tree is moved only when a host-local note (`snapshot/tip.ts`) says this host is
behind the store, so a host holding a capture that never shipped is left alone. Packs are ordered
by their chain, not by `time_created`, because hosts do not agree on the time.
