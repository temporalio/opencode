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
the v1 server uses, with no server and no ports (see [Two modes, one
runner](#two-modes-one-runner)).
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

- `packages/temporal/src/workflow.ts`: the pure per-session workflow (the Temporal equivalent of
  `SessionRunCoordinator`: `wake`/`force` drive one drain, wakes coalesce, quiescent runs end).
- `packages/temporal/src/activities.ts`: the `runTurnStep` activity (heartbeats; forwards
  cancellation;
  injects the attempt's event-log owner token).
- `packages/temporal/src/executor.ts`: the `SessionExecution` layer + node: `wake` →
  `signalWithStart`, `resume` →
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
code, so a retry policy, a timeout, an approval or a budget can live where the model-to-tools
handoff
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
- **A call that already started is not silently repeated.** Whether a side effect may have happened
  is read off the log: `runToolCall` publishes `Tool.Called` before it runs the tool, so a call the
  log shows as running is one a dispatch was already inside. Then only a tool declaring `idempotent`
  runs again; anything else is reported to the model as an unknown outcome. This is the rule the
  crash-resume path already followed, off the same evidence.

  The attempt number would answer the same question far less precisely. It counts every way a
  dispatch can die, including the ones that never reached the tool, so a call nobody had touched
  would come back to the model as an unknown outcome. Publishing the call at dispatch also moves the
  fence in front of the side effect: under a superseded owner the publish fails and the tool never
  runs, where before it ran and then lost its result.

  It is also what closes the zombie window, which the settled-result check on its own does not: that
  check is a read then a write, so an attempt that lost its heartbeat but kept running could race a
  retry past it. For the case that matters, a non-idempotent side effect running twice, it cannot
  happen anyway, because the second dispatch refuses to run the tool at all. What can still race is
  which truthful outcome reaches the model, the zombie's real result or the "unknown", and both
  describe something that did happen. Reporting success for a tool that never ran is not reachable.

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

#### What has been exercised, and what has not

Verified live (dev server, `gpt-5-mini`, stepped mode on):

- **A step's calls fan out and share one log owner.** Four `read` calls in one step became four
  concurrent `runToolCall` activities, started within 2 ms of each other and overlapping, all four
  results durable, zero activity failures. A fenced write would have died and shown as a failed
  activity, so this is the owner-token design holding under real concurrency, which is the thing it
  exists for.
- **Interrupt stops the turn, not the session.** With `sleep 90` in flight, `POST /interrupt`
  returned 204, the child process died, the call was closed as `Tool execution interrupted` so the
  next attempt is not a poisoned request, the workflow stayed RUNNING, and the next prompt answered
  normally.
- **An approval holds only the tool that is waiting.** Reading a `*.env` file parks on the default
  agent's `ask` rule. While the human deliberated, `runModelCall` was **completed** and
  `runToolCall` was the only outstanding activity. Replying `once` completed the tool and the turn.
  Under the whole-step mode the entire step, model call included, sits in one activity for the whole
  of that wait.

- **A tool activity rebuilds a worktree it has never seen.** Each of the three drains calls
  `worktrees.ensure`, so a tool call landing on a worker without the project tree materializes it
  from the snapshot packs. Checked by deleting the entire working directory between two turns of a
  live session: the next turn's tool activity rebuilt both files, the `read` completed, and the
  model
  answered with the file's contents. Worker affinity (below) skips the
  materialization on warm paths;
  the packs are the baseline that works without it.

Covered by tests rather than by a live run:

- **A compaction restart keeps deferring.** The `deferTools` flag is threaded through both
  `ContinueAfterCompaction` recursions; a first version dropped it, which would have silently run
  tools inline on a compacting step.
- **A provider error ends the turn.** The attempt's own continuation decision is carried to the
  seal, because the log cannot tell a failed attempt from one that wants another step.
- **A declined permission halts the turn** rather than reaching the model as one failed tool. Both
  halves matter: the runner raises it as an interrupt, and the workflow tells that failure apart
  from a tool that merely failed.

Not covered, stated plainly:

- **A second machine.** The rebuild above is a worker meeting a missing tree, which is the mechanism
  that matters, but it ran in one process on one host.

#### The contract covers both modes

The conformance suite is the executable definition of what an executor must do, and stepped mode has
to satisfy the same one or the two Temporal modes drift and only the tested one is trustworthy:

```bash
temporal server start-dev --port 7237 --headless &
cd packages/temporal
# whole step per activity
OPENCODE_CONTRACT_TEMPORAL=1 \
  bun test --timeout 180000 test/session-execution-temporal-contract.test.ts
# model call, one activity per tool, seal
OPENCODE_CONTRACT_TEMPORAL=1 OPENCODE_TEMPORAL_STEPPED=1 bun test --timeout 180000 \
  test/session-execution-temporal-contract.test.ts
```

Both modes pass, and so does the local coordinator in core's own `bun test`. Running it the first
time was worth the effort: **stepped mode failed two scenarios**, on a case none of the live testing
had reached. `countingModel` streams a step start and a step finish and no content at all, so the
publisher never mints an assistant message. The whole-step path survives that because it mints one
inside `Step.Ended` via `startAssistant()`; the seal, running in another process with no publisher,
searched the projection, found nothing, and left the turn open forever. The fix carries the
assistant message id out of the attempt the same way the tool call ids are carried.

One scenario is there for this split in particular: a turn that asks for a tool, runs it, and goes
back to the model with the result. Everything else in the suite settles without a tool, so the piece
the split actually changes, one activity per call rather than one for the whole step, would have had
no scenario that both modes must pass.

#### Worker affinity (`OPENCODE_TEMPORAL_WORKTREE_AFFINITY=1`)

Off by default. Without it every worker polls one queue, and a worker drawing a session whose tree
it
has never seen rebuilds that tree from snapshot packs. That is the portable baseline and it works.
Affinity avoids the rebuild by routing instead: the queue name is derived from the session's
directory, and only workers serving that directory poll it.

```bash
# a worker declares the tree it serves; defaults to the process directory
OPENCODE_TEMPORAL_WORKTREE_AFFINITY=1 OPENCODE_TEMPORAL_WORKTREE=/srv/trees/acme \
  OPENCODE_TEMPORAL_ROLE=worker ... bun run packages/server/src/worker.ts
```

The queue is keyed on the session's `location.directory`, not the project root, because that is the
tree `worktrees.ensure` has to produce and two sessions in one project can sit in different
directories. Paths are resolved through `realpath` first: on macOS `/tmp/x` and `/private/tmp/x` are
one tree, and a client and a worker that disagreed would sit on two queues and the session would
hang
with nothing to show for it.

**This trades availability for latency, which is why it is opt-in.** With affinity on, a session
whose tree has no worker polling does not fall back to another worker. It waits. Reconstruction is
what makes any worker able to serve any session, and turning affinity on is choosing not to use it.

Two consequences to plan for, both silent:

- **A worker serves one tree.** In the default `role=both` deployment the embedded worker polls the
  queue for the process directory, so a session in another project has no poller. Point
  `OPENCODE_TEMPORAL_WORKTREE` at the project root, not at a subfolder, since the key is the project
  worktree.
- **Flipping the flag strands workflows already running.** A workflow keeps the task queue it
  started on for life, and its activities inherit it. Restarting workers with the flag changed
  leaves in-flight sessions with nobody polling their queue. They do not fail; they stay `RUNNING`
  forever, because the workflow task that would run their idle timer is never picked up either.
  Drain before flipping, in either direction.

Both processes log the queue they use (`pollQueue` on a worker, `worktree` on the client), so a
mismatch shows up as two names in the logs rather than as a session that never runs.

Verified live, all three halves:

- a worker serving the session's tree ran the turn, and the workflow sat on the derived queue
- with only a worker serving a *different* tree alive, the next prompt was not answered, and the
  queue showed a workflow backlog of one, aged 50 seconds
- bringing the right worker back drained it and the answer arrived, so the work waits rather than
  being lost

#### Durable events across a process boundary

Worth stating separately, because it is **not** specific to stepped mode: it is a property of
running a standalone worker at all, and the mechanism is in `event.ts`.

`commitDurableEvent` publishes its wake in-process and `subscribeDurable` registers in that same
process's map, so a commit in another process cannot wake a tail. Split into
`OPENCODE_TEMPORAL_ROLE=client` serve plus a standalone worker, a turn ran correctly and its log was
complete, but a client subscribed *live* saw exactly one event, `prompt.admitted`, the only one the
serve process writes itself. A UI attached to serve saw a prompt admitted and then silence.

Token deltas are not part of this. `Text.Delta`, `Reasoning.Delta` and `Tool.Input.Delta` are
live-only and never reach the durable log, so what crosses a process boundary is block-level:
`step.started`, `tool.called`, `tool.success`, `step.ended`. The durable tail now also re-reads on a
tick (`LayerOptions.livePollInterval`, default one second, 0 to disable). In-process commits
still wake it instantly, so latency is unchanged where it already
worked; the tick only catches what the wake cannot see. An idle subscriber costs one indexed read
per period, and a tick with nothing new emits nothing. The same split now delivers the worker's
`step.started`, `tool.called`, `tool.success` and `step.ended` to a live subscriber.

### Two modes, one runner

The factory has exactly two modes, both driving the same `SessionRunner` over the same durable event
log. `OPENCODE_SESSION_EXECUTION=temporal` runs each session as a per-session Temporal workflow: the
`sessionTurn` supervisor (`supervisor.ts`) loops a `runTurnStep` drain, so each step (one provider
attempt + its tools) is its own activity with its own retry/timeout/visibility, reusing
`SessionRunner.runStep` (one iteration of `run`'s loop). Anything else (the default) runs in-process
on the proven `SessionRunCoordinator` (`execution/local.ts`) -- no server, no worker, no ports --
which
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
which the provider rejects, a retry poison loop. And if the crashed step had already dispatched
tools
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
  and the AbortSignal interrupts it. That overlap is fenced per attempt in whole-step
  mode: the drain claims the event log
  with an attempt token (`event_sequence.owner_id` via `claim()`), and a live durable append dies
  if a newer attempt has since claimed the log (the check is in `event.ts`, gated by the
  `EventOwner` context the drain provides). In stepped mode a step's three activity kinds share one
  token, so the fence separates steps but not writers inside a step; what keeps the projection right
  there is the projector applying a tool result only while the part is still open, in the same
  transaction as the append. The owner is set activity-side from the run id and
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
process (a standalone worker's ask could never be answered) and gone on restart. A pending ask is
now
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

The runner rebuilds a session's LLM context purely from the shared DB
(`SessionHistory.entriesForRunner`
then `toLLMMessages`); it never reads local disk to reconstruct context. So the **conversation**
resumes
on any worker: messages, tool results (the bounded preview and structured output that the model
sees),
prompt attachments (stored inline as `data:` URIs in the prompt), and credentials
(`CredentialTable`)
all ride the shared store.

**The project working tree** now rides the store too. After each step capture the runner ships
the snapshot tree as an incremental git pack (`snapshot-sync.ts`, `snapshot_pack` table). Before a
drain runs, a worker whose tree is missing or older than the store's newest capture builds it from
those packs (`session/execution/worktree.ts`): uncommitted edits and untracked files included,
checked out at the same absolute path it was captured at (a uniform fleet layout). Ignored files
and dependencies are not captured, so a rebuilt tree may need an install step before `bash` behaves
identically. Worker affinity (below) or a shared volume skips the materialization latency on warm
paths; the packs are the portable baseline that works with neither.

Two rules bound what that refresh may touch, because checking a stored tree out over the wrong one
destroys work. A tree is moved only when a host-local note (`snapshot/tip.ts`) says this host is
behind the store, so a host holding a capture that never shipped is left as it is. And it is moved
only when this host built the tree from packs, so a checkout the host already had, a developer's own
working copy, is never rewritten: that case is logged and left alone. What stays open is the tools
of ONE step running on two hosts, since nothing captures their writes until the step is sealed.
Affinity is what keeps a step's tools on one tree.

Host-local state that does NOT ride the DB, so it is not reconstructed on a different host:

- **The snapshot store (`${data}/snapshot`) and the retained full tool-output files
  (`${data}/tool-output`).** The runner never reads these to rebuild context: snapshot file-diffs
  are
  best-effort (`Effect.catch` to `undefined`), and the model sees the bounded tool-output preview,
  not
  the file. They only affect the diff/restore/revert features and full-output viewing. Point
  `${data}`
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
