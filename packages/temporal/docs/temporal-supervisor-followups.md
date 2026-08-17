# Temporal supervisor: status and follow-ups

Independent review (Codex, several rounds) enumerated correctness issues in the Temporal-mode session
supervisor (now `packages/temporal/src/supervisor.ts` + `workflow.ts` + `executor.ts`).
Local mode is unaffected: it runs the proven `SessionRunCoordinator`
(`execution/local.ts`).

A deterministic test harness now exists for this code: fake-runtime unit tests of the supervisor loop
(`session-supervisor.test.ts`, `session-supervisor-rollover.test.ts`) and real-Temporal tests via
`@temporalio/testing` (`temporal-harness-{smoke,interrupt,multiturn}.test.ts`). Every item below is
reproducible/verifiable through one of those.

## Fixed

- **Blocker: the workflow never drained a turn (and a session could serve only one turn).** On
  `@temporalio/workflow` 1.21, `condition(fn, timeout)` cancels its internal timer scope on resolve
  and that cancellation LEAKS into the parent/root scope, so the next `condition()`/drain saw a
  cancelled scope. The supervisor completed with zero activities, or (after the first turn) a second
  turn's drain was born cancelled. Fixed by not using the SDK's timed `condition` at all: the adapter
  short-circuits an already-true predicate and, for a real wait, races a no-timeout `condition`
  against a bare `sleep` and abandons the loser -- cancelling no scope, so nothing leaks. (This, not
  the HTTP `steer` delivery, was the real cause of the "0 activities" symptom.)
- **Resume/wake lost at the interrupt boundary.** `interrupt` used to end the workflow, so a
  resume/wake admitted in the interrupt→completion window (USE_EXISTING) attached to a doomed run.
  Now `interrupt` cancels only the current turn's child cancellation scope (`runInDrainScope`); the
  long-lived workflow keeps serving, and a later wake/resume drives a fresh turn on the same
  workflow. Verified: `temporal-harness-interrupt.test.ts`.
- **Concurrent resumes duplicated forced turns.** `drainTurn` serialized but did not JOIN. Now every
  drain routes through one in-flight promise and resume joins it (no duplicate provider turns / tool
  side effects), mirroring `SessionRunCoordinator.run`.
- **Fresh-resume spurious drain.** Explicit start intent: `sessionTurn(sessionID, startWithWake)`,
  resume-with-start passes `[id, false]`, and `pendingWake` is carried across `continueAsNew`, so a
  fresh resume does exactly one drain.
- **continue-as-new ignored resume drains / manufactured a wake.** Every drain now counts toward the
  bound, the main loop rolls over on `rolloverPending`, a resume-driven rollover carries no spurious
  wake, and completion/continue-as-new gate on `allHandlersFinished()` so an in-flight update result
  is never abandoned.
- **Real workflow (root) cancellation.** Detected via the root scope's `consideredCancelled`
  (reliable now that the timed wait cancels no scope): a root cancellation stops the supervisor and
  never keeps serving or continue-as-news; a per-turn interrupt (child scope) does not trip it.
- **Owner-token collision (event-log fence).** Token was `runId#attempt`; activity attempts restart
  per step, so tokens repeated across steps and a zombie attempt could re-authorize. Now
  `runId:activityId:attempt` (`temporal-activities.ts`).
- **Interrupt delivery failure reported as success.** A genuine signal failure was swallowed to
  `void`; now surfaced as a defect (`temporal.ts`, `classifyInterruptError`).

## Remaining follow-ups (not correctness bugs on a fresh deployment)

- **Replay / versioning for rolling deploys.** This revision changes command-producing behavior
  (single-flight resume, the timed wait no longer cancels a timer, `continueAsNew` carries a second
  arg). A workflow already running under the OLD code can replay nondeterministically under the new
  code. Before a rolling deploy, use Temporal Worker Versioning / patching or drain old executions. A
  fresh deployment is unaffected.
- **`active` reports idle-but-parked workflows.** The visibility query lists running `sessionTurn`
  workflows, so a session that finished a turn but hasn't idled out yet still shows as active. A
  workflow query or search attribute exposing `pendingWake || inFlight || resumers > 0` would be more
  precise if the API needs it.
- **Broader real-harness coverage.** The harness proves draining, idle retirement, interrupt
  keep-serving, and park-then-wake. Concurrent real updates → one activity, resume-with-start → one
  drain, and a real root-cancel-during-drain are covered at the fake-runtime level; promoting them to
  the real harness would add belt-and-suspenders confidence.
