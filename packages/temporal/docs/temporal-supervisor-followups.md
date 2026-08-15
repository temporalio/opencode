# Temporal supervisor: follow-ups

Independent review (Codex, several rounds) enumerated correctness issues in the Temporal-mode
session supervisor (`packages/core/src/session/execution/workflow-core.ts` + `temporal.ts`). Local
mode is unaffected: it runs the proven `SessionRunCoordinator` (`execution/local.ts`).

A deterministic test harness now exists for this code
(`packages/core/test/temporal-harness-smoke.test.ts`, `@temporalio/testing` time-skipping running the
real workflow with a mock activity), plus fake-runtime unit tests of the supervisor loop. Every item
below is reproducible/verifiable through one of those.

## Fixed

- **Blocker: workflow never drained a turn.** On `@temporalio/workflow` 1.21, `condition(fn, timeout)`
  with `fn` already true left the `CancellationScope` cancelled, so the next `condition()` threw and
  the workflow completed with zero activities. Fixed by short-circuiting an already-true predicate in
  the `condition` adapter (`temporal-workflow.ts`). This was the real cause of the "0 activities /
  turn never runs" symptom (not the HTTP `steer` delivery).
- **Owner-token collision (event-log fence).** Token was `runId#attempt`; activity attempts restart
  per step, so tokens repeated across steps and a zombie attempt could re-authorize. Now
  `runId:activityId:attempt` (`temporal-activities.ts`).
- **Interrupt delivery failure reported as success.** A genuine signal failure was swallowed to
  `void`; now surfaced as a defect (`temporal.ts`, `classifyInterruptError`).
- **continue-as-new ignored resume drains.** Only wake-loop drains counted toward the bound, so a
  resume-heavy workflow grew history without rolling over. Now every drain counts and the main loop
  rolls over on `rolloverPending` (`workflow-core.ts`).

## Open (deep coordination pass — interlocking, do together)

These three entangle (start intent ↔ continue-as-new state ↔ resume join ↔ interrupt generations),
so they are best done as one coherent supervisor pass, mirroring `SessionRunCoordinator`'s proven
semantics, rather than piecemeal.

### 1. Resume/wake lost at the interrupt boundary (critical)

`interrupt` sets `stopping` and cancels the workflow's scope; `drainTurn` then returns without a
successor, and the `resume` update handler ignores `stopping`. With `USE_EXISTING`, a resume admitted
after the interrupt attaches to the doomed workflow and is cancelled/abandoned instead of awaiting a
successor; a wake in the same window is dropped. Reference: `run-coordinator.ts` `run` (stopping
branch awaits cleanup then starts a successor) and `settle` (a wake during cleanup starts a
successor).

Fix sketch: keep the workflow root alive and cancel a per-drain `CancellationScope` instead; model
`running → stopping → retired` generations explicitly; a resume while stopping awaits cleanup then
forces exactly one successor; a wake while stopping registers one non-forced successor. Consider
making `interrupt` an Update so it acknowledges only after cleanup.

Test: harness — start, `interrupt`, then `executeUpdateWithStart(resume)`; assert it runs on a
successor rather than receiving cancellation.

### 2. Concurrent resumes serialize into duplicate forced turns (critical)

Each `resume` handler calls `drainTurn(true)` independently; `drainTurn` serializes on `draining` but
does not JOIN the active drain. Two concurrent resumes (or a resume during a wake drain) therefore
produce two forced drains, and a forced first step bypasses the "no eligible work" check
(`runner/llm.ts`) and calls the provider anyway — duplicate provider turns, charges, transcript
entries, tool side effects. Reference: `run-coordinator.ts` `run` joins the existing `done`.

Fix sketch: route all drains through a single in-flight promise; resume joins it (or forces one only
when idle). A wake that only joins a resume-started drain must still get one follow-up drain (the
`joined` re-arm). Both were prototyped earlier and unit-tested with a fake runtime; fold into the
generation pass.

Test: harness — two concurrent `resume`s → assert exactly one `runTurnStep` activity.

### 3. Fresh-resume spurious drain (medium)

The supervisor always starts `pendingWake = true`, but a resume-with-start carries no wake, so a
fresh resume does its forced drain AND a second, no-op wake drain (extra activity/history; not
incorrect). Coupled to continue-as-new: the current `pendingWake = true` start is also what keeps a
continued-as-new run from losing queued work, so fixing this needs explicit start intent threaded
through the workflow args and `continueAsNew`.

Fix sketch: start `pendingWake` from an explicit initial-intent arg (wake vs resume vs rollover);
carry pending state across `continueAsNew` rather than manufacturing a wake every run.

Test: harness — `executeUpdateWithStart(resume)` on a fresh workflow → assert exactly one drain.

## Also noted (lower priority)

- `active` (visibility query) reports an idle-but-parked workflow as running for the idle window; a
  workflow query or search attribute exposing `pendingWake || draining || handlers > 0` would be
  more accurate.
