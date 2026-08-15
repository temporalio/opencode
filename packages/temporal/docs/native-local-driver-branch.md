# Branch: 2026/08/opencode-native-local-driver

Base: `2026/08/opencode-temporal` (`8c140c0344`). This branch reworks how a v2 session's
`SessionExecution` runs, in two independent parts, backed by a new deterministic test harness.

## Why

The base branch runs BOTH execution modes on one hand-written per-session supervisor
(`execution/workflow-core.ts`, driven for local mode by `execution/local-driver.ts` and for Temporal
mode by `execution/temporal-workflow.ts`). Independent review (Codex, several rounds) found repeated
lifecycle races in that shared supervisor and in the local driver: concurrent successors during
interrupt cleanup, a completion barrier that did not cover resume-started drains, resume/wake lost at
the interrupt boundary, and duplicate provider turns on concurrent resume. Each fix uncovered
another. The pattern was clear: the coordination lifecycle is the dangerous part, and hand-rolling it
kept going wrong, while opencode already had a proven, tested implementation of exactly those
semantics (`SessionRunCoordinator`, `session/run-coordinator.ts`).

## What changed

### 1. Local mode runs on the proven SessionRunCoordinator

`routes.ts` now selects `SessionExecutionLocal` (`execution/local.ts`) for the default mode. It maps
`active` / `wake` / `resume` / `interrupt` onto `SessionRunCoordinator` and drains whole turns with
`SessionRunner.run`, the same lifecycle the v1 server already uses. The hand-written
`local-driver.ts` supervisor path was removed. Local mode is now the well-exercised default and its
correctness comes from reused, tested code rather than a second coordination loop.

`workflow-core.ts` is therefore Temporal-only.

### 2. A Temporal test harness, and a Temporal-mode correctness pass

Standing up a real Temporal test harness (`@temporalio/testing` time-skipping / local server running
the actual `sessionTurn` workflow with a mock activity) immediately caught a blocker and enabled a
correctness pass over the Temporal supervisor. Fixed:

- **Blocker: the workflow never drained a turn, and a session could serve only one turn.** The SDK's
  `condition(fn, timeout)` on `@temporalio/workflow` 1.21 leaks its timer-scope cancellation into the
  root scope on resolve, poisoning the next drain. The timed wait now races a no-timeout `condition`
  against a bare `sleep` and abandons the loser, cancelling no scope. (This, not the HTTP `steer`
  delivery, was the real cause of the "0 activities / turn never runs" symptom.)
- **Interrupt keeps serving.** `interrupt` now cancels only the current turn's child cancellation
  scope; the long-lived workflow keeps serving, so a wake/resume racing the interrupt drives a fresh
  turn on the same workflow instead of being lost to a doomed one.
- **resume joins.** All drains route through one in-flight promise; a resume joins it instead of
  queueing a second forced drain (no duplicate provider turns / tool side effects).
- **Explicit start intent.** `sessionTurn(sessionID, startWithWake)`; resume-with-start passes
  `[id, false]`, so a fresh resume does exactly one drain; the flag is carried across
  `continueAsNew`.
- **continue-as-new** counts every drain, gates on `allHandlersFinished()`, and a resume-driven
  rollover carries no spurious wake.
- **Real (root) workflow cancellation** is detected via the root scope and stops the supervisor,
  never keeps serving or continue-as-news.
- **Event-log owner token** is now unique per activity execution (`runId:activityId:attempt`), so a
  zombie attempt from an earlier step can no longer re-authorize past the fence.
- **Interrupt delivery failure** is surfaced as a defect instead of being swallowed as success.

Full status and the remaining (non-correctness) follow-ups are in
[temporal-supervisor-followups.md](./temporal-supervisor-followups.md).

## Commits

1. Run local sessions on the proven SessionRunCoordinator.
2. Add a Temporal test harness and fix a condition-cancellation blocker.
3. Fix the event-log owner token to be unique per activity execution.
4. Surface genuine interrupt-delivery failures instead of swallowing them.
5. Count all drains toward continue-as-new, including resume-driven ones.
6. Document the remaining Temporal supervisor coordination follow-ups.
7. Redesign the Temporal supervisor: interrupt keeps serving, resume joins.
8. Test the supervisor redesign: fake-runtime + real-Temporal harness.
9. Update the Temporal supervisor follow-ups doc: deep items now fixed.

## Verification

- `bun typecheck` (core + server): clean.
- Fake-runtime + regression suites: green (supervisor join/interrupt/rollover/root-cancel, owner
  token, interrupt classification, local coordinator integration, run-coordinator, and the
  session-runner suites).
- Real Temporal harness (run each file on its own; two native Temporal servers in one bun process
  segfault the runtime):
  - `temporal-harness-smoke.test.ts`: a wake drives a turn, then the workflow idle-retires.
  - `temporal-harness-interrupt.test.ts`: an interrupt cancels the current turn; a later wake runs a
    second turn on the same workflow.
  - `temporal-harness-multiturn.test.ts`: turn 1 completes, the supervisor parks in the idle wait
    (asserted via a `TimerStarted` history event), then a wake drives turn 2.
- Live: with the blocker fixed, a real `gpt-5-mini` turn completes end-to-end against a dev server in
  Temporal mode (previously it recorded zero activities).

Each substantive change was reviewed with Codex (twice per round); the redesign went through several
review-and-fix rounds until both runs returned no remaining correctness must-fix for a fresh
deployment.

## Known limitations

- **Rolling deploys need Temporal versioning/patching or draining old executions**, because the
  Temporal workflow's command sequence changed. Fresh deployments are unaffected. See the follow-ups
  doc.
- The full HTTP-stack end-to-end (create + prompt via `opencode serve`) has an unrelated `steer`
  prompt-delivery quirk in this checkout that predates the branch; it does not affect the automated
  tests, which drive the engine directly.
