// The Temporal driver's per-session supervisor, expressed over a runtime interface so the SDK's
// condition/signals/updates/activities/cancellation plug in (workflow.ts) and it stays
// unit-testable off a live cluster (a fake runtime). Local mode does NOT run this loop: it uses the
// proven SessionRunCoordinator directly (execution/local.ts). The two modes share SessionRunner and
// the durable event log, not this supervisor.
//
// MUST stay pure: the Temporal driver bundles this into the workflow sandbox, so no `effect`, no
// `@opencode-ai/core` runtime imports, no Node builtins. Type-only imports are erased and safe.
//
// Semantics mirror SessionRunCoordinator (run-coordinator.ts): at most one drain runs at a time; a
// `wake` drives a drain and tolerates errors; `resume` JOINS the active drain (or forces one when
// idle) and surfaces its result; `interrupt` stops the CURRENT turn (not the session) and the
// long-lived supervisor keeps serving later wakes/resumes, terminating only after an idle period.

import type { StepDrainInput, StepDrainResult } from "./activities"

/** What a driver must provide; everything else is supervisor logic. */
export interface SupervisorRuntime {
  /** Wait until the predicate is true. With a timeout, resolve false when it expires first. */
  readonly condition: (predicate: () => boolean, timeout?: string) => Promise<boolean>
  readonly setSignalHandler: (name: "wake" | "interrupt", handler: () => void) => void
  readonly setUpdateHandler: (name: "resume", handler: () => Promise<void>) => void
  /** One step of a turn (SessionRunner.runStep). The Temporal driver runs it as an activity. */
  readonly runTurnStep: (input: StepDrainInput) => Promise<StepDrainResult>
  /**
   * Run one whole turn's drain inside a fresh cancellable scope. `cancelCurrentScope()` cancels the
   * scope of the drain currently running, which aborts its in-flight step; the supervisor stays
   * alive. Scoping the cancellation to the turn (not the workflow) is what lets an interrupt stop
   * the current turn without killing the session.
   */
  readonly runInDrainScope: <A>(fn: () => Promise<A>) => Promise<A>
  /** Cancel the drain scope currently running (interrupt the active turn). No-op when idle. */
  readonly cancelCurrentScope: () => void
  /** Whether an error is the driver's cancellation (a normal stop, not a failure). */
  readonly isCancellation: (error: unknown) => boolean
  /** Whether the WHOLE run (root scope) is cancelled -- a real workflow cancellation, as opposed to
   * a per-turn interrupt (which cancels only the current drain's child scope). A root cancellation
   * must stop the supervisor; it must never keep serving or continue-as-new. Reliable because the
   * timed wait no longer cancels any scope (so it cannot contaminate the root). */
  readonly isRootCancelled: () => boolean
  /** Whether every signal/update handler has fully finished (Temporal's own accounting). Gates
   * completion and continue-as-new so an in-flight update's result is never abandoned. Optional:
   * drivers without a handler protocol return true. */
  readonly allHandlersFinished?: () => boolean
  /** Restart the run with fresh history, carrying whether work is still pending. History-keeping
   * drivers only (Temporal). */
  readonly continueAsNew?: (sessionID: string, startWithWake: boolean) => Promise<never>
  /** Whether the driver says this run's history is large enough to roll over. A drain count cannot
   * answer this: one drain is a whole turn, and a stepped turn of 200 steps is thousands of events,
   * so a handful of drains can cross the server's limit long before the count does. Optional:
   * drivers without a history return false. */
  readonly historyWantsRollover?: () => boolean
}

export interface WorkflowOptions {
  /** How long to stay alive with no work before self-terminating. */
  readonly idleTimeout?: string
  /** Drains per run before continue-as-new, when the driver supports it. */
  readonly maxDrainsPerRun?: number
}

export const makeSupervisor = (rt: SupervisorRuntime, options?: WorkflowOptions) => {
  const IDLE_TIMEOUT = options?.idleTimeout ?? "5 minutes"
  // A continuously busy session never hits the idle return, so without a bound its history grows
  // until Temporal terminates the workflow. continue-as-new carries the pending-wake state, so no
  // queued work is lost across the boundary.
  const MAX_DRAINS_PER_RUN = options?.maxDrainsPerRun ?? 30

  // Each step (one provider attempt + its tools) is its own activity; the step loop is supervisor
  // control flow (step / promotion / first mirror SessionRunner.run's loop). `startWithWake` is the
  // explicit start intent: a wake-with-start begins with pending work, a resume-with-start does not
  // (its forced drain comes from the resume update), so resume must not manufacture a spurious wake.
  async function sessionTurn(sessionID: string, startWithWake: boolean = true): Promise<void> {
    let pendingWake = startWithWake
    let drains = 0
    let rolloverPending = false
    let resumers = 0 // in-flight resume handlers awaiting a drain
    // The single in-flight drain, or null when idle. New callers JOIN it rather than starting a
    // second, mirroring SessionRunCoordinator.run: one execution per session at a time, and a resume
    // attaches to the running one instead of queueing a redundant forced drain.
    let inFlight: Promise<void> | null = null

    const drive = (force: boolean): Promise<void> => {
      if (inFlight) return inFlight
      const running = rt
        .runInDrainScope(async () => {
          drains++
          if (drains >= MAX_DRAINS_PER_RUN) rolloverPending = true
          if (rt.historyWantsRollover?.()) rolloverPending = true
          let step = 1
          let promotion: string | null = null
          let first = true
          for (;;) {
            const r: StepDrainResult = await rt.runTurnStep({ sessionID, step, promotion, first, force })
            // Inside the loop as well, because one drain is a whole turn: a long one outgrows the
            // history without ever reaching the next drain's check.
            if (rt.historyWantsRollover?.()) rolloverPending = true
            if (!r.continue) break
            // A queued prompt continues this same drain as a fresh turn, so a session fed without a
            // gap never goes quiet and the rollover it is waiting for never happens. Stop at that
            // boundary instead and let the new run pick the queue up: the work is not lost, it is
            // one turn later. A steer is not a boundary, so it still rides this drain through.
            if (rolloverPending && r.promotion === "queue") {
              pendingWake = true
              break
            }
            step = r.step
            promotion = r.promotion
            first = false
          }
        })
        .finally(() => {
          inFlight = null
        })
      inFlight = running
      return running
    }

    rt.setSignalHandler("wake", () => {
      pendingWake = true
    })
    // interrupt stops the CURRENT turn, not the session: cancel the active drain's child scope. The
    // supervisor keeps serving, so a later wake/resume drives a fresh turn on this same long-lived
    // workflow and a prompt that races the interrupt is never stranded. A per-turn interrupt leaves
    // the root scope untouched, so isRootCancelled() distinguishes it from a real cancellation.
    rt.setSignalHandler("interrupt", () => {
      rt.cancelCurrentScope()
    })
    // resume = coordinator.run: join the active drain (or force one when idle) and surface its
    // result. A run error, or an interruption of the joined drain, rejects the update so the caller
    // observes it.
    rt.setUpdateHandler("resume", async () => {
      resumers++
      try {
        await drive(true)
      } finally {
        resumers--
      }
    })

    // "Idle" for completion/continue-as-new means: no drain in flight, no resume handler in our
    // accounting, AND Temporal reports every handler finished (so an update's result is never
    // abandoned by completing/continuing before the protocol records it).
    const quiescent = () => !inFlight && resumers === 0 && (rt.allHandlersFinished?.() ?? true)

    try {
      for (;;) {
        // Wake on a real wake, or on an ACTIONABLE rollover (bound crossed and quiescent). Gating
        // the rollover on quiescence keeps the loop from spinning while a resume drain is still in
        // flight, and -- crucially -- keeps a rollover from being mis-handled as a wake below.
        const woke = await rt.condition(() => pendingWake || (rolloverPending && quiescent()), IDLE_TIMEOUT)
        // A real root cancellation must stop the supervisor -- never keep serving or continue-as-new.
        // (Checked here too so the rollover short-circuit path can't continue-as-new a cancelled run.)
        if (rt.isRootCancelled()) return
        // continue-as-new: only from the main method, only when quiescent. Carry the pending wake so
        // queued work survives the boundary (a pure rollover carries false -> no spurious drain).
        if (rt.continueAsNew && rolloverPending && quiescent()) {
          await rt.continueAsNew(sessionID, pendingWake)
        }
        if (pendingWake) {
          pendingWake = false
          // If a resume already started the drain we take, we only JOIN it -- and it may be past the
          // point where it could pick up the work this wake signals. Re-arm pendingWake for one
          // follow-up drain; a fresh drain we start ourselves already covers the wake.
          const joined = inFlight !== null
          try {
            await drive(false)
          } catch (e) {
            // A real root cancellation wins: propagate it to end the workflow. A per-turn interrupt
            // or a run error (already recorded in the log) is tolerated and the supervisor keeps
            // serving.
            if (rt.isRootCancelled()) throw e
          }
          if (joined) pendingWake = true
          continue
        }
        // No real wake. Retire only on a genuine idle timeout with nothing in flight; a rollover
        // wakeup was either handled above (continue-as-new) or is waiting for the drain to finish.
        if (!woke && quiescent()) return
      }
    } catch (e) {
      // A real root cancellation (workflow cancelled) or a defect reaches here. End the workflow: a
      // cancellation completes the run cleanly, a defect fails it.
      if (rt.isCancellation(e)) return
      throw e
    }
  }

  return { sessionTurn }
}
