// The Temporal driver's per-session supervisor, expressed over a six-primitive runtime interface so
// the SDK's condition/signals/updates/activities plug in (temporal-workflow.ts) and it can be
// unit-tested off a live cluster. Local mode does NOT run this loop: it uses the proven
// SessionRunCoordinator directly (execution/local.ts). The two modes share SessionRunner and the
// durable event log, not this supervisor.
//
// MUST stay pure: the Temporal driver bundles this into the workflow sandbox, so no `effect`, no
// `@opencode-ai/core` runtime imports, no Node builtins. Type-only imports are erased and safe.
//
// Semantics mirror the local coordinator (run-coordinator.ts): drains are serialized (one at a
// time), a `wake` drives a drain and is tolerant of errors, and `resume` (an update) drives a
// forced drain and returns its result to the caller (throwing the run's error). The supervisor
// stays alive to serve later wakes/resumes and terminates after an idle period.

import type { StepDrainInput, StepDrainResult } from "./temporal-activities"

/** What a driver must provide. Six primitives; everything else is supervisor logic. */
export interface WorkflowRuntime {
  /** Wait until the predicate is true. With a timeout, resolve false when it expires first. */
  readonly condition: (predicate: () => boolean, timeout?: string) => Promise<boolean>
  readonly setSignalHandler: (name: "wake" | "interrupt", handler: () => void) => void
  readonly setUpdateHandler: (name: "resume", handler: () => Promise<void>) => void
  /** One step of a turn (SessionRunner.runStep). The Temporal driver runs it as an activity. */
  readonly runTurnStep: (input: StepDrainInput) => Promise<StepDrainResult>
  /** Cancel the in-flight drain and any parked condition (interrupt semantics). */
  readonly cancelCurrentScope: () => void
  /** Whether an error is the driver's cancellation (a normal stop, not a failure). */
  readonly isCancellation: (error: unknown) => boolean
  /** Restart the run with fresh history. Only meaningful for drivers that keep history. */
  readonly continueAsNew?: (sessionID: string) => Promise<never>
}

export interface WorkflowOptions {
  /** How long to stay alive with no work before self-terminating. */
  readonly idleTimeout?: string
  /** Drains per run before continue-as-new, when the driver supports it. */
  readonly maxDrainsPerRun?: number
}

export const makeWorkflows = (rt: WorkflowRuntime, options?: WorkflowOptions) => {
  const IDLE_TIMEOUT = options?.idleTimeout ?? "5 minutes"
  // A continuously busy session never hits the idle return, so without a bound its history grows
  // until Temporal terminates the workflow. A fresh run starts with a pending wake, so no work is
  // lost across the boundary.
  const MAX_DRAINS_PER_RUN = options?.maxDrainsPerRun ?? 30

  // The turn is driven one step at a time: each step (one provider attempt + its tools) is its
  // own drain call, and the step loop is supervisor control flow. The loop state
  // (step / promotion / first) mirrors SessionRunner.run's loop.
  async function sessionTurn(sessionID: string): Promise<void> {
    let pendingWake = true // started by a wake -> there is work to drain
    let stopping = false
    let draining = false
    let handlers = 0

    const drainTurn = async (force: boolean) => {
      // Serialize drains, like the coordinator (one owner fiber per session at a time). Re-check
      // after every wakeup: two waiters parked on the same condition can both observe `!draining`
      // in one activation, and without the loop both would start a drain.
      for (;;) {
        await rt.condition(() => !draining || stopping)
        if (stopping) return
        if (!draining) break
      }
      draining = true
      try {
        let step = 1
        let promotion: string | null = null
        let first = true
        for (;;) {
          const r: StepDrainResult = await rt.runTurnStep({ sessionID, step, promotion, first, force })
          if (!r.continue) break
          step = r.step
          promotion = r.promotion
          first = false
        }
      } finally {
        draining = false
      }
    }

    rt.setSignalHandler("wake", () => {
      pendingWake = true
    })
    rt.setSignalHandler("interrupt", () => {
      stopping = true
      rt.cancelCurrentScope()
    })
    // resume = coordinator.run: force one drain and surface its result (a run error rejects the
    // update, so the caller observes it).
    rt.setUpdateHandler("resume", async () => {
      handlers++
      try {
        await drainTurn(true)
      } finally {
        handlers--
      }
    })

    // interrupt cancels the whole scope, so a cancellation can surface at the idle wait itself,
    // not just inside a drain; treat it as a normal stop rather than a failure.
    let drains = 0
    try {
      for (;;) {
        const gotWork = await rt.condition(() => pendingWake || stopping, IDLE_TIMEOUT)
        if (stopping) return
        if (!gotWork) {
          // A wake can race the idle timer; without this re-check it would be dropped.
          if (pendingWake) continue
          // Idle: terminate only when nothing is in flight. A later wake/resume starts a fresh run.
          if (!draining && handlers === 0) return
          continue
        }
        pendingWake = false
        try {
          await drainTurn(false)
        } catch (e) {
          // wake tolerates run errors (the coordinator logs and moves on); only cancellation stops us.
          if (rt.isCancellation(e)) return
        }
        drains++
        if (rt.continueAsNew && drains >= MAX_DRAINS_PER_RUN && handlers === 0)
          await rt.continueAsNew(sessionID)
      }
    } catch (e) {
      if (rt.isCancellation(e)) return
      throw e
    }
  }

  return { sessionTurn }
}
