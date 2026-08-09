// The durable equivalent of SessionRunCoordinator, as a Temporal workflow (one per session).
//
// MUST stay pure: Temporal bundles this in an isolated sandbox, so no `effect`, no
// `@opencode-ai/core`, no Node builtins. It only ever sees the sessionID string. All real work
// (SessionRunner.run against the durable event log) happens in the runContinuation activity.
//
// Semantics mirror the coordinator (run-coordinator.ts): drains are serialized (one at a time),
// a `wake` drives a drain and is tolerant of errors, and `resume` (an Update) drives a forced
// drain and returns its result to the caller (throwing the run's error). The workflow stays alive
// to serve later wakes/resumes and terminates after an idle period.

import {
  proxyActivities,
  defineSignal,
  defineUpdate,
  setHandler,
  condition,
  CancellationScope,
  isCancellation,
} from "@temporalio/workflow"
import type { Activities, StepActivities, StepDrainResult } from "./temporal-activities"

const activityOptions = {
  startToCloseTimeout: "30 minutes",
  // Short heartbeat so a dead worker's in-flight drain is re-driven quickly; the run re-reads the
  // durable log, so a retry is a safe re-attach. A genuine run error is thrown non-retryable by the
  // activity, so only crashes/timeouts actually retry.
  heartbeatTimeout: "10 seconds",
  retry: { maximumAttempts: 100 },
} as const

const { runContinuation } = proxyActivities<Activities>(activityOptions)
const { runTurnStep } = proxyActivities<StepActivities>(activityOptions)

export const wake = defineSignal("wake")
export const interrupt = defineSignal("interrupt")
export const resume = defineUpdate<void>("resume")

const IDLE_TIMEOUT = "5 minutes"

export async function sessionExecution(sessionID: string): Promise<void> {
  let pendingWake = true // started via signalWithStart -> there is work to drain
  let stopping = false
  let draining = false
  let handlers = 0

  // Serialize drains, like the coordinator (one owner fiber per session at a time).
  const drainOnce = async (force: boolean) => {
    await condition(() => !draining || stopping)
    if (stopping) return
    draining = true
    try {
      await runContinuation({ sessionID, force })
    } finally {
      draining = false
    }
  }

  setHandler(wake, () => {
    pendingWake = true
  })
  setHandler(interrupt, () => {
    stopping = true
    CancellationScope.current().cancel()
  })
  // resume = coordinator.run: force one drain and surface its result (a run error rejects the
  // Update, so the caller observes it).
  setHandler(resume, async () => {
    handlers++
    try {
      await drainOnce(true)
    } finally {
      handlers--
    }
  })

  for (;;) {
    const gotWork = await condition(() => pendingWake || stopping, IDLE_TIMEOUT)
    if (stopping) return
    if (!gotWork) {
      // Idle: terminate only when nothing is in flight. A later wake/resume starts a fresh run.
      if (!draining && handlers === 0) return
      continue
    }
    pendingWake = false
    try {
      await drainOnce(false)
    } catch (e) {
      // wake tolerates run errors (the coordinator logs and moves on); only cancellation stops us.
      if (isCancellation(e)) return
    }
  }
}

// Per-step variant (OPENCODE_SESSION_EXECUTION=temporal-turn): identical lifecycle, but a turn is
// driven one step at a time -- each step (one provider attempt + its tools) is its own
// runTurnStep activity, and the step loop is workflow control flow. The loop state (step /
// promotion / first) mirrors SessionRunner.run's loop and lives in the (deterministic) workflow.
export async function sessionTurn(sessionID: string): Promise<void> {
  let pendingWake = true
  let stopping = false
  let draining = false
  let handlers = 0

  const drainTurn = async (force: boolean) => {
    await condition(() => !draining || stopping)
    if (stopping) return
    draining = true
    try {
      let step = 1
      let promotion: string | null = null
      let first = true
      for (;;) {
        const r: StepDrainResult = await runTurnStep({ sessionID, step, promotion, first, force })
        if (!r.continue) break
        step = r.step
        promotion = r.promotion
        first = false
      }
    } finally {
      draining = false
    }
  }

  setHandler(wake, () => {
    pendingWake = true
  })
  setHandler(interrupt, () => {
    stopping = true
    CancellationScope.current().cancel()
  })
  setHandler(resume, async () => {
    handlers++
    try {
      await drainTurn(true)
    } finally {
      handlers--
    }
  })

  for (;;) {
    const gotWork = await condition(() => pendingWake || stopping, IDLE_TIMEOUT)
    if (stopping) return
    if (!gotWork) {
      if (!draining && handlers === 0) return
      continue
    }
    pendingWake = false
    try {
      await drainTurn(false)
    } catch (e) {
      if (isCancellation(e)) return
    }
  }
}
