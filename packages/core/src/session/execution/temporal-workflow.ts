// The durable equivalent of SessionRunCoordinator, as a Temporal workflow (one per session).
//
// MUST stay pure: Temporal bundles this in an isolated sandbox, so no `effect`, no
// `@opencode-ai/core`, no Node builtins. It only ever sees the sessionID string. All real work
// (SessionRunner.run against the durable event log) happens in the runContinuation activity.
//
// Semantics mirror the coordinator (run-coordinator.ts): a wake (or force) drives exactly one
// drain, repeated wakes coalesce into at most one follow-up, and the workflow ends when a drain
// finishes with nothing pending. A later wake starts a fresh run via signalWithStart.

import {
  proxyActivities,
  defineSignal,
  setHandler,
  condition,
  CancellationScope,
  isCancellation,
} from "@temporalio/workflow"
import type { Activities } from "./temporal-activities"

const { runContinuation } = proxyActivities<Activities>({
  startToCloseTimeout: "30 minutes",
  // Short heartbeat so a dead worker's in-flight drain is re-driven quickly; the run re-reads the
  // durable log, so a retry is a safe re-attach.
  heartbeatTimeout: "10 seconds",
  retry: { maximumAttempts: 100 },
})

export const wake = defineSignal("wake")
export const force = defineSignal("force")
export const interrupt = defineSignal("interrupt")

export async function sessionExecution(sessionID: string): Promise<void> {
  // Starting the workflow implies there is work to drain (it is started via signalWithStart).
  let pendingWake = true
  let forceNext = false
  let stopping = false

  setHandler(wake, () => {
    pendingWake = true
  })
  setHandler(force, () => {
    pendingWake = true
    forceNext = true
  })
  setHandler(interrupt, () => {
    stopping = true
    CancellationScope.current().cancel()
  })

  for (;;) {
    await condition(() => pendingWake || stopping)
    if (stopping) return
    const f = forceNext
    pendingWake = false
    forceNext = false
    try {
      await runContinuation({ sessionID, force: f })
    } catch (e) {
      if (isCancellation(e)) return
      throw e
    }
    // Quiescent: no wake arrived while draining. End; a future wake starts a new run.
    if (!pendingWake) return
  }
}
