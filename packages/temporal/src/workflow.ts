// The Temporal driver for the session supervisor. The supervisor loop lives in supervisor.ts;
// this file adapts the real SDK's primitives (condition, signal/update handlers, activity proxies,
// cancellation) to the SupervisorRuntime interface and exports the workflow function the worker
// registers. Local mode does not use this loop -- it runs the proven SessionRunCoordinator directly
// (execution/local.ts); the two modes share SessionRunner and the durable event log.
//
// MUST stay sandbox-safe: Temporal bundles this in an isolated context, so no `effect`, no
// `@opencode-ai/core` runtime imports, no Node builtins.

import {
  proxyActivities,
  defineSignal,
  defineUpdate,
  setHandler,
  condition,
  sleep,
  continueAsNew,
  CancellationScope,
  isCancellation,
  allHandlersFinished,
} from "@temporalio/workflow"
import type { StepActivities } from "./activities"
import { SIGNALS, RESUME_UPDATE } from "./protocol"
import { makeSupervisor, type SupervisorRuntime } from "./supervisor"

const activityOptions = {
  // The heartbeat is the liveness bound (it stops within seconds of a worker death and Temporal
  // re-drives). startToClose is only the backstop for a drain that hangs while its process stays
  // alive, so it must comfortably exceed any legitimate turn: long tool runs, many steps, or a
  // human taking their time over a permission ask. A bound that kills a live turn also opens a
  // two-writer window until the old attempt notices its heartbeat was rejected.
  startToCloseTimeout: "12 hours",
  heartbeatTimeout: "10 seconds",
  retry: { maximumAttempts: 100 },
} as const

const { runTurnStep } = proxyActivities<StepActivities>(activityOptions)

export const wake = defineSignal(SIGNALS.wake)
export const interrupt = defineSignal(SIGNALS.interrupt)
export const resume = defineUpdate<void>(RESUME_UPDATE)
const signals = { wake, interrupt } as const

const runtime: SupervisorRuntime = {
  // A timed wait that does not use the SDK's condition(fn, timeout). On @temporalio/workflow 1.21
  // that variant cancels its internal timer scope on resolve, and the cancellation leaks into the
  // parent scope, so the next drain's child scope is born cancelled and the turn never runs. An
  // already-true predicate is answered without a timer for the same reason. For a real wait, a
  // no-timeout condition races a bare timer and the loser is abandoned: nothing here cancels a
  // scope, and a pending timer is cleaned up when the workflow closes.
  condition: async (predicate, timeout) => {
    if (predicate()) return true
    if (timeout === undefined) {
      await condition(predicate)
      return true
    }
    let timedOut = false
    const timer = sleep(timeout as never).then(() => {
      timedOut = true
    })
    timer.catch(() => {})
    await Promise.race([condition(() => predicate() || timedOut), timer])
    return predicate()
  },
  setSignalHandler: (name, handler) => setHandler(signals[name], handler),
  setUpdateHandler: (_name, handler) => setHandler(resume, handler),
  runTurnStep,
  // Run the turn's drain inside its own cancellable scope, tracked as the active one. An interrupt
  // cancels this scope (aborting the in-flight activity) without touching the workflow root, so the
  // supervisor survives to serve later turns.
  runInDrainScope: (fn) =>
    CancellationScope.cancellable(async () => {
      activeDrainScope = CancellationScope.current()
      try {
        return await fn()
      } finally {
        activeDrainScope = undefined
      }
    }),
  cancelCurrentScope: () => activeDrainScope?.cancel(),
  isCancellation,
  // A per-turn interrupt cancels only the child drain scope; a real workflow cancellation cancels
  // the root. Reliable now that the timed wait no longer cancels any scope (nothing contaminates the
  // root's consideredCancelled).
  isRootCancelled: () => rootScope?.consideredCancelled ?? false,
  allHandlersFinished,
  continueAsNew: (sessionID, startWithWake) => continueAsNew<typeof sessionTurn>(sessionID, { startWithWake }),
}

// The scope of the drain currently running, so an interrupt signal can cancel exactly that turn.
let activeDrainScope: CancellationScope | undefined
// The workflow's root scope, captured at entry, to detect a whole-run cancellation.
let rootScope: CancellationScope | undefined

const workflows = makeSupervisor(runtime)

// One evolvable input record instead of positional arguments: new settings ride along without a
// signature change, and a continue-as-new run carries the record forward. The sandbox cannot read
// env, so the idle override arrives here from the client (which reads the same variable local mode
// honors).
export interface SessionTurnOptions {
  readonly startWithWake?: boolean
  readonly idleTimeout?: string
}

export async function sessionTurn(sessionID: string, options?: SessionTurnOptions): Promise<void> {
  rootScope = CancellationScope.current()
  const startWithWake = options?.startWithWake ?? true
  const idleTimeout = options?.idleTimeout
  if (!idleTimeout) return workflows.sessionTurn(sessionID, startWithWake)
  return makeSupervisor(
    {
      ...runtime,
      continueAsNew: (id, wake) => continueAsNew<typeof sessionTurn>(id, { startWithWake: wake, idleTimeout }),
    },
    { idleTimeout },
  ).sessionTurn(sessionID, startWithWake)
}
