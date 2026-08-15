// The Temporal driver for the session supervisor. The supervisor loop lives in workflow-core.ts;
// this file adapts the real SDK's primitives (condition, signal/update handlers, activity proxies,
// cancellation) to the WorkflowRuntime interface and exports the workflow function the worker
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
  continueAsNew,
  CancellationScope,
  isCancellation,
} from "@temporalio/workflow"
import type { StepActivities } from "./temporal-activities"
import { makeWorkflows, type WorkflowRuntime } from "./workflow-core"

const activityOptions = {
  // The heartbeat is the liveness bound (it stops within seconds of a worker death and Temporal
  // re-drives). startToClose is only the backstop for a drain that hangs while its process stays
  // alive, so it must comfortably exceed any legitimate turn: long tool runs, many steps, or a
  // human taking their time over a permission ask. 30 minutes proved far too tight -- it hard-killed
  // legitimate turns and each kill opened a short two-writer window until the zombie attempt
  // noticed its heartbeat rejection.
  startToCloseTimeout: "12 hours",
  heartbeatTimeout: "10 seconds",
  retry: { maximumAttempts: 100 },
} as const

const { runTurnStep } = proxyActivities<StepActivities>(activityOptions)

export const wake = defineSignal("wake")
export const interrupt = defineSignal("interrupt")
export const resume = defineUpdate<void>("resume")
const signals = { wake, interrupt } as const

const runtime: WorkflowRuntime = {
  condition: async (predicate, timeout) => {
    if (timeout === undefined) {
      await condition(predicate)
      return true
    }
    // The runtime interface uses plain strings; the SDK's Duration is a branded string template.
    return condition(predicate, timeout as never)
  },
  setSignalHandler: (name, handler) => setHandler(signals[name], handler),
  setUpdateHandler: (_name, handler) => setHandler(resume, handler),
  runTurnStep,
  cancelCurrentScope: () => CancellationScope.current().cancel(),
  isCancellation,
  continueAsNew: (sessionID) => continueAsNew<(id: string) => Promise<void>>(sessionID),
}

const workflows = makeWorkflows(runtime)

export async function sessionTurn(sessionID: string): Promise<void> {
  return workflows.sessionTurn(sessionID)
}
