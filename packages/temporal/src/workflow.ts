// The Temporal driver for the session supervisor. The supervisor loop lives in supervisor.ts;
// this file adapts the real SDK's primitives (condition, signal/update handlers, activity proxies,
// cancellation) to the SupervisorRuntime interface and exports the workflow function the worker
// registers. Local mode does not use this loop -- it runs the proven SessionRunCoordinator directly
// (execution/local.ts); the two modes share SessionRunner and the durable event log.
//
// MUST stay sandbox-safe: Temporal bundles this in an isolated context, so no `effect`, no
// `@opencode-ai/core` runtime imports, no Node builtins.

import {
  patched,
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
  workflowInfo,
  log,
  startChild,
  getExternalWorkflowHandle,
  ParentClosePolicy,
} from "@temporalio/workflow"
import { WorkflowExecutionAlreadyStartedError } from "@temporalio/common"
import type { StepActivities, SteppedTurnActivities } from "./activities"
import { isHaltFailure, isUnclaimedFailure, makeSteppedTurn } from "./l2-step"
import { SIGNALS, RESUME_UPDATE, WORKFLOW_ID_PREFIX } from "./protocol"
import { makeSupervisor, type SupervisorRuntime } from "./supervisor"

const activityOptions = {
  // Human approvals can outlast a normal turn. Heartbeat expiry does not terminate the body.
  startToCloseTimeout: "12 hours",
  heartbeatTimeout: "10 seconds",
  retry: { maximumAttempts: 100 },
} as const

const { runTurnStep } = proxyActivities<StepActivities>(activityOptions)

// The stepped mode's three activities, each with its own bounds. Separate proxies are the point of
// the split, not an accident of it: a tool that hangs, or that is waiting on a human, no longer
// holds the provider attempt and every other tool of the same step under one shared timeout.
const { runModelCall } = proxyActivities<SteppedTurnActivities>(activityOptions)
const { runToolCall } = proxyActivities<SteppedTurnActivities>(activityOptions)
// Sealing is a snapshot, a diff and one event. It should not inherit a turn-sized backstop.
const sealOptions = { ...activityOptions, startToCloseTimeout: "10 minutes" } as const
const { sealStep } = proxyActivities<SteppedTurnActivities>(sealOptions)

// A private queue can stop polling or run out of slots. Bound the wait before unstarted work moves.
const PINNED_SCHEDULE_TO_START = "30 seconds"

/** The same two activities, addressed to one worker's own queue. Built per queue rather than once,
 * because the queue is not known until the model call reports it; that report comes out of history,
 * so this is deterministic on replay. */
const pinnedTo = (taskQueue: string) => ({
  runToolCall: proxyActivities<SteppedTurnActivities>({
    ...activityOptions,
    retry: { maximumAttempts: 1 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).runToolCall,
  sealStep: proxyActivities<SteppedTurnActivities>({
    ...sealOptions,
    retry: { maximumAttempts: 1 },
    taskQueue,
    scheduleToStartTimeout: PINNED_SCHEDULE_TO_START,
  }).sealStep,
})

// Admitting a prompt is a row in the store, so it is an activity; it is small and it must not hold
// a firing open if no worker is polling.
const { promptSession } = proxyActivities<{
  promptSession(input: { sessionID: string; messageID: string; text: string }): Promise<void>
}>({
  startToCloseTimeout: "2 minutes",
  scheduleToCloseTimeout: "30 minutes",
  retry: { maximumAttempts: 10 },
})

export const wake = defineSignal(SIGNALS.wake)
export const interrupt = defineSignal(SIGNALS.interrupt)
export const resume = defineUpdate<void>(RESUME_UPDATE)
const signals = { wake, interrupt } as const

const runtime: SupervisorRuntime = {
  // Short-circuit when the predicate already holds. Besides saving a round trip, this avoids a real
  // breakage: on @temporalio/workflow 1.21, calling condition(fn, timeout) when fn is already true
  // leaves the current CancellationScope cancelled, so the NEXT condition() throws CancelledFailure
  // -- which the supervisor reads as an interrupt and the workflow completes without ever draining
  // a
  // turn. Checking fn() first keeps the timeout timer (and its scope) out of the already-true path.
  // A timed wait that does NOT use the SDK's condition(fn, timeout). On @temporalio/workflow 1.21
  // that variant cancels its internal timer scope on resolve and the cancellation LEAKS into the
  // parent (root) scope, so the next drain's child scope is born cancelled and the turn never runs
  // (a session could serve only one turn). Instead: short-circuit an already-true predicate; for a
  // real wait, race a no-timeout condition against a bare timer and abandon the loser. Nothing here
  // cancels a scope, so nothing leaks; an unfired timer / unresolved condition is harmless and a
  // pending timer is cleaned up when the workflow closes.
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
  // the root. Reliable now that the timed wait no longer cancels any scope (nothing contaminates
  // the
  // root's consideredCancelled).
  isRootCancelled: () => rootScope?.consideredCancelled ?? false,
  allHandlersFinished,
  continueAsNew: (sessionID, startWithWake) => continueAsNew<typeof sessionTurn>(sessionID, { startWithWake }),
  // The server's own read of whether this run has grown enough to roll over. The drain count alone
  // misses it: a stepped turn is thousands of events, so a handful of drains can cross the limit.
  historyWantsRollover: () => workflowInfo().continueAsNewSuggested,
  // False only while replaying a history written before the ceiling existed. See the runtime field.
  boundsStepsPerTurn: () => patched("a-turn-has-a-step-ceiling"),
  warn: (message, attributes) => log.warn(message, attributes),
}

// Same supervisor, different step body: wake, interrupt, idle timeout and continue-as-new are
// unchanged, and only what "one step" means differs. Built per run rather than once, because
// whether a step's tools may overlap rides the workflow input: the sandbox cannot read env.
const steppedRuntime = (serial: boolean): SupervisorRuntime => ({
  ...runtime,
  runTurnStep: makeSteppedTurn({
    activities: { runModelCall, runToolCall, sealStep },
    isCancellation,
    isHalt: isHaltFailure,
    isUnclaimed: isUnclaimedFailure,
    // False only while replaying a history written before this rule existed. See the dep.
    resumesAfterLostHost: () => patched("lost-host-does-not-end-the-turn"),
    pinnedTo,
    serial,
    nonCancellable: (fn) => CancellationScope.nonCancellable(fn),
    // The SDK's logger, so a line carries its workflow and run id and is suppressed on replay.
    log: (message, attributes) => log.info(message, attributes),
  }),
})

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
  /** Drive each step as a provider attempt, one activity per tool call, and a seal, instead of one
   * activity for the whole step. Off by default: the whole-step mode is what runs today. */
  readonly stepped?: boolean
  /** Run a step's tool calls one at a time. Each ships the tree from the host that ran it, so two
   * on two hosts each publish a tree without the other's work. The client decides, because only it
   * can read whether the store is shared. */
  readonly serialTools?: boolean
}

export async function sessionTurn(sessionID: string, options?: SessionTurnOptions): Promise<void> {
  rootScope = CancellationScope.current()
  const startWithWake = options?.startWithWake ?? true
  const idleTimeout = options?.idleTimeout
  const stepped = options?.stepped === true
  const serialTools = options?.serialTools === true
  if (!idleTimeout && !stepped) return workflows.sessionTurn(sessionID, startWithWake)
  return makeSupervisor(
    {
      ...(stepped ? steppedRuntime(serialTools) : runtime),
      // The mode has to survive the boundary, or a long session silently reverts to whole-step
      // activities the first time it rolls over.
      continueAsNew: (id, wake) =>
        continueAsNew<typeof sessionTurn>(id, {
          startWithWake: wake,
          idleTimeout,
          stepped,
          serialTools,
        }),
    },
    idleTimeout ? { idleTimeout } : undefined,
  ).sessionTurn(sessionID, startWithWake)
}

/**
 * A turn nobody started.
 *
 * A schedule fires this, and it runs where no client and no serve process exist: the prompt is
 * admitted by an activity, because it is a row in the store, and the session's own supervisor is
 * started as an abandoned child (or signalled, when it is already running). Nothing here waits for
 * the turn: this workflow's job is to hand the work over and finish, which is what makes a firing
 * cheap and a missed one visible in the schedule rather than in a run that never ends.
 *
 * The message id comes from the firing's own workflow id, so a re-drive admits the same prompt
 * rather than a second one.
 */
export async function scheduledPrompt(input: {
  readonly sessionID: string
  readonly text: string
  readonly session?: SessionTurnOptions
}): Promise<void> {
  const messageID = `msg_sched_${workflowInfo().workflowId}`
  await promptSession({ sessionID: input.sessionID, messageID, text: input.text })
  const options: SessionTurnOptions = { ...input.session, startWithWake: true }
  try {
    await startChild(sessionTurn, {
      workflowId: `${WORKFLOW_ID_PREFIX}${input.sessionID}`,
      args: [input.sessionID, options],
      parentClosePolicy: ParentClosePolicy.ABANDON,
    })
  } catch (error) {
    // The session is already being driven, which is the ordinary case for a schedule that fires
    // faster than a turn takes. The prompt is admitted either way; what it needs is a wake, because
    // a supervisor waiting out its idle period is not watching the store.
    if (!(error instanceof WorkflowExecutionAlreadyStartedError)) throw error
    await getExternalWorkflowHandle(`${WORKFLOW_ID_PREFIX}${input.sessionID}`).signal(wake)
  }
}
