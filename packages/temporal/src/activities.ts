// The runTurnStep activity: one step of a turn (one provider attempt + its tools), run through the
// `stepDrain` closure the layer captured over the app's Effect context. It heartbeats so a worker
// crash is detected quickly, and forwards Temporal cancellation as an AbortSignal so an interrupt
// turns into Effect fiber interruption inside the runner.

import { heartbeat, Context } from "@temporalio/activity"

// The event-log owner token for one activity execution: run id + activity id + attempt. A Temporal
// retry of the SAME step gets a fresh attempt, so once the retry claims the log the previous
// attempt
// (if still running) is fenced out. The activity id is essential: activity attempt numbers restart
// at 1 for every step, so a run-id+attempt token alone would repeat across steps (step 1 attempt 1
// and step 2 attempt 1 both mint `run#1`), letting a zombie attempt from an earlier step re-match
// the current owner and append stale events. Including the per-execution activity id keeps every
// step's tokens disjoint.
export function ownerTokenFrom(run: string, activityId: string, attempt: number): string {
  return `${run}:${activityId}:${attempt}`
}

function ownerToken(): string {
  const info = Context.current().info
  // runId/workflowType are typed optional on ActivityInfo; activityId is always present and already
  // makes the token unique per execution, so an empty run prefix in the (degenerate) missing case
  // is
  // harmless.
  const run = info.workflowExecution?.runId ?? info.workflowType ?? ""
  return ownerTokenFrom(run, info.activityId, info.attempt)
}

// The step contract lives with the drain in core; re-exported here so the workflow and its tests
// keep one import site inside this package.
import type { StepDrainInput, StepDrainResult } from "./drain"
import type {
  ModelCallDrainInput,
  ModelCallDrainResult,
  SealDrainInput,
  ToolCallDrainInput,
  ToolCallDrainResult,
} from "./l2-drain"
export type { StepDrainInput, StepDrainResult }

export type StepActivities = {
  runTurnStep(input: StepDrainInput): Promise<StepDrainResult>
}

// Heartbeat while a drain runs so a dead worker is noticed quickly. This only proves the process is
// alive, not that the work is progressing.
const beating = async <A>(body: () => Promise<A>): Promise<A> => {
  const beat = setInterval(() => {
    try {
      heartbeat()
    } catch {
      // heartbeat outside an activity context is a no-op for our purposes
    }
  }, 3000)
  try {
    return await body()
  } finally {
    clearInterval(beat)
  }
}

export function makeStepActivities(
  stepDrain: (input: StepDrainInput, signal: AbortSignal) => Promise<StepDrainResult>,
): StepActivities {
  return {
    async runTurnStep(input) {
      return beating(() =>
        stepDrain({ ...input, owner: ownerToken() }, Context.current().cancellationSignal),
      )
    },
  }
}

export type SteppedTurnActivities = {
  runModelCall(input: ModelCallDrainInput): Promise<ModelCallDrainResult>
  runToolCall(input: ToolCallDrainInput): Promise<ToolCallDrainResult>
  sealStep(input: SealDrainInput): Promise<StepDrainResult>
}

// The three activities of a stepped step. Only the model call mints and claims an owner token: it
// is
// the writer that supersedes the attempt before it, and the tool and seal activities publish under
// the token it returned. Giving each of them its own token would make a step's writers fence each
// other out of the log.
export function makeSteppedTurnActivities(drains: {
  modelCallDrain: (
    input: ModelCallDrainInput & { readonly owner: string },
    signal: AbortSignal,
  ) => Promise<ModelCallDrainResult>
  toolCallDrain: (input: ToolCallDrainInput, signal: AbortSignal) => Promise<ToolCallDrainResult>
  sealDrain: (input: SealDrainInput, signal: AbortSignal) => Promise<StepDrainResult>
}): SteppedTurnActivities {
  return {
    async runModelCall(input) {
      return beating(() =>
        drains.modelCallDrain(
          { ...input, owner: ownerToken() },
          Context.current().cancellationSignal,
        ),
      )
    },
    async runToolCall(input) {
      // Whether the side effect may already have happened is the activity's own knowledge, not the
      // workflow's: a second attempt of THIS call is a re-run of a tool whose result never landed.
      // The workflow could not compute this without reading non-deterministic state.
      const retry = Context.current().info.attempt > 1
      return beating(() =>
        drains.toolCallDrain({ ...input, retry }, Context.current().cancellationSignal),
      )
    },
    async sealStep(input) {
      return beating(() => drains.sealDrain(input, Context.current().cancellationSignal))
    },
  }
}
