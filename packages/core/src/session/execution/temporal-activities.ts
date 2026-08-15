// The runTurnStep activity: one step of a turn (one provider attempt + its tools), run through the
// `stepDrain` closure the layer captured over the app's Effect context. It heartbeats so a worker
// crash is detected quickly, and forwards Temporal cancellation as an AbortSignal so an interrupt
// turns into Effect fiber interruption inside the runner.

import { heartbeat, Context } from "@temporalio/activity"

// The event-log owner token for one activity execution: run id + activity id + attempt. A Temporal
// retry of the SAME step gets a fresh attempt, so once the retry claims the log the previous attempt
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
  // makes the token unique per execution, so an empty run prefix in the (degenerate) missing case is
  // harmless.
  const run = info.workflowExecution?.runId ?? info.workflowType ?? ""
  return ownerTokenFrom(run, info.activityId, info.attempt)
}

// The workflow loops runTurnStep, so each step is its own activity with its own
// retry/timeout/visibility. `promotion` is null (not undefined) so it serializes cleanly.
export interface StepDrainInput {
  sessionID: string
  step: number
  promotion: string | null
  first: boolean
  force: boolean
  // The attempt that owns the event log while this drain runs. Set activity-side (see
  // ownerToken), so it stays out of the workflow's deterministic input.
  owner?: string
}

export interface StepDrainResult {
  ran: boolean
  continue: boolean
  step: number
  promotion: string | null
}

export type StepActivities = {
  runTurnStep(input: StepDrainInput): Promise<StepDrainResult>
}

export function makeStepActivities(
  stepDrain: (input: StepDrainInput, signal: AbortSignal) => Promise<StepDrainResult>,
): StepActivities {
  return {
    async runTurnStep(input) {
      const beat = setInterval(() => {
        try {
          heartbeat()
        } catch {
          // heartbeat outside an activity context is a no-op for our purposes
        }
      }, 3000)
      try {
        return await stepDrain({ ...input, owner: ownerToken() }, Context.current().cancellationSignal)
      } finally {
        clearInterval(beat)
      }
    },
  }
}
