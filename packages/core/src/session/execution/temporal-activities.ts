// The runContinuation activity: it runs one drain (SessionRunner.run for the whole turn) by calling
// the `drain` closure the layer captured over the app's Effect context. It heartbeats so a worker
// crash is detected quickly, and forwards Temporal cancellation as an AbortSignal so an interrupt
// turns into Effect fiber interruption inside the runner.

import { heartbeat, Context } from "@temporalio/activity"

export interface DrainInput {
  sessionID: string
  force: boolean
}

export type Activities = {
  runContinuation(input: DrainInput): Promise<void>
}

export function makeActivities(
  drain: (input: DrainInput, signal: AbortSignal) => Promise<void>,
): Activities {
  return {
    async runContinuation(input) {
      const beat = setInterval(() => {
        try {
          heartbeat()
        } catch {
          // heartbeat outside an activity context is a no-op for our purposes
        }
      }, 3000)
      try {
        await drain(input, Context.current().cancellationSignal)
      } finally {
        clearInterval(beat)
      }
    },
  }
}

// Per-step variant (OPENCODE_SESSION_EXECUTION=temporal-turn): one runTurnStep activity = one step
// (one provider attempt + its tools). The workflow loops it, so each step is its own activity with
// its own retry/timeout/visibility. `promotion` is null (not undefined) so it serializes cleanly.
export interface StepDrainInput {
  sessionID: string
  step: number
  promotion: string | null
  first: boolean
  force: boolean
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
        } catch {}
      }, 3000)
      try {
        return await stepDrain(input, Context.current().cancellationSignal)
      } finally {
        clearInterval(beat)
      }
    },
  }
}
