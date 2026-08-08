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
