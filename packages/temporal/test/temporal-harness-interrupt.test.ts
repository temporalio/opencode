// An interrupt cancels the current turn and the supervisor keeps serving. A real clock, because a
// blocking, heartbeating activity under time-skipping would jump past its heartbeat timeout and be
// retried for the wrong reason.
import { describe, it, expect } from "bun:test"
import { Context, heartbeat, CancelledFailure } from "@temporalio/activity"
import { poll, withSessionWorkflow } from "./lib/workflow-harness"

describe("temporal workflow harness: interrupt", () => {
  it("interrupt cancels the current turn but the workflow keeps serving the next", async () => {
    let steps = 0
    let firstStarted = false
    await withSessionWorkflow(
      {
        name: "harness-interrupt",
        runTurnStep: async () => {
          steps++
          if (steps === 1) {
            // Turn 1 blocks until the interrupt cancels it, heartbeating so it is not failed for
            // liveness while it waits.
            firstStarted = true
            const beat = setInterval(() => {
              try {
                heartbeat()
              } catch {
                // no-op outside an activity context
              }
            }, 200)
            try {
              await new Promise<never>((_resolve, reject) => {
                Context.current().cancellationSignal.addEventListener("abort", () =>
                  reject(new CancelledFailure("interrupted")),
                )
              })
            } finally {
              clearInterval(beat)
            }
          }
          return { continue: false, step: 1, promotion: null }
        },
      },
      async (handle) => {
        await poll(() => firstStarted)
        await handle.signal("interrupt")
        // A workflow the interrupt had ended would drop this wake, and the second turn would
        // never run.
        await handle.signal("wake")
        await poll(() => steps === 2)
      },
    )
    expect(steps).toBe(2)
  }, 120_000)
})
