// A session serves more than one turn: turn 1 completes, the supervisor parks in its idle wait,
// and a later wake drives turn 2 on the same workflow. A real clock, so the idle timer does not
// retire the workflow before the second wake arrives.
import { describe, it, expect } from "bun:test"
import { poll, withSessionWorkflow } from "./lib/workflow-harness"

describe("temporal workflow harness: multi-turn", () => {
  it("serves a second turn after parking in the idle wait", async () => {
    let steps = 0
    await withSessionWorkflow(
      {
        name: "harness-multiturn",
        runTurnStep: async () => {
          steps++
          return { continue: false, step: 1, promotion: null }
        },
      },
      async (handle) => {
        await poll(() => steps === 1)
        // Wake only once the supervisor has parked, which is when its sleep timer is in history.
        // A wake that lands during turn 1 would test a different path.
        const deadline = Date.now() + 20_000
        for (;;) {
          const history = await handle.fetchHistory()
          if ((history.events ?? []).some((e) => e.timerStartedEventAttributes)) break
          if (Date.now() > deadline) throw new Error("supervisor never parked in the idle timed wait")
          await new Promise((r) => setTimeout(r, 100))
        }
        await handle.signal("wake")
        await poll(() => steps === 2)
      },
    )
    expect(steps).toBe(2)
  }, 120_000)
})
