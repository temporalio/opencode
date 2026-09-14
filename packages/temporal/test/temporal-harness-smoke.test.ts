// One wake drives one drain, and the supervisor then idles out. Under time-skipping the idle
// timer fast-forwards, so the workflow completes on its own. A drain count of zero here means the
// workflow's timed wait cancelled the scope the drain would have run in.
import { describe, it, expect } from "bun:test"
import { withSessionWorkflow } from "./lib/workflow-harness"

describe("temporal workflow harness", () => {
  it("drives the real sessionTurn workflow through a wake-driven drain, then idles out", async () => {
    let steps = 0
    await withSessionWorkflow(
      {
        name: "harness-smoke",
        timeSkipping: true,
        runTurnStep: async () => {
          steps++
          return { continue: false, step: 1, promotion: null }
        },
      },
      (handle) => handle.result(),
    )
    expect(steps).toBe(1)
  }, 120_000)
})
