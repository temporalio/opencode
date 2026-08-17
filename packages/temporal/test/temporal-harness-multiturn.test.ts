// Real-Temporal validation that a session serves MULTIPLE turns: turn 1 completes, the supervisor
// parks in its idle timed wait, and a later wake drives turn 2 on the SAME workflow. This is the
// exact path the timed-wait rewrite fixes -- the SDK's condition(fn, timeout) leaked its cancellation
// into the root scope when it resolved, so turn 2's drain scope was born cancelled and never ran (a
// session could serve only one turn). With the leaking condition this test fails (steps stays 1).
//
// Uses a real (non-time-skipping) local server so the idle timer doesn't fast-forward and retire the
// workflow before the second wake. Own file: two native Temporal servers in one bun process segfault.
import { describe, it, expect } from "bun:test"
import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

const WORKFLOW = fileURLToPath(new URL("../src/workflow.ts", import.meta.url))
const poll = async (fn: () => boolean, ms = 20_000) => {
  const deadline = Date.now() + ms
  while (!fn()) {
    if (Date.now() > deadline) throw new Error("condition not reached")
    await new Promise((r) => setTimeout(r, 50))
  }
}

describe("temporal workflow harness: multi-turn", () => {
  it("serves a second turn after parking in the idle wait", async () => {
    const env = await TestWorkflowEnvironment.createLocal()
    let steps = 0
    try {
      const worker = await Worker.create({
        connection: env.nativeConnection,
        namespace: env.namespace,
        taskQueue: "harness-multiturn",
        workflowsPath: WORKFLOW,
        activities: {
          runTurnStep: async () => {
            steps++
            return { ran: true, continue: false, step: 1, promotion: null }
          },
        },
      })

      await worker.runUntil(async () => {
        const handle = await env.client.workflow.signalWithStart("sessionTurn", {
          taskQueue: "harness-multiturn",
          workflowId: "wf-multiturn",
          args: ["ses_multiturn"],
          signal: "wake",
          signalArgs: [],
        })
        await poll(() => steps === 1) // turn 1 drained
        // Deterministically wait until the supervisor has actually PARKED in the idle timed wait --
        // i.e. its sleep timer is recorded in history -- before waking it, so we genuinely exercise
        // the timed-wait/park-then-wake path (not a race where the wake lands during turn 1).
        const deadline = Date.now() + 20_000
        for (;;) {
          const history = await handle.fetchHistory()
          if ((history.events ?? []).some((e) => e.timerStartedEventAttributes)) break
          if (Date.now() > deadline) throw new Error("supervisor never parked in the idle timed wait")
          await new Promise((r) => setTimeout(r, 100))
        }
        await handle.signal("wake")
        await poll(() => steps === 2) // turn 2 drained on the SAME workflow (no leak poisoning it)
      })

      expect(steps).toBe(2)
    } finally {
      await env.teardown()
    }
  }, 120_000)
})
