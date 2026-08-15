// Real-Temporal validation that an interrupt cancels the CURRENT turn but the supervisor keeps
// serving (the redesigned interrupt semantics). Uses a real (non-time-skipping) local server so a
// blocking, heartbeating activity behaves in real time -- time-skipping would jump past its
// heartbeat timeout and spuriously retry it. Runs in its own file: two native Temporal servers in
// one bun process segfault the runtime.
import { describe, it, expect } from "bun:test"
import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import { Context, heartbeat, CancelledFailure } from "@temporalio/activity"

const WORKFLOW = fileURLToPath(new URL("../src/session/execution/temporal-workflow.ts", import.meta.url))
const poll = async (fn: () => boolean, ms = 20_000) => {
  const deadline = Date.now() + ms
  while (!fn()) {
    if (Date.now() > deadline) throw new Error("condition not reached")
    await new Promise((r) => setTimeout(r, 50))
  }
}

describe("temporal workflow harness: interrupt", () => {
  it("interrupt cancels the current turn but the workflow keeps serving the next", async () => {
    const env = await TestWorkflowEnvironment.createLocal()
    let steps = 0
    let firstStarted = false
    try {
      const worker = await Worker.create({
        connection: env.nativeConnection,
        namespace: env.namespace,
        taskQueue: "harness-interrupt",
        workflowsPath: WORKFLOW,
        activities: {
          runTurnStep: async () => {
            steps++
            if (steps === 1) {
              // Turn 1 blocks until the interrupt cancels it, heartbeating so it isn't failed for
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
            return { ran: true, continue: false, step: 1, promotion: null }
          },
        },
      })

      await worker.runUntil(async () => {
        const handle = await env.client.workflow.signalWithStart("sessionTurn", {
          taskQueue: "harness-interrupt",
          workflowId: "wf-interrupt",
          args: ["ses_interrupt"],
          signal: "wake",
          signalArgs: [],
        })
        await poll(() => firstStarted) // turn 1's activity is running
        await handle.signal("interrupt") // cancel the current turn's scope
        // The workflow must still be alive to accept a new prompt; if the interrupt had ended it,
        // this wake would target a completed workflow and the second turn would never run.
        await handle.signal("wake")
        await poll(() => steps === 2) // a second turn ran on the SAME workflow
      })

      expect(steps).toBe(2)
    } finally {
      await env.teardown()
    }
  }, 120_000)
})
