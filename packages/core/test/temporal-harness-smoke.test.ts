// A real Temporal test harness for the session workflow: it runs the ACTUAL sessionTurn workflow
// (temporal-workflow.ts + workflow-core.ts) against @temporalio/testing's time-skipping server with
// a mock runTurnStep activity, entirely in-process -- no dev server, no provider, deterministic time.
// This is the harness for validating the Temporal supervisor's real behavior (draining, idle
// retirement, and -- as the suite grows -- interrupt/resume/join/continue-as-new).
//
// It also pins a regression: on @temporalio/workflow 1.21, condition(fn, timeout) called when fn is
// already true left the CancellationScope cancelled, so sessionTurn completed without ever draining
// a turn. The adapter now short-circuits an already-true predicate; this test fails without that fix
// (zero activities scheduled) and passes with it.
import { describe, it, expect } from "bun:test"
import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

const WORKFLOW = fileURLToPath(new URL("../src/session/execution/temporal-workflow.ts", import.meta.url))

describe("temporal workflow harness", () => {
  it("drives the real sessionTurn workflow through a wake-driven drain, then idles out", async () => {
    const env = await TestWorkflowEnvironment.createTimeSkipping()
    let steps = 0
    try {
      const worker = await Worker.create({
        connection: env.nativeConnection,
        namespace: env.namespace,
        taskQueue: "harness-smoke",
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
          taskQueue: "harness-smoke",
          workflowId: "wf-smoke",
          args: ["ses_smoke"],
          signal: "wake",
          signalArgs: [],
        })
        // The wake-driven drain runs the (mock) step; the supervisor then idles and, under
        // time-skipping, the 5-minute idle timer fast-forwards and the workflow completes.
        await handle.result()
      })

      expect(steps).toBe(1)
    } finally {
      await env.teardown()
    }
  }, 120_000)
})
