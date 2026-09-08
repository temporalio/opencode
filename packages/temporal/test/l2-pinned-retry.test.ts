import { expect, it } from "bun:test"
import { fileURLToPath } from "node:url"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

// A pinned dispatch gets one attempt, and what happens after it fails is the contract that matters:
// the rest of the step carries on somewhere else rather than taking the turn down with it. The host
// it left behind cannot revert anything, because a snapshot pack names the one it was built on.
it("gives a pinned dispatch one attempt and then moves the step to the shared queue", async () => {
  const env = await TestWorkflowEnvironment.createLocal()
  let phase: "tool" | "seal" = "tool"
  const attempts = { tool: 0, seal: 0 }
  let shared = 0
  try {
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: "pin-retry-main",
      workflowsPath: fileURLToPath(new URL("../src/workflow.ts", import.meta.url)),
      activities: {
        runModelCall: async () => ({
          kind: "called",
          step: 1,
          calls: phase === "tool" ? [{ id: "call_write", name: "write", assistantMessageID: "msg_write" }] : [],
          owner: "run:1:1",
          queue: "pin-retry-tools",
        }),
        runToolCall: async () => {
          shared++
          return { outcome: "settled" }
        },
        sealStep: async () => {
          shared++
          return { ran: true, continue: false, step: 1, promotion: null }
        },
      },
    })
    const pinned = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: "pin-retry-tools",
      activities: {
        runToolCall: async () => {
          attempts.tool++
          throw ApplicationFailure.create({ message: "tool failed after dispatch", type: "ToolUnavailable" })
        },
        sealStep: async () => {
          attempts.seal++
          throw ApplicationFailure.create({ message: "seal failed after dispatch", type: "SealUnavailable" })
        },
      },
    })
    await worker.runUntil(() =>
      pinned.runUntil(async () => {
        for (const kind of ["tool", "seal"] as const) {
          phase = kind
          shared = 0
          const handle = await env.client.workflow.start("sessionTurn", {
            workflowId: `pin-retry-session-${kind}`,
            taskQueue: "pin-retry-main",
            args: ["ses_pin_retry", { stepped: true, startWithWake: false }],
          })
          const resumed = handle.executeUpdate("resume").then(
            () => "completed",
            () => "failed",
          )
          let timer: ReturnType<typeof setTimeout> | undefined
          try {
            const outcome = await Promise.race([
              resumed,
              new Promise<string>((resolve) => {
                timer = setTimeout(() => resolve("waiting for retries"), 2_000)
              }),
            ])
            expect(outcome).toBe("completed")
            expect(attempts[kind]).toBe(1)
            expect(shared).toBeGreaterThan(0)
          } finally {
            clearTimeout(timer)
            await handle.terminate()
            await resumed
          }
        }
      }),
    )
  } finally {
    await env.teardown()
  }
}, 120_000)
