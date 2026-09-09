import { expect, it } from "bun:test"
import { fileURLToPath } from "node:url"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

it("keeps distinct firing IDs after a long schedule name", async () => {
  const env = await TestWorkflowEnvironment.createLocal()
  const received: string[] = []
  const prefix = "daily-review-".repeat(5)
  try {
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: "schedule-id-check",
      workflowsPath: fileURLToPath(new URL("../src/workflow.ts", import.meta.url)),
      activities: {
        promptSession: async (input: { messageID: string }) => {
          received.push(input.messageID)
        },
        runTurnStep: async () => ({ ran: true, continue: false, step: 1, promotion: null }),
      },
    })
    await worker.runUntil(async () => {
      for (const day of ["01", "02"]) {
        await env.client.workflow.execute("scheduledPrompt", {
          workflowId: `${prefix}-workflow-2026-09-${day}T09:00:00Z`,
          taskQueue: "schedule-id-check",
          args: [{ sessionID: "ses_schedule_id", text: "review yesterday's changes" }],
        })
      }
    })
    expect(received).toHaveLength(2)
    expect(new Set(received).size).toBe(2)
  } finally {
    await env.teardown()
  }
}, 120_000)
