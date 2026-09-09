import { expect, it } from "bun:test"
import { fileURLToPath } from "node:url"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

// The server must not retry a pinned body or move it while its physical state is unknown. What the
// step does instead is close itself where a worker is answering: the call stays with the host that
// has it, and the turn goes on rather than ending with that host.
it("gives a pinned dispatch one attempt, keeps its call, and closes the step elsewhere", async () => {
  const env = await TestWorkflowEnvironment.createLocal()
  let phase: "tool" | "seal" = "tool"
  const attempts = { tool: 0, seal: 0 }
  const shared = { tool: 0, seal: 0 }
  const sealed: Array<boolean | undefined> = []
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
          shared.tool++
          return { outcome: "settled" }
        },
        sealStep: async (input: { withoutTheTree?: boolean }) => {
          shared.seal++
          sealed.push(input.withoutTheTree === true)
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
          shared.tool = 0
          shared.seal = 0
          sealed.length = 0
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
            // The turn is handed back, not ended: the step was closed on the shared queue and the
            // supervisor gets a result to carry on from.
            expect(outcome).toBe("completed")
            // One attempt on the pinned queue, and never a second one anywhere: a retry's queue
            // timeout cannot rule out the first attempt still running.
            expect(attempts[kind]).toBe(1)
            // The call itself does not move. Only the seal does, and that is the step saying what
            // it had rather than the tool being run somewhere else.
            expect(shared.tool).toBe(0)
            expect(shared.seal).toBe(1)
            // And it seals without the tree: this worker never ran the step, and the one that did
            // may still be inside a tool, so rebuilding here would put it on the newest state and
            // shipping from here would publish the state before the step.
            expect(sealed).toEqual([true])
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
