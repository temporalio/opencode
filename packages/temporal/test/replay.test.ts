// A stepped turn's decisions are workflow code, so they are written into the history of every run
// that hits them. The one check that must always hold is that this code replays a history it
// wrote itself; a run that lost a pinned dispatch is the branch with the most decisions in it.

import { expect, it } from "bun:test"
import { fileURLToPath } from "node:url"
import { ApplicationFailure } from "@temporalio/common"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

const workflowsPath = fileURLToPath(new URL("../src/workflow.ts", import.meta.url))

it("replays a stepped turn that lost its host", async () => {
  const env = await TestWorkflowEnvironment.createLocal()
  try {
    let called = false
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: "replay-main",
      workflowsPath,
      activities: {
        runModelCall: async () => {
          // One step with a tool, then nothing left to do: the turn has to be able to end after the
          // step that lost its host, or the replay would only ever see the failure.
          if (called) return { kind: "settled", result: { continue: false, step: 2, promotion: null } }
          called = true
          return {
            kind: "called",
            step: 1,
            calls: [{ id: "call_lost", name: "write", assistantMessageID: "msg_lost" }],
            owner: "run:1:1",
            queue: "replay-tools",
          }
        },
        runToolCall: async () => ({ outcome: "settled" }),
        sealStep: async () => ({ continue: true, step: 2, promotion: null }),
      },
    })
    const pinned = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: "replay-tools",
      activities: {
        runToolCall: async () => {
          throw ApplicationFailure.create({ message: "the tool failed after starting", type: "ToolUnavailable" })
        },
        sealStep: async () => ({ continue: false, step: 2, promotion: null }),
      },
    })

    const history = await worker.runUntil(() =>
      pinned.runUntil(async () => {
        const handle = await env.client.workflow.start("sessionTurn", {
          workflowId: "replay-session",
          taskQueue: "replay-main",
          args: ["ses_replay", { stepped: true, startWithWake: false }],
        })
        await handle.executeUpdate("resume")
        await handle.terminate().catch(() => undefined)
        return handle.fetchHistory()
      }),
    )

    await Worker.runReplayHistory({ workflowsPath }, history)
    expect(history.events?.length ?? 0).toBeGreaterThan(0)
  } finally {
    await env.teardown()
  }
}, 120_000)
