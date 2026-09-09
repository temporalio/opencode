// Whether a session that is already running can be served by a worker carrying this code.
//
// What a stepped turn does after a pinned dispatch fails is a workflow decision, so it is written
// into every history that hits it: a run recorded before the rule failed the turn where this code
// seals it somewhere else and carries on. Replaying one of those against this code is a
// nondeterminism error unless the rule is behind a patch. It is, so the old runs keep the behaviour
// they recorded and nothing has to be drained before a deploy.
//
// Two directions, because a patch nothing replays through is a patch nobody knows is wired up: a
// history this code writes, and the kept ones under `fixture/histories`. Record a new fixture by
// reverting the rule, running this file with `RECORD_HISTORY=<name>`, and keeping the file it
// writes. They are the server's own wire form, because a fetched history does not survive a trip
// through proto3 JSON.

import { expect, it } from "bun:test"
import { readdir, readFile, writeFile } from "node:fs/promises"
import path from "path"
import { fileURLToPath } from "node:url"
import { ApplicationFailure } from "@temporalio/common"
import { temporal } from "@temporalio/proto"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"

const workflowsPath = fileURLToPath(new URL("../src/workflow.ts", import.meta.url))
const histories = fileURLToPath(new URL("./fixture/histories", import.meta.url))

it("replays a stepped turn that lost its host, its own and the kept ones", async () => {
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
          if (called) return { kind: "settled", result: { ran: true, continue: false, step: 2, promotion: null } }
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
        sealStep: async () => ({ ran: true, continue: true, step: 2, promotion: null }),
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
        sealStep: async () => ({ ran: true, continue: false, step: 2, promotion: null }),
      },
    })

    const history = await worker.runUntil(() =>
      pinned.runUntil(async () => {
        const handle = await env.client.workflow.start("sessionTurn", {
          workflowId: "replay-session",
          taskQueue: "replay-main",
          args: ["ses_replay", { stepped: true, startWithWake: false }],
        })
        // Recording runs against the code that predates the rule, where this update fails, and the
        // history is what the run is for either way.
        const resumed = await handle.executeUpdate("resume").then(
          () => "completed",
          () => "failed",
        )
        if (!process.env.RECORD_HISTORY) expect(resumed).toBe("completed")
        await handle.terminate().catch(() => undefined)
        return handle.fetchHistory()
      }),
    )

    const record = process.env.RECORD_HISTORY
    if (record) {
      // The wire form rather than JSON: a fetched history holds payloads the proto3 JSON converter
      // will not take, and a fixture that has been through a lossy encoding is not the history the
      // server wrote.
      const encoded = temporal.api.history.v1.History.encode(history).finish()
      await writeFile(path.join(histories, `${record}.bin`), Buffer.from(encoded))
      console.log(`recorded ${record}.bin`)
    }

    // The same code replaying its own history is the case that must always work.
    await Worker.runReplayHistory({ workflowsPath }, history)

    // And the kept ones, each written by the code that predates a rule this one changed.
    const kept = (await readdir(histories).catch(() => [] as string[])).filter((name) => name.endsWith(".bin"))
    expect(kept.length).toBeGreaterThan(0)
    for (const name of kept) {
      const older = temporal.api.history.v1.History.decode(await readFile(path.join(histories, name)))
      await Worker.runReplayHistory({ workflowsPath }, older)
    }
  } finally {
    await env.teardown()
  }
}, 120_000)
