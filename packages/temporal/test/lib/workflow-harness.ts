// The real sessionTurn workflow (workflow.ts and supervisor.ts) against @temporalio/testing's
// in-process server, with the step activity mocked: no dev server, no provider. Each test file
// gets one environment, because two native Temporal servers in one bun process crash the runtime.
import { fileURLToPath } from "node:url"
import type { WorkflowHandle } from "@temporalio/client"
import { TestWorkflowEnvironment } from "@temporalio/testing"
import { Worker } from "@temporalio/worker"
import type { StepDrainResult } from "../../src/drain"

const WORKFLOW = fileURLToPath(new URL("../../src/workflow.ts", import.meta.url))

export interface HarnessOptions {
  /** Names the task queue, the workflow and the session, so two files never share a queue. */
  readonly name: string
  /** Time-skipping fast-forwards the idle timer; a real clock is needed when an activity blocks. */
  readonly timeSkipping?: boolean
  readonly runTurnStep: () => Promise<StepDrainResult>
}

/** Start the workflow with one wake and run `body` against its handle while a worker serves it. */
export async function withSessionWorkflow(
  options: HarnessOptions,
  body: (handle: WorkflowHandle) => Promise<void>,
): Promise<void> {
  const env = options.timeSkipping
    ? await TestWorkflowEnvironment.createTimeSkipping()
    : await TestWorkflowEnvironment.createLocal()
  try {
    const worker = await Worker.create({
      connection: env.nativeConnection,
      namespace: env.namespace,
      taskQueue: options.name,
      workflowsPath: WORKFLOW,
      activities: { runTurnStep: options.runTurnStep },
    })
    await worker.runUntil(async () => {
      const handle = await env.client.workflow.signalWithStart("sessionTurn", {
        taskQueue: options.name,
        workflowId: `wf-${options.name}`,
        args: [`ses_${options.name}`],
        signal: "wake",
        signalArgs: [],
      })
      await body(handle)
    })
  } finally {
    await env.teardown()
  }
}

export async function poll(fn: () => boolean, ms = 20_000): Promise<void> {
  const deadline = Date.now() + ms
  while (!fn()) {
    if (Date.now() > deadline) throw new Error("condition not reached")
    await new Promise((r) => setTimeout(r, 50))
  }
}
