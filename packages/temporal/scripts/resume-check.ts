// Verifies the v2 SessionExecution `resume` path: it must AWAIT the forced run and surface its
// result — resolve on a healthy session, reject on a failing one (a run error is no longer
// swallowed). Drives the workflow's `resume` Update via Update-with-Start, exactly as the
// SessionExecutionTemporal layer does. Needs the v2 server (OPENCODE_SESSION_EXECUTION=temporal)
// on :4601 and a Temporal dev server on :7237.

import { readFileSync } from "node:fs"
import { Client, Connection, WithStartWorkflowOperation } from "@temporalio/client"

const TEMPORAL = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
const QUEUE = process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? "opencode-session-exec"
const B = "http://127.0.0.1:4601/api"
const AUTH =
  "Basic " +
  Buffer.from("opencode:" + readFileSync(`${process.env.HOME}/.local/state/opencode/password`, "utf8").trim()).toString(
    "base64",
  )

const headers = { authorization: AUTH, "content-type": "application/json" }
const wait = (ms: number) => new Promise((r) => setTimeout(r, ms))

async function createSession(model?: { providerID: string; id: string }): Promise<string> {
  const r = await fetch(`${B}/session`, { method: "POST", headers, body: JSON.stringify(model ? { model } : {}) })
  const d: any = await r.json()
  return (d.data ?? d).id
}
async function prompt(sid: string, text: string): Promise<void> {
  await fetch(`${B}/session/${sid}/prompt`, { method: "POST", headers, body: JSON.stringify({ prompt: { text } }) })
}

async function resume(client: Client, sid: string): Promise<void> {
  const startOp = new WithStartWorkflowOperation("sessionExecution", {
    taskQueue: QUEUE,
    workflowId: `session-exec-${sid}`,
    args: [sid],
    workflowIdConflictPolicy: "USE_EXISTING" as any,
  })
  await client.workflow.executeUpdateWithStart("resume", { startWorkflowOperation: startOp, args: [] })
}

async function main() {
  const client = new Client({ connection: await Connection.connect({ address: TEMPORAL }) })

  const good = await createSession({ providerID: "openai", id: "gpt-5-mini" })
  await prompt(good, "Reply with exactly: HI")
  await wait(7000)
  let healthy = "?"
  try {
    await resume(client, good)
    healthy = "RESOLVED"
  } catch (e: any) {
    healthy = "REJECTED:" + (e?.message ?? String(e))
  }
  console.log("resume(healthy)  ->", healthy)

  const bad = await createSession() // no model -> default endpoint is unavailable, the run fails
  await prompt(bad, "Reply with exactly: HI")
  await wait(7000)
  let failing = "?"
  try {
    await resume(client, bad)
    failing = "RESOLVED (unexpected)"
  } catch (e: any) {
    failing = "REJECTED: " + String(e?.message ?? e).slice(0, 120)
  }
  console.log("resume(bad-model)->", failing)

  const pass = healthy === "RESOLVED" && failing.startsWith("REJECTED")
  console.log("RESUME-TYPED-ERROR:", pass ? "PASS" : "FAIL")
  process.exit(pass ? 0 : 1)
}

main().catch((e) => {
  console.error(e)
  process.exit(1)
})
