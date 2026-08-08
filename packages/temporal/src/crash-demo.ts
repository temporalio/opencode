import { spawn, type ChildProcess } from "node:child_process"
import { Client, Connection } from "@temporalio/client"
import { durableSession, submitPrompt, closeSession, getState } from "./workflows"
import * as oc from "./opencode"

// Self-contained crash-recovery proof. It owns the worker lifecycle: start a turn, KILL the worker
// mid-turn, restart it, and show the workflow still completes, the runTurn activity was re-driven
// (attempt > 1), and no duplicate prompt was sent (exactly one user message). The turn itself keeps
// running in the opencode server while the worker is down; recovery re-attaches to it.

const ADDR = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
const QUEUE = process.env.TEMPORAL_TASK_QUEUE ?? "opencode-durable-crash"
const OPENCODE = process.env.OPENCODE_BASE_URL ?? "http://127.0.0.1:4599"

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms))

function startWorker(): ChildProcess {
  return spawn("bun", ["run", "worker"], {
    cwd: process.cwd(),
    env: { ...process.env, TEMPORAL_ADDRESS: ADDR, TEMPORAL_TASK_QUEUE: QUEUE, OPENCODE_BASE_URL: OPENCODE },
    stdio: "ignore",
    detached: true, // own process group, so we can kill the whole tree (bun -> tsx -> node)
  })
}

function killWorker(w: ChildProcess): void {
  try {
    process.kill(-(w.pid as number), "SIGKILL") // negative pid = the group
  } catch {
    w.kill("SIGKILL")
  }
}

async function main() {
  const connection = await Connection.connect({ address: ADDR })
  const client = new Client({ connection })

  console.log("[1] starting worker A")
  let worker = startWorker()
  await wait(10_000) // let it bundle + connect

  const workflowId = `crash-${Date.now()}`
  const handle = await client.workflow.start(durableSession, {
    taskQueue: QUEUE,
    workflowId,
    args: [{ title: "crash demo" }],
  })
  await handle.signal(submitPrompt, "Create a file crash.txt containing exactly RECOVERED, then read it back and reply with only its contents.")
  await handle.signal(closeSession)
  console.log(`[2] started workflow ${workflowId}, turn in progress`)

  await wait(4_000) // let the turn get going (prompt posted, polling)
  console.log("[3] KILLING worker A mid-turn")
  killWorker(worker)

  await wait(4_000) // worker down; the turn keeps running server-side
  console.log("[4] starting worker B (recovery)")
  worker = startWorker()

  console.log("[5] awaiting workflow completion ...")
  await handle.result()
  const state = await handle.query(getState)
  const reply = state.turns[0]?.reply ?? ""

  // Evidence from history: how many attempts did runTurn take?
  const events = (await handle.fetchHistory()).events ?? []
  const startedAttempts = events
    .filter((e: any) => e.activityTaskStartedEventAttributes)
    .map((e: any) => Number(e.activityTaskStartedEventAttributes.attempt ?? 1))
  const maxAttempt = startedAttempts.length ? Math.max(...startedAttempts) : 1

  // Idempotency: exactly one user message for the one prompt (no double-send on retry).
  const userMsgs = (await oc.listMessages(state.sessionID!)).filter((m) => m.info.role === "user").length

  console.log("\n=== RESULT ===")
  console.log("reply:", JSON.stringify(reply))
  console.log("runTurn attempts (max):", maxAttempt, "| started-event attempts:", startedAttempts)
  console.log("user messages in session:", userMsgs)
  const ok = reply.includes("RECOVERED") && maxAttempt >= 2 && userMsgs === 1
  console.log("CRASH-RECOVERY:", ok ? "PASS" : "FAIL")

  killWorker(worker)
  process.exit(ok ? 0 : 1)
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
