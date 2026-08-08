import { Client, Connection } from "@temporalio/client"
import { durableSession, submitPrompt, closeSession, getState } from "./workflows"

// Drives one durable session: start the workflow, queue prompts by signal, close it, wait, and
// print the recorded conversation. Nothing here holds the turn open; the workflow owns it.
async function main() {
  const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
  const taskQueue = process.env.TEMPORAL_TASK_QUEUE ?? "opencode-durable"

  const connection = await Connection.connect({ address })
  const client = new Client({ connection })

  const workflowId = process.env.WORKFLOW_ID ?? `oc-session-${Date.now()}`
  const prompts = process.argv.slice(2)
  if (prompts.length === 0) {
    prompts.push(
      "Create a file called note.txt containing exactly the token DURABLE_OK, then confirm.",
      "Run `cat note.txt` and reply with only its contents.",
    )
  }

  const handle = await client.workflow.start(durableSession, {
    taskQueue,
    workflowId,
    args: [{ title: "durable demo" }],
  })
  console.log(`started workflow ${workflowId}`)

  for (const p of prompts) await handle.signal(submitPrompt, p)
  await handle.signal(closeSession)

  await handle.result()
  const state = await handle.query(getState)
  console.log(JSON.stringify(state, null, 2))
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
