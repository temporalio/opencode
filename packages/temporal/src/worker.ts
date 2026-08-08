import { fileURLToPath } from "node:url"
import { NativeConnection, Worker } from "@temporalio/worker"
import * as activities from "./activities"

async function main() {
  const address = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
  const namespace = process.env.TEMPORAL_NAMESPACE ?? "default"
  const taskQueue = process.env.TEMPORAL_TASK_QUEUE ?? "opencode-durable"

  const connection = await NativeConnection.connect({ address })
  const worker = await Worker.create({
    connection,
    namespace,
    taskQueue,
    workflowsPath: fileURLToPath(new URL("./workflows.ts", import.meta.url)),
    activities,
  })
  console.log(
    `opencode-temporal worker ready: queue=${taskQueue} temporal=${address} ` +
      `opencode=${process.env.OPENCODE_BASE_URL ?? "http://127.0.0.1:4599"}`,
  )
  await worker.run()
}

main().catch((err) => {
  console.error(err)
  process.exit(1)
})
