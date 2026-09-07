// A standalone Temporal activity worker for the v2 durable execution, decoupled from the HTTP server.
// It builds the same application context serve uses (createServiceLayer) but hosts no HTTP surface:
// forcing SessionExecution constructs the Temporal worker, which then polls the task queue and runs
// session continuations. Run one or many of these next to (or instead of) an embedded worker.
//
//   OPENCODE_TEMPORAL_ROLE=worker OPENCODE_SESSION_EXECUTION=temporal \
//   TEMPORAL_ADDRESS=127.0.0.1:7237 OPENCODE_DB_URL=... \
//   bun run packages/server/src/worker.ts
//
// Note: file-touching tools run against the local working tree, so a worker must have the session's
// worktree present (co-locate by worktree, share the filesystem, or reconstruct from a snapshot).
import { Effect } from "effect"
import { createWorkerLayer } from "./routes"

const program = Effect.gen(function* () {
  // createWorkerLayer builds SessionExecution as a member, so providing it here starts the Temporal
  // worker (OPENCODE_TEMPORAL_ROLE must be worker or both).
  yield* Effect.logInfo("opencode v2 Temporal worker running").pipe(
    Effect.annotateLogs({ role: process.env.OPENCODE_TEMPORAL_ROLE ?? "both" }),
  )
  yield* Effect.never
}).pipe(Effect.provide(createWorkerLayer()), Effect.scoped)

Effect.runPromise(program).catch((error) => {
  console.error(error)
  process.exit(1)
})
