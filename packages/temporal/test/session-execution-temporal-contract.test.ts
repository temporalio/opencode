// The driver-contract suite run against the Temporal driver: the same scenarios the local
// coordinator passes, driven through real workflows on a Temporal server. It is what makes the
// parity between the two executors a test rather than a claim.
//
// It needs a dev server and generous timeouts (worker startup bundles the workflow), so it is
// opt-in:
//
//   temporal server start-dev --port 7237 --headless &
//   OPENCODE_CONTRACT_TEMPORAL=1 bun test --timeout 120000 test/session-execution-temporal-contract.test.ts
//
// Without the opt-in the file registers nothing, so a plain `bun test` stays server-free.
import { makeExecutionFor, runContract } from "../../core/test/lib/execution-conformance"

if (process.env.OPENCODE_CONTRACT_TEMPORAL === "1") {
  // One task queue per run: a stale worker from an earlier run against the same dev server would
  // otherwise steal activities and answer with its own (differently mocked) graph.
  process.env.OPENCODE_TEMPORAL_TASK_QUEUE ??= `contract-${crypto.randomUUID()}`
  // Imported inside the opt-in so a plain `bun test` never loads the Temporal SDK.
  const { SessionExecutionTemporal } = await import("@opencode-ai/temporal/executor")
  runContract("temporal driver", makeExecutionFor(SessionExecutionTemporal.node))
}
