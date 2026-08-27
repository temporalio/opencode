// The driver-contract suite run against the Temporal driver: the same scenarios the local
// coordinator passes, driven through real workflows on a Temporal server. This is the second half
// of the parity story in packages/temporal/README.md "Two modes, one drain".
//
// It needs a dev server and generous timeouts (worker startup bundles the workflow), so it is
// opt-in:
//
//   temporal server start-dev --port 7237 --headless &
//   OPENCODE_CONTRACT_TEMPORAL=1 \
//     bun test --timeout 120000 test/session-execution-temporal-contract.test.ts
//
// Without the opt-in the file registers nothing, so a plain `bun test` stays server-free.
import { makeExecutionFor, runContract } from "@opencode-ai/core/session/execution/conformance"

if (process.env.OPENCODE_CONTRACT_TEMPORAL === "1") {
  // One task queue per run: a stale worker from an earlier run against the same dev server would
  // otherwise steal activities and answer with its own (differently mocked) graph.
  process.env.OPENCODE_TEMPORAL_TASK_QUEUE ??= `contract-${crypto.randomUUID()}`
  // The stepped mode has to satisfy the SAME contract, or the two Temporal modes drift and the
  // parity story only holds for the one that happens to be tested. Which mode this run covers comes
  // from the same env the executor reads, so the label never disagrees with what actually ran:
  //
  //   OPENCODE_CONTRACT_TEMPORAL=1                              -> whole step per activity
  //   OPENCODE_CONTRACT_TEMPORAL=1 OPENCODE_TEMPORAL_STEPPED=1  -> model call, per tool, seal
  const stepped = process.env.OPENCODE_TEMPORAL_STEPPED === "1"
  // Imported dynamically because the driver reads its connection config at module load.
  const { SessionExecutionTemporal } = await import("@opencode-ai/temporal/executor")
  const label = stepped ? "temporal driver (stepped)" : "temporal driver"
  runContract(label, makeExecutionFor(SessionExecutionTemporal.node))
}
