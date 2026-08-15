// The conformance suite run against the built-in local executor: SessionRunCoordinator behind the
// SessionExecution seam, whole turns through SessionRunner.run, no server. The same scenarios run
// against the Temporal executor in packages/temporal; the shared suite is what holds any executor
// to one behavior.
import { SessionExecutionLocal } from "@opencode-ai/core/session/execution/local"
import { makeExecutionFor, runContract } from "@opencode-ai/core/session/execution/conformance"

runContract("local executor", makeExecutionFor(SessionExecutionLocal.node))
