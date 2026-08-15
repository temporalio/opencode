// The driver-contract suite run against the in-process native coordinator (local-driver.ts): a
// per-session async loop with no server. The suite itself lives in lib/session-execution-contract
// and also runs against the Temporal driver (session-execution-temporal-contract.test.ts); the two
// runs are what hold the modes to one behavior now that they share only the drain.
import { SessionExecutionLocalDriver } from "@opencode-ai/core/session/execution/local-driver"
import { makeExecutionFor, runContract } from "./lib/session-execution-contract"

runContract("local coordinator", makeExecutionFor(SessionExecutionLocalDriver.node))
