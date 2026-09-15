export * as TemporalConfig from "./config"

// Connection and behavior settings for the Temporal executor. The executor reads them at layer
// build: an embedder or a test provides the service to override, and absent that the values come
// from env. Nothing reads env at module load, so import order carries no configuration.
import { Context } from "effect"
import { DEFAULTS } from "./protocol"

// `both` (default) hosts the activity worker AND the workflow client in one process (the serve
// process). `client` drives workflows without hosting a worker (a packaged binary cannot carry the
// worker's bundler); `worker` runs a standalone activity worker with no HTTP surface.
export type Role = "both" | "client" | "worker"

export interface Interface {
  readonly address: string
  readonly namespace: string
  readonly taskQueue: string
  readonly role: Role
  /** Override for the supervisor's idle self-termination; local mode honors the same variable. */
  readonly idleTimeout?: string
  /** Drive each step as a provider attempt, one activity per tool call, and a seal. Off by default:
   * the whole-step mode is what runs today, and this only changes how new sessions start. */
  readonly stepped?: boolean
  /** Run a step's tool calls one at a time instead of together. Tools of one step write the same
   * tree and each ships from the host that ran it, so two on two hosts each publish a tree without
   * the other's work: the second is refused rather than reverting the first, which leaves its work
   * stranded there. `OPENCODE_TEMPORAL_SERIAL_TOOLS=1` forces it on; it is on by default only where
   * the store is shared, which is what lets one step's tools land on different workers. */
  readonly serialTools?: boolean
}

export class Service extends Context.Service<Service, Interface>()("@opencode/temporal/Config") {}

const given = (name: string) => process.env[name] !== undefined && process.env[name] !== ""
const onOff = (name: string, fallback: boolean) => (given(name) ? process.env[name] === "1" : fallback)

export const fromEnv = (): Interface => {
  const sharedStore = !!process.env.OPENCODE_DB_URL
  return {
    address: process.env.TEMPORAL_ADDRESS ?? DEFAULTS.address,
    namespace: process.env.TEMPORAL_NAMESPACE ?? DEFAULTS.namespace,
    taskQueue: process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? DEFAULTS.taskQueue,
    role: (process.env.OPENCODE_TEMPORAL_ROLE as Role | undefined) ?? "both",
    idleTimeout: process.env.OPENCODE_SESSION_IDLE_TIMEOUT,
    stepped: onOff("OPENCODE_TEMPORAL_STEPPED", false),
    // Serial by default only where two hosts can end up writing one step's tree: a shared store,
    // with nothing keeping the step's tools on one worker.
    serialTools: onOff("OPENCODE_TEMPORAL_SERIAL_TOOLS", sharedStore),
  }
}
