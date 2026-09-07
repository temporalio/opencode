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
}

export class Service extends Context.Service<Service, Interface>()("@opencode/temporal/Config") {}

export const fromEnv = (): Interface => ({
  address: process.env.TEMPORAL_ADDRESS ?? DEFAULTS.address,
  namespace: process.env.TEMPORAL_NAMESPACE ?? DEFAULTS.namespace,
  taskQueue: process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? DEFAULTS.taskQueue,
  role: (process.env.OPENCODE_TEMPORAL_ROLE as Role | undefined) ?? "both",
  idleTimeout: process.env.OPENCODE_SESSION_IDLE_TIMEOUT,
})
