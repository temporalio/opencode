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
  /** Route a session's work to workers that already hold its project tree, instead of letting any
   * worker draw it and rebuild the tree from snapshot packs. Off by default, because it trades the
   * reconstruction fallback for latency: a session whose worktree has no worker polling waits
   * rather than being served elsewhere. */
  readonly worktreeAffinity?: boolean
  /** The worktree this worker serves, when affinity is on. Defaults to the process directory, which
   * is what a serve process with an embedded worker is already sitting in. */
  readonly worktree?: string
}

export class Service extends Context.Service<Service, Interface>()("@opencode/temporal/Config") {}

export const fromEnv = (): Interface => ({
  address: process.env.TEMPORAL_ADDRESS ?? DEFAULTS.address,
  namespace: process.env.TEMPORAL_NAMESPACE ?? DEFAULTS.namespace,
  taskQueue: process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? DEFAULTS.taskQueue,
  role: (process.env.OPENCODE_TEMPORAL_ROLE as Role | undefined) ?? "both",
  idleTimeout: process.env.OPENCODE_SESSION_IDLE_TIMEOUT,
  stepped: process.env.OPENCODE_TEMPORAL_STEPPED === "1",
  worktreeAffinity: process.env.OPENCODE_TEMPORAL_WORKTREE_AFFINITY === "1",
  worktree: process.env.OPENCODE_TEMPORAL_WORKTREE,
})
