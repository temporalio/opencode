export * as TemporalConfig from "./config"

// Connection and behavior settings for the Temporal executor. The executor reads them at layer
// build: an embedder or a test provides the service to override, and absent that the values come
// from env. Nothing reads env at module load, so import order carries no configuration.
import { readFileSync } from "node:fs"
import { Context } from "effect"
import { DEFAULTS } from "./protocol"

// `both` (default) hosts the activity worker AND the workflow client in one process (the serve
// process). `client` drives workflows without hosting a worker (a packaged binary cannot carry the
// worker's bundler); `worker` runs a standalone activity worker with no HTTP surface.
export type Role = "both" | "client" | "worker"

// Which deployment this is. The settings below are not independent: a fleet whose store is not
// shared is a set of workers that cannot see each other's sessions, and finding that out takes a
// session that answers with the wrong files. `fleet` sets what has to agree, and `preflight`
// refuses what cannot.
export type Profile = "local" | "fleet"

export interface Interface {
  readonly profile: Profile
  readonly address: string
  readonly namespace: string
  readonly taskQueue: string
  readonly role: Role
  /** How a server that is not the dev server is reached: an API key for Cloud, a certificate pair
   * for a cluster with mTLS. Read from files, never from argv, and never logged. */
  readonly apiKey?: string
  readonly tls?: { readonly cert: string; readonly key: string; readonly ca?: string } | true
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
  /** Run a step's tool calls one at a time instead of together. Tools of one step write the same
   * tree and each ships from the host that ran it, so two on two hosts each publish a tree without
   * the other's work: the second is refused rather than reverting the first, which leaves its work
   * stranded there. `OPENCODE_TEMPORAL_SERIAL_TOOLS=1` forces it on; it is not needed while a step
   * is pinned to one worker, which is the default. */
  readonly serialTools?: boolean
  /** Send the tools and the seal of a step back to the worker that made its model call, on a queue
   * that worker polls on its own. That worker is standing in the tree the tools are about to write,
   * so the step's tools see each other's writes through the filesystem and can run at once. On by
   * default: a pin nobody answers falls back to the shared queue after a schedule-to-start bound,
   * so the worst it costs is that wait. `OPENCODE_TEMPORAL_STEP_AFFINITY=0` turns it off. */
  readonly stepAffinity?: boolean
}

export class Service extends Context.Service<Service, Interface>()("@opencode/temporal/Config") {}

const read = (path: string | undefined) => (path ? readFileSync(path, "utf8") : undefined)
const given = (name: string) => process.env[name] !== undefined && process.env[name] !== ""
const onOff = (name: string, fallback: boolean) => (given(name) ? process.env[name] === "1" : fallback)

export const fromEnv = (): Interface => ({
  profile: process.env.OPENCODE_TEMPORAL_PROFILE === "fleet" ? "fleet" : "local",
  address: process.env.TEMPORAL_ADDRESS ?? DEFAULTS.address,
  namespace: process.env.TEMPORAL_NAMESPACE ?? DEFAULTS.namespace,
  taskQueue: process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? DEFAULTS.taskQueue,
  role: (process.env.OPENCODE_TEMPORAL_ROLE as Role | undefined) ?? "both",
  apiKey: process.env.OPENCODE_TEMPORAL_API_KEY ?? read(process.env.OPENCODE_TEMPORAL_API_KEY_FILE),
  tls:
    process.env.OPENCODE_TEMPORAL_TLS_CERT && process.env.OPENCODE_TEMPORAL_TLS_KEY
      ? {
          cert: readFileSync(process.env.OPENCODE_TEMPORAL_TLS_CERT, "utf8"),
          key: readFileSync(process.env.OPENCODE_TEMPORAL_TLS_KEY, "utf8"),
          ca: read(process.env.OPENCODE_TEMPORAL_TLS_CA),
        }
      : process.env.OPENCODE_TEMPORAL_TLS === "1"
        ? true
        : undefined,
  idleTimeout: process.env.OPENCODE_SESSION_IDLE_TIMEOUT,
  // A fleet's unit of work is the smaller one: a worker dying takes one tool call with it rather
  // than a whole step, and a tool call is where the retry policy and the approval belong.
  stepped: onOff("OPENCODE_TEMPORAL_STEPPED", process.env.OPENCODE_TEMPORAL_PROFILE === "fleet"),
  worktreeAffinity: process.env.OPENCODE_TEMPORAL_WORKTREE_AFFINITY === "1",
  worktree: process.env.OPENCODE_TEMPORAL_WORKTREE,
  stepAffinity: process.env.OPENCODE_TEMPORAL_STEP_AFFINITY !== "0",
  serialTools:
    process.env.OPENCODE_TEMPORAL_SERIAL_TOOLS === "1" ||
    (process.env.OPENCODE_TEMPORAL_SERIAL_TOOLS !== "0" &&
      process.env.OPENCODE_TEMPORAL_STEP_AFFINITY === "0" &&
      process.env.OPENCODE_TEMPORAL_WORKTREE_AFFINITY !== "1" &&
      !!process.env.OPENCODE_DB_URL),
})

/** What `Connection.connect` and `NativeConnection.connect` both take, built once so a client and a
 * worker in different processes cannot disagree about how the cluster is reached. */
export const connectionOptions = (config: Interface) => {
  const tls =
    config.tls === true || (config.apiKey && config.tls === undefined)
      ? true
      : config.tls
        ? {
            clientCertPair: { crt: Buffer.from(config.tls.cert), key: Buffer.from(config.tls.key) },
            ...(config.tls.ca ? { serverRootCACertificate: Buffer.from(config.tls.ca) } : {}),
          }
        : undefined
  return {
    address: config.address,
    ...(tls ? { tls } : {}),
    ...(config.apiKey ? { apiKey: config.apiKey } : {}),
  }
}

const LOOPBACK = /^(127\.0\.0\.1|localhost|\[::1\]|0\.0\.0\.0)(:|$)/

/**
 * What is wrong with this deployment, said before it takes work rather than after. Each of these
 * fails as something else: a store only one process can see reads as a worker that never picks
 * anything up, and a client with no worker anywhere reads as a session that accepts a prompt and
 * never answers it.
 */
export const preflight = (config: Interface): string[] => {
  const problems: string[] = []
  const shared = !!process.env.OPENCODE_DB_URL
  if (config.profile === "fleet") {
    if (!shared)
      problems.push(
        "the fleet profile needs OPENCODE_DB_URL: the store is the record, and workers that do " +
          "not share it cannot serve each other's sessions",
      )
    if (config.role === "both")
      problems.push(
        "OPENCODE_TEMPORAL_ROLE is `both` in a fleet: a serve that also polls is a laptop " +
          "deployment. Run `client` next to standalone `worker` processes",
      )
  }
  if (config.apiKey && LOOPBACK.test(config.address))
    problems.push(`an API key is set but TEMPORAL_ADDRESS is ${config.address}, which is a dev server`)
  if (config.apiKey && config.namespace === "default")
    problems.push("an API key is set but TEMPORAL_NAMESPACE is `default`, which is not a Cloud namespace")
  if (!!process.env.OPENCODE_TEMPORAL_TLS_CERT !== !!process.env.OPENCODE_TEMPORAL_TLS_KEY)
    problems.push("OPENCODE_TEMPORAL_TLS_CERT and OPENCODE_TEMPORAL_TLS_KEY come as a pair")
  return problems
}

/**
 * Worth saying, not worth refusing. Only what cannot work belongs in `preflight`, because a process
 * that exits takes a deployment with it, and plaintext to an address that is not loopback is a
 * private network in most deployments and a mistake in some. Nothing here can tell which.
 */
export const notes = (config: Interface): string[] => {
  const said: string[] = []
  if (!LOOPBACK.test(config.address) && !config.apiKey && !config.tls)
    said.push(
      `reaching ${config.address} in plaintext. For Temporal Cloud set OPENCODE_TEMPORAL_API_KEY; ` +
        "for a cluster with mTLS set OPENCODE_TEMPORAL_TLS_CERT and OPENCODE_TEMPORAL_TLS_KEY",
    )
  return said
}

/** Every setting that decides how this process behaves, and nothing that is a credential. */
export const describe = (config: Interface): Record<string, string> => ({
  profile: config.profile,
  address: config.address,
  namespace: config.namespace,
  taskQueue: config.taskQueue,
  role: config.role,
  store: process.env.OPENCODE_DB_URL ? "shared (OPENCODE_DB_URL)" : "this process only",
  stepped: String(config.stepped === true),
  stepAffinity: String(config.stepAffinity !== false),
  serialTools: String(config.serialTools === true),
  credentials: config.apiKey ? "api key" : config.tls ? "certificate pair" : "none (plaintext)",
})
