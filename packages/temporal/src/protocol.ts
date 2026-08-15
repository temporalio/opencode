export * as TemporalProtocol from "./protocol"

// One source of truth for the names the client, the worker, and the tests must agree on. Workflows
// start by the string type, never the function: a minified (packaged) client would otherwise
// register the mangled function name as the type and no worker would match it.
export const WORKFLOW_TYPE = "sessionTurn"

export const SIGNALS = { wake: "wake", interrupt: "interrupt" } as const
export const RESUME_UPDATE = "resume"

export const WORKFLOW_ID_PREFIX = "session-exec-"
export const workflowId = (sessionID: string) => `${WORKFLOW_ID_PREFIX}${sessionID}`

export const DEFAULTS = {
  address: "127.0.0.1:7237",
  namespace: "default",
  taskQueue: "opencode-session-exec",
} as const
