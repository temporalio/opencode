export * as TemporalProtocol from "./protocol"

// One source of truth for the names the client, the worker, and the tests must agree on. Workflows
// start by the string type, never the function: a minified (packaged) client would otherwise
// register the mangled function name as the type and no worker would match it.
export const WORKFLOW_TYPE = "sessionTurn"

export const SIGNALS = { wake: "wake", interrupt: "interrupt" } as const
export const RESUME_UPDATE = "resume"

/** The failure type a drain raises when the user stopped the turn (a declined permission, a
 * rejected question). The workflow has to tell it apart from a failed tool, so the name is shared
 * rather than spelled twice. */
export const HALTED_FAILURE_TYPE = "SessionRunDeclined"

export const WORKFLOW_ID_PREFIX = "session-exec-"
export const workflowId = (sessionID: string) => `${WORKFLOW_ID_PREFIX}${sessionID}`

export const DEFAULTS = {
  address: "127.0.0.1:7237",
  namespace: "default",
  taskQueue: "opencode-session-exec",
} as const
