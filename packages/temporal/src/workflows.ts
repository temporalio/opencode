import { proxyActivities, defineSignal, defineQuery, setHandler, condition } from "@temporalio/workflow"
import type * as activities from "./activities"

// Unlimited retries with heartbeat: this is what makes a turn survive a worker crash. The turn
// runs server-side, so a re-run just re-attaches (idempotent on the user-message count).
const { runTurn } = proxyActivities<typeof activities>({
  startToCloseTimeout: "15 minutes",
  // Short heartbeat so a dead worker's in-flight turn is detected and re-driven quickly. The turn
  // keeps running server-side meanwhile, so the retry just re-attaches.
  heartbeatTimeout: "8 seconds",
})
// These calls never heartbeat, so they must not carry a heartbeat timeout (any call slower than it
// would fail spuriously). createSession is a non-idempotent POST: a retry after an ambiguous
// failure can orphan a server-side session, so retries are bounded.
const { createSession, abortTurn } = proxyActivities<typeof activities>({
  startToCloseTimeout: "1 minute",
  retry: { maximumAttempts: 3 },
})

export const submitPrompt = defineSignal<[string]>("submitPrompt")
export const abortSession = defineSignal("abortSession")
export const closeSession = defineSignal("closeSession")
export const getState = defineQuery<DurableSessionState>("getState")

export interface DurableSessionInput {
  title?: string
}

export interface DurableSessionState {
  sessionID?: string
  turns: Array<{ text: string; reply: string }>
  pending: number
}

/**
 * One durable opencode session. The conversation, the queue of prompts, and which turns have
 * completed all live in workflow state, so the session survives worker loss, can run detached or
 * in the background, and can be driven from anywhere by signal. The turns still execute in the
 * opencode server; this workflow is the durable brain that drives and remembers them.
 */
export async function durableSession(input: DurableSessionInput = {}): Promise<void> {
  const queue: string[] = []
  const turns: Array<{ text: string; reply: string }> = []
  let closed = false

  const sessionID = await createSession(input.title)

  setHandler(submitPrompt, (text) => {
    queue.push(text)
  })
  setHandler(closeSession, () => {
    closed = true
  })
  setHandler(abortSession, () => {
    // Best-effort: tell the server to abort the active turn; the running activity then returns the
    // (aborted) assistant message and the loop moves on. A floating rejection would fail the
    // workflow task, so it is swallowed.
    abortTurn(sessionID).catch(() => {})
  })
  setHandler(getState, () => ({ sessionID, turns, pending: queue.length }))

  for (;;) {
    await condition(() => queue.length > 0 || closed)
    if (queue.length === 0 && closed) break
    const text = queue.shift()!
    const reply = await runTurn({ sessionID, turnIndex: turns.length, text })
    turns.push({ text, reply })
  }
}
