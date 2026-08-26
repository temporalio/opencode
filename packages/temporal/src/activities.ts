import { heartbeat } from "@temporalio/activity"
import * as oc from "./opencode"

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms))

export async function createSession(title?: string): Promise<string> {
  return oc.createSession(title)
}

/**
 * Run one turn idempotently and return the assistant's text.
 *
 * `turnIndex` is the 0-based position of this turn's user message in the session. The workflow
 * drives turns strictly in order, so the count of user messages already recorded is the
 * idempotency key: post the prompt only when this turn's user message does not exist yet. A
 * Temporal retry (e.g. after a worker crash) therefore never double-sends; it just resumes
 * polling. Because the turn runs server-side (prompt_async), it keeps going while the worker is
 * down, and recovery re-attaches by reading the recorded messages.
 */
export async function runTurn(input: { sessionID: string; turnIndex: number; text: string }): Promise<string> {
  const { sessionID, turnIndex, text } = input

  const userCount = (await oc.listMessages(sessionID)).filter((m) => m.info.role === "user").length
  if (userCount <= turnIndex) {
    await oc.promptAsync(sessionID, text)
  }

  // A turn produces several assistant messages (one per step). It is done when the session is idle
  // AND this turn's user message has at least one completed assistant message after it; the final
  // answer is the last such message's text.
  for (;;) {
    heartbeat()
    const msgs = await oc.listMessages(sessionID)
    const users = msgs.filter((m) => m.info.role === "user")
    if (users.length > turnIndex && (await oc.isIdle(sessionID))) {
      const userCreated = users[turnIndex].info.time?.created ?? 0
      const replies = msgs.filter(
        (m) =>
          m.info.role === "assistant" &&
          (m.info.time?.created ?? 0) >= userCreated &&
          m.info.time?.completed,
      )
      if (replies.length > 0) {
        const last = replies[replies.length - 1]
        return oc.assistantText(last) || replies.map(oc.assistantText).filter(Boolean).join("\n")
      }
    }
    await wait(1500)
  }
}

export async function abortTurn(sessionID: string): Promise<void> {
  await oc.abort(sessionID)
}
