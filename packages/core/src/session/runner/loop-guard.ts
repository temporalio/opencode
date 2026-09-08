import type { SessionMessage } from "../message"

/** Ceiling on provider attempts per run when the agent config sets no step limit. */
export const DEFAULT_MAX_STEPS = 200

/** Consecutive identical tool-call steps that count as a stuck loop. */
export const REPEAT_LIMIT = 3

export const REPEATED_CALLS_PROMPT = `CRITICAL - REPEATED IDENTICAL TOOL CALLS

The last several steps made exactly the same tool calls with exactly the same inputs. Repeating them
again will not produce a different result. Tools are disabled until next user input. Respond with
text only.

Response must include:
- A summary of what has been accomplished so far
- What the repeated tool calls were trying to achieve and why that appears blocked
- Recommendations for what should be done next

Do NOT make any tool calls. Respond with text ONLY.`

// One step's identity: the full set of its tool-call signatures (name + exact input). The entire
// set must repeat for a step to count -- a step that reads the same file but also makes a different
// edit is iterating, not stuck.
const stepSignature = (message: SessionMessage.Assistant) => {
  const signatures: string[] = []
  for (const part of message.content) {
    if (part.type !== "tool") continue
    signatures.push(`${part.name}\u0000${JSON.stringify(part.state.input)}`)
  }
  if (signatures.length === 0) return undefined
  return signatures.sort().join("\u0001")
}

// Steers, compaction, and differing calls break repetition so normal iteration can continue.
export const trailingIdenticalToolSteps = (context: ReadonlyArray<SessionMessage.Message>) => {
  let reference: string | undefined
  let count = 0
  for (let index = context.length - 1; index >= 0; index--) {
    const message = context[index]
    if (message?.type !== "assistant") break
    const signature = stepSignature(message)
    if (signature === undefined) break
    if (reference === undefined) reference = signature
    else if (signature !== reference) break
    count++
  }
  return count
}
