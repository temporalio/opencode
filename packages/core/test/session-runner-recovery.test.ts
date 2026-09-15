// Re-drive eligibility: a crashed drain leaves no pending input rows (promotion consumed them
// inside the turn), so eligibility must come from the log. A promoted-but-unanswered prompt or an
// in-flight assistant makes a force=false re-drive run the turn; a settled history stays a no-op,
// so retries of a completed drain never spin the model.
import type { LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { createLLMEventPublisher } from "@opencode-ai/core/session/runner/publish-llm-event"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { describe, expect } from "bun:test"
import { DateTime, Effect } from "effect"
import { emptyStep, runnerHarness, seedSession } from "./lib/runner-harness"

// Counts provider calls and answers each with an empty completed step.
const countingModel = () => {
  const requests: number[] = []
  const stream: LLMClientShape["stream"] = (request) => {
    requests.push(1)
    return emptyStep(request)
  }
  return { requests, stream }
}

// A prompt whose input row was already consumed: only the projected user message remains, exactly
// what a crash after promotion leaves behind.
const seedPromotedPrompt = (sessionID: SessionV2.ID) =>
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    yield* events.publish(SessionEvent.Prompted, {
      sessionID,
      timestamp: yield* DateTime.now,
      messageID: SessionMessage.ID.create(),
      prompt: Prompt.make({ text: "do the thing" }),
      delivery: "queue",
    })
  })

describe("SessionRunner crash-recovery eligibility", () => {
  {
    const { requests, stream } = countingModel()
    const sessionID = SessionV2.ID.make("ses_recovery_prompted")
    runnerHarness(stream).effect("re-drives a promoted-but-unanswered prompt, then settles", () =>
      Effect.gen(function* () {
        yield* seedSession(sessionID)
        yield* seedPromotedPrompt(sessionID)
        const runner = yield* SessionRunner.Service
        yield* runner.run({ sessionID, force: false })
        expect(requests).toHaveLength(1)
        const context = yield* (yield* SessionStore.Service).context(sessionID)
        const assistant = context.findLast((message) => message.type === "assistant")
        expect(assistant?.type === "assistant" ? Boolean(assistant.time.completed) : false).toBe(true)
        // Settled history: a retried completed drain stays a no-op instead of re-calling the model.
        yield* runner.run({ sessionID, force: false })
        expect(requests).toHaveLength(1)
      }),
    )
  }

  {
    const { requests, stream } = countingModel()
    const sessionID = SessionV2.ID.make("ses_recovery_inflight")
    runnerHarness(stream).effect("re-drives an in-flight assistant and closes its dangling tool", () =>
      Effect.gen(function* () {
        yield* seedSession(sessionID)
        const events = yield* EventV2.Service
        const publisher = createLLMEventPublisher(events, {
          sessionID,
          agent: "build",
          model: { id: ModelV2.ID.make("gpt-4o-mini"), providerID: ProviderV2.ID.make("openai") },
        })
        yield* publisher.publish(LLMEvent.toolInputStart({ id: "call_dangling", name: "read" }))
        const runner = yield* SessionRunner.Service
        yield* runner.run({ sessionID, force: false })
        expect(requests).toHaveLength(1)
        const context = yield* (yield* SessionStore.Service).context(sessionID)
        for (const message of context) {
          if (message.type !== "assistant") continue
          for (const part of message.content)
            if (part.type === "tool" && part.id === "call_dangling") expect(part.state.status).toBe("error")
        }
      }),
    )
  }

  {
    const { requests, stream } = countingModel()
    const sessionID = SessionV2.ID.make("ses_recovery_step")
    runnerHarness(stream).effect("runStep(first) recovers the same window instead of no-opping", () =>
      Effect.gen(function* () {
        yield* seedSession(sessionID)
        yield* seedPromotedPrompt(sessionID)
        const runner = yield* SessionRunner.Service
        yield* runner.runStep({ sessionID, step: 1, promotion: undefined, first: true, force: false })
        expect(requests).toHaveLength(1)
      }),
    )
  }
})
