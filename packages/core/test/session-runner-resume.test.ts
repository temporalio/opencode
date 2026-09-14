// Resumability of a per-step turn (temporal mode): a Temporal step retry re-invokes runStep on the
// same durable log. These tests seed a crashed in-flight step (Step.Started + tool events, no
// Step.Ended) and drive runStep to check the two recovery behaviors:
//   - Slice 1: a dangling tool left by an interrupted attempt is closed on every step entry, not
//     just the first, so a re-drive never re-streams a request with a tool_use and no tool_result.
//   - Slice 2: a step whose tools already ran is finalized from the log without re-calling the model
//     (a dying mock LLM proves the model is never re-streamed) or re-running a completed tool.
import type { LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { createLLMEventPublisher } from "@opencode-ai/core/session/runner/publish-llm-event"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { Tool } from "@opencode-ai/core/tool/tool"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { describe, expect } from "bun:test"
import { Effect, Schema, Stream } from "effect"
import { emptyStep, runnerHarness, seedSession } from "./lib/runner-harness"

// The model call is what a resume must NOT repeat, so tests drive it explicitly: `dying` fails if
// the runner streams at all (proves resume skips the model); `emptyStep` is a benign one-step response.
const dying: LLMClientShape["stream"] = () => Stream.die("LLMClient.stream should not be called on resume")

const sessionID = SessionV2.ID.make("ses_runner_resume")

// Each seed records a step that started but never published Step.Ended -- what a worker crash mid
// turn leaves in the log.
const seededPublisher = Effect.gen(function* () {
  const events = yield* EventV2.Service
  return createLLMEventPublisher(events, {
    sessionID,
    agent: "build",
    model: { id: ModelV2.ID.make("gpt-4o-mini"), providerID: ProviderV2.ID.make("openai") },
  })
})
// Only a tool INPUT started -- the call was never dispatched, so no side effect ran and re-streaming
// is safe. This exercises slice 1 (close the dangling tool so the re-drive is not a poison loop).
const seedPendingStep = Effect.gen(function* () {
  const publisher = yield* seededPublisher
  yield* publisher.publish(LLMEvent.toolInputStart({ id: "call_pending", name: "read" }))
})
// Tools were dispatched: `call_done` recorded its result (completed), `call_running` did not (its
// side effect may have run). This exercises slice 2 (finalize from the log, never re-stream).
const seedDispatchedStep = Effect.gen(function* () {
  const publisher = yield* seededPublisher
  yield* publisher.publish(LLMEvent.toolCall({ id: "call_done", name: "read", input: { path: "a.txt" } }))
  yield* publisher.publish(
    LLMEvent.toolResult({
      id: "call_done",
      name: "read",
      result: { type: "content", value: [{ type: "text", text: "done" }] },
      output: { structured: {}, content: [{ type: "text", text: "done" }] },
    }),
  )
  yield* publisher.publish(LLMEvent.toolCall({ id: "call_running", name: "read", input: { path: "b.txt" } }))
})

const toolPart = (messages: ReadonlyArray<SessionMessage.Message>, callID: string) => {
  for (const message of messages) {
    if (message.type !== "assistant") continue
    for (const part of message.content) if (part.type === "tool" && part.id === callID) return part
  }
  return undefined
}

describe("SessionRunner resume", () => {
  runnerHarness(emptyStep).effect("closes a dangling tool on a mid-turn (first=false) re-drive", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      yield* seedPendingStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      yield* runner.runStep({ sessionID, step: 2, promotion: "steer", first: false, force: false })
      const part = toolPart(yield* store.context(sessionID), "call_pending")
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("error")
    }),
  )

  runnerHarness(dying).effect("finalizes a dispatched step from the log without re-calling the model", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      yield* seedDispatchedStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      // A dying stream would fail this call if the runner re-streamed; it resolving proves resume.
      const result = yield* runner.runStep({ sessionID, step: 3, promotion: "steer", first: false, force: false })
      expect(result.continue).toBe(true)
      const context = yield* store.context(sessionID)
      const done = toolPart(context, "call_done")
      const running = toolPart(context, "call_running")
      // The completed tool keeps its recorded result (not re-run); the unsettled one is failed.
      expect(done?.type === "tool" ? done.state.status : undefined).toBe("completed")
      expect(running?.type === "tool" ? running.state.status : undefined).toBe("error")
      // The step is finalized in place (no duplicate assistant message).
      const assistants = context.filter((message) => message.type === "assistant")
      expect(assistants).toHaveLength(1)
      expect(assistants[0]?.type === "assistant" ? Boolean(assistants[0].time.completed) : false).toBe(true)
    }),
  )

  runnerHarness(dying).effect("re-settles an idempotent tool on resume but fails a side-effecting one", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      // A pure-read tool is safe to re-run; a side-effecting one is not.
      yield* (yield* ApplicationTools.Service).register({
        probe_read: Tool.make({
          description: "read probe",
          idempotent: true,
          input: Schema.Struct({}),
          output: Schema.String,
          toModelOutput: ({ output }) => [{ type: "text", text: output }],
          execute: () => Effect.succeed("resettled"),
        }),
        probe_write: Tool.make({
          description: "write probe",
          input: Schema.Struct({}),
          output: Schema.String,
          execute: () => Effect.succeed("wrote"),
        }),
      })
      const publisher = yield* seededPublisher
      yield* publisher.publish(LLMEvent.toolCall({ id: "call_ro", name: "probe_read", input: {} }))
      yield* publisher.publish(LLMEvent.toolCall({ id: "call_rw", name: "probe_write", input: {} }))
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      // Dying stream: resume must not re-call the model; the idempotent tool is re-run via the registry.
      yield* runner.runStep({ sessionID, step: 4, promotion: "steer", first: false, force: false })
      const context = yield* store.context(sessionID)
      const readPart = toolPart(context, "call_ro")
      const writePart = toolPart(context, "call_rw")
      expect(readPart?.type === "tool" ? readPart.state.status : undefined).toBe("completed")
      expect(
        readPart?.type === "tool" && readPart.state.status === "completed"
          ? readPart.state.content.some((item) => item.type === "text" && item.text === "resettled")
          : false,
      ).toBe(true)
      expect(writePart?.type === "tool" ? writePart.state.status : undefined).toBe("error")
    }),
  )
})
