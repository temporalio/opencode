// Resumability of a per-step turn (temporal-turn): a Temporal step retry re-invokes runStep on the
// same durable log. These tests seed a crashed in-flight step (Step.Started + tool events, no
// Step.Ended) and drive runStep to check the two recovery behaviors:
//   - Slice 1: a dangling tool left by an interrupted attempt is closed on every step entry, not
//     just the first, so a re-drive never re-streams a request with a tool_use and no tool_result.
//   - Slice 2: a step whose tools already ran is finalized from the log without re-calling the model
//     (a dying mock LLM proves the model is never re-streamed) or re-running a completed tool.
import { LLMClient, type LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { AgentV2 } from "@opencode-ai/core/agent"
import { Config } from "@opencode-ai/core/config"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import * as SessionRunnerLLM from "@opencode-ai/core/session/runner/llm"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { createLLMEventPublisher } from "@opencode-ai/core/session/runner/publish-llm-event"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { Location } from "@opencode-ai/core/location"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SkillGuidance } from "@opencode-ai/core/skill/guidance"
import { ReferenceGuidance } from "@opencode-ai/core/reference/guidance"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Auth } from "@opencode-ai/llm/route"
import { describe, expect } from "bun:test"
import { Effect, Layer, Stream } from "effect"
import { testEffect } from "./lib/effect"

const model = OpenAIChat.route
  .with({ endpoint: { baseURL: "https://api.openai.com/v1" }, auth: Auth.bearer("fixture") })
  .model({ id: "gpt-4o-mini" })
const models = SessionRunnerModel.layerWith(() => Effect.succeed(model))
const systemContext = AppNodeBuilder.build(SystemContextRegistry.node)
const skillGuidance = Layer.mock(SkillGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const referenceGuidance = Layer.mock(ReferenceGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const config = Layer.succeed(Config.Service, Config.Service.of({ entries: () => Effect.succeed([]) }))
const permission = Layer.mock(PermissionV2.Service, {})

// The model call is what a resume must NOT repeat, so tests drive it explicitly: `dying` fails if
// the runner streams at all (proves resume skips the model); `empty` is a benign one-step response.
const mockClient = (stream: LLMClientShape["stream"]) =>
  Layer.succeed(
    LLMClient.Service,
    LLMClient.Service.of({
      prepare: () => Effect.die("LLMClient.prepare should not be called"),
      generate: () => Effect.die("LLMClient.generate should not be called"),
      stream,
    }),
  )
const dying: LLMClientShape["stream"] = () => Stream.die("LLMClient.stream should not be called on resume")
const empty: LLMClientShape["stream"] = () =>
  Stream.fromIterable([LLMEvent.stepStart({ index: 0 }), LLMEvent.stepFinish({ index: 0, reason: "stop" })])

const harness = (stream: LLMClientShape["stream"]) =>
  testEffect(
    AppNodeBuilder.build(
      LayerNode.group([
        Database.node,
        EventV2.node,
        SessionProjector.node,
        SessionStore.node,
        AgentV2.node,
        ToolRegistry.node,
        SessionRunnerModel.node,
        SystemContextRegistry.node,
        SkillGuidance.node,
        ReferenceGuidance.node,
        Config.node,
        Snapshot.node,
        SessionRunnerLLM.node,
      ]),
      [
        [LayerNodePlatform.llmClient, mockClient(stream)],
        [PermissionV2.node, permission],
        [ToolOutputStore.node, ToolOutputStore.nodeWithoutConfig],
        [SessionRunnerModel.node, models],
        [SystemContextRegistry.node, systemContext],
        [Location.node, Location.boundNode({ directory: AbsolutePath.make("/project") })],
        [SkillGuidance.node, skillGuidance],
        [ReferenceGuidance.node, referenceGuidance],
        [Config.node, config],
        [Snapshot.node, Snapshot.noopLayer],
      ],
    ),
  )

const sessionID = SessionV2.ID.make("ses_runner_resume")

const seedSession = Effect.gen(function* () {
  const { db } = yield* Database.Service
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
  yield* db
    .insert(SessionTable)
    .values({ id: sessionID, project_id: Project.ID.global, slug: "t", directory: "/project", title: "t", version: "t" })
    .onConflictDoNothing()
    .run()
    .pipe(Effect.orDie)
})

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
  harness(empty).effect("closes a dangling tool on a mid-turn (first=false) re-drive", () =>
    Effect.gen(function* () {
      yield* seedSession
      yield* seedPendingStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      yield* runner.runStep({ sessionID, step: 2, promotion: "steer", first: false, force: false })
      const part = toolPart(yield* store.context(sessionID), "call_pending")
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("error")
    }),
  )

  harness(dying).effect("finalizes a dispatched step from the log without re-calling the model", () =>
    Effect.gen(function* () {
      yield* seedSession
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
})
