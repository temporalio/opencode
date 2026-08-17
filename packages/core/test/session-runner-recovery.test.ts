// Re-drive eligibility: a crashed drain leaves no pending input rows (promotion consumed them
// inside the turn), so eligibility must come from the log. A promoted-but-unanswered prompt or an
// in-flight assistant makes a force=false re-drive run the turn; a settled history stays a no-op,
// so retries of a completed drain never spin the model.
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
import { SessionEvent } from "@opencode-ai/core/session/event"
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
import { Prompt } from "@opencode-ai/core/session/prompt"
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
import { DateTime, Effect, Layer, Stream } from "effect"
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

// Counts provider calls and answers each with an empty completed step.
const countingModel = () => {
  const requests: number[] = []
  const stream: LLMClientShape["stream"] = () => {
    requests.push(1)
    return Stream.fromIterable([LLMEvent.stepStart({ index: 0 }), LLMEvent.stepFinish({ index: 0, reason: "stop" })])
  }
  return { requests, stream }
}

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
        [
          LayerNodePlatform.llmClient,
          Layer.succeed(
            LLMClient.Service,
            LLMClient.Service.of({
              prepare: () => Effect.die("unused"),
              generate: () => Effect.die("unused"),
              stream,
            }),
          ),
        ],
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

const seedSession = (sessionID: SessionV2.ID) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "t",
        directory: "/project",
        title: "t",
        version: "t",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
  })

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
    harness(stream).effect("re-drives a promoted-but-unanswered prompt, then settles", () =>
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
    harness(stream).effect("re-drives an in-flight assistant and closes its dangling tool", () =>
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
    harness(stream).effect("runStep(first) recovers the same window instead of no-opping", () =>
      Effect.gen(function* () {
        yield* seedSession(sessionID)
        yield* seedPromotedPrompt(sessionID)
        const runner = yield* SessionRunner.Service
        const result = yield* runner.runStep({ sessionID, step: 1, promotion: undefined, first: true, force: false })
        expect(result.ran).toBe(true)
        expect(requests).toHaveLength(1)
      }),
    )
  }
})
