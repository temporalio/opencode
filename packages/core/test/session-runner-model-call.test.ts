// The model-only attempt (L2): runModelCall performs one provider attempt, records the tool calls it
// asked for, and stops. The caller dispatches each call as its own unit of work and seals the step
// afterwards, which is what puts the model-to-tools loop in a durable executor rather than inside a
// single activity. These tests pin the three properties that split depends on:
//   - the call is durable (Tool.Called) but its side effect has NOT run
//   - the provider-minted callID and the publisher's assistantMessageID are handed back, never
//     regenerated, so the dispatcher's result can be matched to the recorded call
//   - the step is left open (no Step.Ended), because the tools have not run yet
// The last test is the contrast: the same stream through runStep runs the tool, as L1 does today.
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
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { Tool } from "@opencode-ai/core/tool/tool"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Location } from "@opencode-ai/core/location"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SkillGuidance } from "@opencode-ai/core/skill/guidance"
import { ReferenceGuidance } from "@opencode-ai/core/reference/guidance"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Auth } from "@opencode-ai/llm/route"
import { describe, expect } from "bun:test"
import { Effect, Layer, Schema, Stream } from "effect"
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

const mockClient = (stream: LLMClientShape["stream"]) =>
  Layer.succeed(
    LLMClient.Service,
    LLMClient.Service.of({
      prepare: () => Effect.die("LLMClient.prepare should not be called"),
      generate: () => Effect.die("LLMClient.generate should not be called"),
      stream,
    }),
  )

// One tool call, then a clean finish: the shape a step that wants to keep going produces.
const callsTool: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_write", input: {} }),
    LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  ])
// An answer and no tool call: the step is over as soon as the stream is, but the seal still has to
// happen, and there is a real assistant message for it to complete.
const textOnly: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.textStart({ id: "txt_1" }),
    LLMEvent.textDelta({ id: "txt_1", text: "done" }),
    LLMEvent.textEnd({ id: "txt_1" }),
    LLMEvent.stepFinish({ index: 0, reason: "stop" }),
  ])

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
        ApplicationTools.node,
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

const sessionID = SessionV2.ID.make("ses_runner_model_call")

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

// A side-effecting tool that counts its own executions, so "the call was recorded but not run" is
// checked against the tool itself rather than only against the projection.
const registerProbe = (ran: { count: number }) =>
  Effect.gen(function* () {
    yield* (yield* ApplicationTools.Service).register({
      probe_write: Tool.make({
        description: "write probe",
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () =>
          Effect.sync(() => {
            ran.count += 1
            return "wrote"
          }),
      }),
    })
  })

const toolPart = (messages: ReadonlyArray<SessionMessage.Message>, callID: string) => {
  for (const message of messages) {
    if (message.type !== "assistant") continue
    for (const part of message.content) if (part.type === "tool" && part.id === callID) return part
  }
  return undefined
}

const assistant = (messages: ReadonlyArray<SessionMessage.Message>) =>
  messages.find((message) => message.type === "assistant")

describe("SessionRunner model-only attempt", () => {
  harness(callsTool).effect("records the tool call, hands it back, and does not run it", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = { count: 0 }
      yield* registerProbe(ran)
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const result = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })

      expect(result.kind).toBe("called")
      if (result.kind !== "called") return
      // The provider's callID and the publisher's assistant message id are carried out, because a
      // second run of this step would mint different ones and nothing would match the log.
      expect(result.calls.map((call) => call.id)).toEqual(["call_probe"])
      expect(result.calls[0]?.name).toBe("probe_write")
      expect(result.calls[0]?.assistantMessageID).toBeTruthy()
      // The provider's finish reason has to travel with the calls: it lives only in the publisher's
      // memory, so whoever seals the step in another process cannot read it back from the log.
      expect(result.settlement?.finish).toBe("tool-calls")
      expect(ran.count).toBe(0)

      const context = yield* store.context(sessionID)
      const part = toolPart(context, "call_probe")
      // Durably recorded and left mid-flight: this is what a dispatcher picks up.
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("running")
      // The step stays open, so no Step.Ended and no completed assistant message.
      const message = assistant(context)
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : true).toBe(false)
    }),
  )

  harness(textOnly).effect("leaves a text-only step open with no calls to dispatch", () =>
    Effect.gen(function* () {
      yield* seedSession
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const result = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })

      expect(result.kind).toBe("called")
      if (result.kind !== "called") return
      expect(result.calls).toHaveLength(0)
      expect(result.settlement?.finish).toBe("stop")
      // Sealing is uniform: even with nothing to dispatch, the step is closed by the seal, not here,
      // so the answer is recorded but its message is still open.
      const message = assistant(yield* store.context(sessionID))
      expect(message?.type).toBe("assistant")
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : true).toBe(false)
    }),
  )

  harness(callsTool).effect("still runs the tool and closes the step through runStep", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = { count: 0 }
      yield* registerProbe(ran)
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      yield* runner.runStep({ sessionID, step: 2, promotion: undefined, first: false, force: false })

      // The contrast that makes the split meaningful: the whole-step path dispatches and seals.
      expect(ran.count).toBe(1)
      const context = yield* store.context(sessionID)
      const part = toolPart(context, "call_probe")
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("completed")
      const message = assistant(context)
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : false).toBe(true)
    }),
  )
})
