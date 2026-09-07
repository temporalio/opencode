// Turn-level loop bounds: a run whose model repeats the exact same tool call step after step must
// terminate on its own -- after REPEAT_LIMIT identical steps the next attempt runs as a last step
// (tools disabled, text-only wrap-up) instead of looping forever. Driven end to end through
// SessionRunner.run with a mock LLM that always answers with the same tool call until tools are
// disabled. Also unit-covers the trailing-signature detection.
import { LLMClient, type LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
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
import { REPEAT_LIMIT, trailingIdenticalToolSteps } from "@opencode-ai/core/session/runner/loop-guard"
import { ToolRegistry } from "@opencode-ai/core/tool/registry"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { Tool } from "@opencode-ai/core/tool/tool"
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
import { describe, expect, test } from "bun:test"
import { DateTime, Effect, Layer, Schema, Stream } from "effect"
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

// Always answers with the same tool call until tools are disabled, then a bare text-less final step.
// Records each request's toolChoice so the test can assert the guard fired.
const stuckModel = () => {
  // LLM.request normalizes toolChoice into a ToolChoice class; record its `type`.
  const requests: Array<{ readonly tools: number; readonly toolChoice: string | undefined }> = []
  let attempt = 0
  const stream: LLMClientShape["stream"] = (request) => {
    const toolChoice = (request.toolChoice as { type?: string } | undefined)?.type
    requests.push({ tools: request.tools.length, toolChoice })
    attempt++
    if (toolChoice === "none" || request.tools.length === 0)
      return Stream.fromIterable([LLMEvent.stepStart({ index: 0 }), LLMEvent.stepFinish({ index: 0, reason: "stop" })])
    return Stream.fromIterable([
      LLMEvent.stepStart({ index: 0 }),
      LLMEvent.toolCall({ id: `call_${attempt}`, name: "probe_stuck", input: { target: "same" } }),
      LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
    ])
  }
  return { requests, stream }
}

const harness = (stream: LLMClientShape["stream"]) =>
  testEffect(
    AppNodeBuilder.build(
      LayerNode.group([
        Database.node,
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

const sessionID = SessionV2.ID.make("ses_loop_guard")

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

describe("SessionRunner loop guard", () => {
  const { requests, stream } = stuckModel()
  harness(stream).effect("ends a run whose model repeats the same tool call every step", () =>
    Effect.gen(function* () {
      yield* seedSession
      yield* (yield* ApplicationTools.Service).register({
        probe_stuck: Tool.make({
          description: "always same result",
          input: Schema.Struct({ target: Schema.String }),
          output: Schema.String,
          toModelOutput: ({ output }) => [{ type: "text", text: output }],
          execute: () => Effect.succeed("unchanged"),
        }),
      })
      const runner = yield* SessionRunner.Service
      // Terminates on its own: REPEAT_LIMIT identical tool steps, then one text-only wrap-up step.
      yield* runner.run({ sessionID, force: true })
      const context = yield* (yield* SessionStore.Service).context(sessionID)
      const assistants = context.filter((message) => message.type === "assistant")
      expect(assistants).toHaveLength(REPEAT_LIMIT + 1)
      const last = assistants.at(-1)
      expect(last?.type === "assistant" ? last.content.filter((part) => part.type === "tool") : undefined).toEqual([])
      // The guard, not the model, ended the run: the final request had tools disabled.
      expect(requests.at(-1)?.tools).toBe(0)
      expect(requests.at(-1)?.toolChoice).toBe("none")
      expect(requests).toHaveLength(REPEAT_LIMIT + 1)
    }),
  )

  test("trailing-signature detection counts only consecutive identical non-empty tool steps", () => {
    const created = DateTime.makeUnsafe(0)
    const assistant = (value: string, tools: Array<{ name: string; input: Record<string, unknown> }>) =>
      SessionMessage.Assistant.make({
        id: SessionMessage.ID.make(`msg_${value}`),
        type: "assistant",
        agent: "build",
        model: { id: ModelV2.ID.make("model"), providerID: ProviderV2.ID.make("provider") },
        content: tools.map((tool) =>
          SessionMessage.AssistantTool.make({
            type: "tool",
            id: `call_${value}_${tool.name}`,
            name: tool.name,
            time: { created },
            state: SessionMessage.ToolStateRunning.make({
              status: "running",
              input: tool.input,
              structured: {},
              content: [],
            }),
          }),
        ),
        time: { created, completed: created },
      })
    const same = { name: "read", input: { path: "a.txt" } }
    const other = { name: "edit", input: { path: "a.txt", change: 1 } }

    expect(trailingIdenticalToolSteps([assistant("1", [same]), assistant("2", [same]), assistant("3", [same])])).toBe(3)
    // A step that also does different work breaks the run (iterating, not stuck).
    expect(
      trailingIdenticalToolSteps([assistant("1", [same]), assistant("2", [same, other]), assistant("3", [same])]),
    ).toBe(1)
    // A text-only step breaks the run.
    expect(trailingIdenticalToolSteps([assistant("1", [same]), assistant("2", []), assistant("3", [same])])).toBe(1)
  })
})
