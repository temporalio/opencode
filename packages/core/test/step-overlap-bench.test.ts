// What the stepped split actually costs, measured rather than argued.
//
// A whole-step activity forks a tool fiber the moment a tool-call event arrives, so the tool runs
// while the model is still streaming. Splitting the step means the attempt has to return before any
// tool starts, because a workflow cannot consume a stream. The claim under test is that the loss is
// exactly the part of the stream that happened after the first tool call, bounded by how long the
// tool takes:
//
//     overlap loss per step  =  min(stream tail after the first tool call, tool duration)
//
// A mock model and a sleeping tool are used deliberately: both are dialled, so the number is the
// overlap and nothing else. No provider latency, no network, no Temporal round trip. The live cost
// is this plus two extra activity round trips per step, which is measured separately.
//
// Uses the harness's `live` variant: `.effect` installs a TestClock, under which Effect.sleep never
// advances and both the tool and the stream tail would hang forever.
//
// Opt-in, because it sleeps:
//   OPENCODE_OVERLAP_BENCH=1 bun test --timeout 60000 test/step-overlap-bench.test.ts
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
import { Location } from "@opencode-ai/core/location"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SkillGuidance } from "@opencode-ai/core/skill/guidance"
import { ReferenceGuidance } from "@opencode-ai/core/reference/guidance"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Auth } from "@opencode-ai/llm/route"
import { describe, expect } from "bun:test"
import { Duration, Effect, Layer, Schema, Stream } from "effect"
import { testEffect } from "./lib/effect"

// The dials. TAIL_MS is how much stream arrives after the first tool call; TOOL_MS is how long each
// tool takes. The predicted loss is min(TAIL_MS, TOOL_MS).
// `||` not `??`: an empty env var must fall back too, or the dials silently become NaN.
const num = (name: string, fallback: number) => Number(process.env[name] || fallback)
const TAIL_MS = num("BENCH_TAIL_MS", 1500)
const TOOL_MS = num("BENCH_TOOL_MS", 1500)
const TAIL_CHUNKS = TAIL_MS === 0 ? 0 : 3
const TAIL_CHUNK_MS = TAIL_CHUNKS === 0 ? 0 : TAIL_MS / TAIL_CHUNKS

const model = OpenAIChat.route
  .with({ endpoint: { baseURL: "https://api.openai.com/v1" }, auth: Auth.bearer("fixture") })
  .model({ id: "gpt-4o-mini" })
const models = SessionRunnerModel.layerWith(() => Effect.succeed(model))
const systemContext = AppNodeBuilder.build(SystemContextRegistry.node)
const skillGuidance = Layer.mock(SkillGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const referenceGuidance = Layer.mock(ReferenceGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const config = Layer.succeed(Config.Service, Config.Service.of({ entries: () => Effect.succeed([]) }))
const permission = Layer.mock(PermissionV2.Service, {})

// One tool call, then a stream that keeps going for TAIL_MS. That trailing stream is the whole
// question: it is what a whole-step activity overlaps with the tool, and what a split cannot.
const withTail: LLMClientShape["stream"] = () =>
  Stream.concat(
    Stream.fromIterable([
      LLMEvent.stepStart({ index: 0 }),
      LLMEvent.toolCall({ id: "call_bench", name: "bench_sleep", input: {} }),
      LLMEvent.textStart({ id: "txt_1" }),
    ]),
    Stream.concat(
      Stream.fromIterable(
        Array.from({ length: TAIL_CHUNKS }, (_, i) => LLMEvent.textDelta({ id: "txt_1", text: `chunk ${i} ` })),
      ).pipe(Stream.mapEffect((event) => Effect.sleep(Duration.millis(TAIL_CHUNK_MS)).pipe(Effect.as(event)))),
      Stream.fromIterable([LLMEvent.textEnd({ id: "txt_1" }), LLMEvent.stepFinish({ index: 0, reason: "tool-calls" })]),
    ),
  )

const harness = testEffect(
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
      [LayerNodePlatform.llmClient, Layer.succeed(
        LLMClient.Service,
        LLMClient.Service.of({
          prepare: () => Effect.die("unused"),
          generate: () => Effect.die("unused"),
          stream: withTail,
        }),
      )],
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

const seed = (sessionID: SessionV2.ID) =>
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
      .values({ id: sessionID, project_id: Project.ID.global, slug: "t", directory: "/project", title: "t", version: "t" })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* (yield* ApplicationTools.Service).register({
      bench_sleep: Tool.make({
        description: "sleeps",
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () => Effect.sleep(Duration.millis(TOOL_MS)).pipe(Effect.as("slept")),
      }),
    })
  })

const elapsed = <A, E>(body: Effect.Effect<A, E>) =>
  Effect.gen(function* () {
    const started = yield* Effect.sync(() => performance.now())
    yield* body
    return Math.round((yield* Effect.sync(() => performance.now())) - started)
  })

describe.skipIf(!process.env.OPENCODE_OVERLAP_BENCH)("step overlap cost", () => {
  harness.live("measures what deferring dispatch costs one step", () =>
    Effect.gen(function* () {
      const runner = yield* SessionRunner.Service
      const step = { step: 2, promotion: undefined, first: false, force: false } as const

      const fused = SessionV2.ID.make("ses_bench_fused")
      yield* seed(fused)
      // Whole step in one go: the tool forks the instant its call arrives, so it runs under the tail.
      const fusedMs = yield* elapsed(runner.runStep({ sessionID: fused, ...step }))

      const split = SessionV2.ID.make("ses_bench_split")
      yield* seed(split)
      // The same step, dispatched by a caller: the attempt returns first, then the tool runs.
      const splitMs = yield* elapsed(
        Effect.gen(function* () {
          const result = yield* runner.runModelCall({ sessionID: split, ...step })
          if (result.kind !== "called") return
          for (const call of result.calls) yield* runner.runToolCall({ sessionID: split, call, retry: false })
          yield* runner.sealStep({ sessionID: split, step: result.step, settlement: result.settlement })
        }),
      )

      const loss = splitMs - fusedMs
      const predicted = Math.min(TAIL_MS, TOOL_MS)
      console.log(
        `\n  stream tail after first call: ${TAIL_MS}ms   tool: ${TOOL_MS}ms` +
          `\n  whole step : ${fusedMs}ms` +
          `\n  split step : ${splitMs}ms` +
          `\n  loss       : ${loss}ms   (predicted min(tail, tool) = ${predicted}ms)\n`,
      )

      // The fused path overlaps the tool with the tail, so it cannot exceed the sum. The slack is
      // the step's own fixed cost (projection reads, snapshot, event writes), not overlap.
      expect(fusedMs).toBeLessThan(TAIL_MS + TOOL_MS + 400)
      // The loss is the overlap, within scheduling slop. Loose bounds: this is a measurement, and a
      // tight assertion here would only buy a flaky test. With no tail there is nothing to overlap,
      // so the floor does not apply.
      if (predicted > 200) expect(loss).toBeGreaterThan(predicted * 0.5)
      expect(loss).toBeLessThan(predicted + 400)
    }),
  )
})
