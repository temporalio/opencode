// Contract tests for the in-process micro-driver: the SAME supervisor function the Temporal
// workflow runs (workflow-core.ts), driven with plain promises and no server. The contract is the
// SessionExecution interface: wake drives a turn to settlement, resume surfaces the exact tagged
// RunError (through the same encode/decode path the Temporal boundary uses), interrupt cancels an
// in-flight turn, and an idle supervisor retires itself.
import { LLMClient, type LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNodePlatform } from "@opencode-ai/core/effect/app-node-platform"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { Config } from "@opencode-ai/core/config"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionExecutionLocalDriver } from "@opencode-ai/core/session/execution/local-driver"
import { SessionRunnerModel, ModelNotSelectedError } from "@opencode-ai/core/session/runner/model"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { Prompt } from "@opencode-ai/core/session/prompt"
import { Location } from "@opencode-ai/core/location"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SkillGuidance } from "@opencode-ai/core/skill/guidance"
import { ReferenceGuidance } from "@opencode-ai/core/reference/guidance"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Auth } from "@opencode-ai/llm/route"
import { describe, expect } from "bun:test"
import { realpathSync } from "node:fs"
import { tmpdir } from "node:os"
import { Cause, Context, DateTime, Effect, Exit, Layer, Stream } from "effect"
import { testEffect } from "./lib/effect"

// The per-location service build resolves the session directory on disk, so it must exist.
const WORKSPACE = AbsolutePath.make(realpathSync(tmpdir()))

const model = OpenAIChat.route
  .with({ endpoint: { baseURL: "https://api.openai.com/v1" }, auth: Auth.bearer("fixture") })
  .model({ id: "gpt-4o-mini" })
const okModels = SessionRunnerModel.layerWith(() => Effect.succeed(model))
const systemContext = AppNodeBuilder.build(SystemContextRegistry.node)
const skillGuidance = Layer.mock(SkillGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const referenceGuidance = Layer.mock(ReferenceGuidance.Service, { load: () => Effect.succeed(SystemContext.empty) })
const config = Layer.succeed(Config.Service, Config.Service.of({ entries: () => Effect.succeed([]) }))
const permission = Layer.mock(PermissionV2.Service, {})

const mockClient = (stream: LLMClientShape["stream"]) =>
  Layer.succeed(
    LLMClient.Service,
    LLMClient.Service.of({
      prepare: () => Effect.die("unused"),
      generate: () => Effect.die("unused"),
      stream,
    }),
  )

const countingModel = () => {
  const requests: number[] = []
  const stream: LLMClientShape["stream"] = () => {
    requests.push(1)
    return Stream.fromIterable([LLMEvent.stepStart({ index: 0 }), LLMEvent.stepFinish({ index: 0, reason: "stop" })])
  }
  return { requests, stream }
}

// The executor under test, built as its own graph over the shared database file (the same way the
// serve process builds it), with the model/LLM mocked.
const makeExecution = (stream: LLMClientShape["stream"], models = okModels) =>
  AppNodeBuilder.build(SessionExecutionLocalDriver.node, [
    [LayerNodePlatform.llmClient, mockClient(stream)],
    [PermissionV2.node, permission],
    [ToolOutputStore.node, ToolOutputStore.nodeWithoutConfig],
    [SessionRunnerModel.node, models],
    [SystemContextRegistry.node, systemContext],
    [Location.node, Location.boundNode({ directory: WORKSPACE })],
    [SkillGuidance.node, skillGuidance],
    [ReferenceGuidance.node, referenceGuidance],
    [Config.node, config],
    [Snapshot.node, Snapshot.noopLayer],
  ])

// Reads and seeds go through a separate graph sharing the same database file.
const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, SessionProjector.node, SessionStore.node])),
)

const seedSession = (sessionID: SessionV2.ID) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: WORKSPACE, sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "t",
        directory: WORKSPACE,
        title: "t",
        version: "t",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
  })

const seedPrompt = (sessionID: SessionV2.ID) =>
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

const until = <A, E>(read: Effect.Effect<A, E>, predicate: (value: A) => boolean, timeoutMs = 8000) =>
  Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs
    for (;;) {
      const value = yield* read
      if (predicate(value)) return value
      if (Date.now() > deadline) throw new Error("condition not reached in time")
      yield* Effect.sleep(50)
    }
  })

describe("SessionExecution local micro-driver", () => {
  {
    const { requests, stream } = countingModel()
    const sessionID = SessionV2.ID.make("ses_driver_wake")
    it.live("wake drives a turn to settlement, then the idle supervisor retires", () =>
      Effect.gen(function* () {
        const previousIdle = process.env.OPENCODE_SESSION_IDLE_TIMEOUT
        process.env.OPENCODE_SESSION_IDLE_TIMEOUT = "2 seconds"
        yield* seedSession(sessionID)
        yield* seedPrompt(sessionID)
        const exec = Context.get(yield* Layer.build(makeExecution(stream)), SessionExecution.Service)
        yield* exec.wake(sessionID)
        const store = yield* SessionStore.Service
        yield* until(store.context(sessionID), (context) => {
          const assistant = context.findLast((message) => message.type === "assistant")
          return assistant?.type === "assistant" && Boolean(assistant.time.completed)
        })
        expect(requests).toHaveLength(1)
        expect((yield* exec.active).has(sessionID)).toBe(true)
        // Idle self-termination: the driver retires without an interrupt.
        yield* until(exec.active, (active) => !active.has(sessionID))
        // Restore so later layer builds in this process get the real default.
        if (previousIdle === undefined) delete process.env.OPENCODE_SESSION_IDLE_TIMEOUT
        else process.env.OPENCODE_SESSION_IDLE_TIMEOUT = previousIdle
      }),
    )
  }

  {
    const sessionID = SessionV2.ID.make("ses_driver_error")
    const failingModels = SessionRunnerModel.layerWith(() =>
      Effect.fail(new ModelNotSelectedError({ sessionID })),
    )
    it.live("resume surfaces the exact tagged RunError through the shared codec", () =>
      Effect.gen(function* () {
        yield* seedSession(sessionID)
        const { stream } = countingModel()
        const exec = Context.get(yield* Layer.build(makeExecution(stream, failingModels)), SessionExecution.Service)
        const exit = yield* exec.resume(sessionID).pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
        // The same encode -> details -> decode path the Temporal boundary uses, so the caller gets
        // the identical tagged instance in both modes.
        expect(error).toBeInstanceOf(ModelNotSelectedError)
      }),
    )
  }

  {
    const sessionID = SessionV2.ID.make("ses_driver_interrupt")
    it.live("interrupt cancels an in-flight turn and the driver retires", () =>
      Effect.gen(function* () {
        yield* seedSession(sessionID)
        yield* seedPrompt(sessionID)
        // A model that never answers: the turn hangs until interrupted.
        const exec = Context.get(
          yield* Layer.build(makeExecution(() => Stream.never)),
          SessionExecution.Service,
        )
        yield* exec.wake(sessionID)
        yield* Effect.sleep(200)
        expect((yield* exec.active).has(sessionID)).toBe(true)
        yield* exec.interrupt(sessionID)
        yield* until(exec.active, (active) => !active.has(sessionID), 4000)
      }),
    )
  }
})
