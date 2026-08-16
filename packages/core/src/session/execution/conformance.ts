// The SessionExecution conformance suite: the executable definition of what an executor must do.
// Any driver behind the SessionExecution seam registers the same scenarios through runContract; the
// built-in local executor runs it in core's tests, and the Temporal executor runs it against real
// workflows. The contract: wake drives a turn to settlement then the idle executor retires, resume
// forces a healthy turn to completion, resume surfaces the exact tagged RunError (through the same
// encode/decode path a process boundary uses), and interrupt cancels an in-flight turn and the
// session eventually leaves the active set.
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
import { SessionExecutionLocal } from "@opencode-ai/core/session/execution/local"
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
import { testEffect } from "../../testing/effect"

// The per-location service build resolves the session directory on disk, so it must exist.
const WORKSPACE = AbsolutePath.make(realpathSync(tmpdir()))

// Inert fixture data: the suite replaces LLMClient.Service with an injected stream, so nothing
// dials this endpoint or sends this token. The runner only needs a well-formed model descriptor
// to select and record; the .invalid TLD (RFC 2606) makes the inertness visible.
const model = OpenAIChat.route
  .with({ endpoint: { baseURL: "https://llm.fixture.invalid/v1" }, auth: Auth.bearer("fixture") })
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
// serve process builds it), with the model/LLM mocked. Any SessionExecution node with the standard
// dependency set (the local coordinator, the Temporal driver) plugs in here.
export const makeExecutionFor =
  (node: typeof SessionExecutionLocal.node) =>
  (stream: LLMClientShape["stream"], models = okModels) =>
    AppNodeBuilder.build(node, [
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

// Generous defaults: the Temporal driver adds worker startup, activity scheduling, and
// eventually-consistent visibility (about a second) on top of the turn itself.
const until = <A, E>(read: Effect.Effect<A, E>, predicate: (value: A) => boolean, timeoutMs = 20000) =>
  Effect.gen(function* () {
    const deadline = Date.now() + timeoutMs
    for (;;) {
      const value = yield* read
      if (predicate(value)) return value
      if (Date.now() > deadline) throw new Error("condition not reached in time")
      yield* Effect.sleep(50)
    }
  })

// The idle override is read at the executor's layer build (the Temporal client forwards it as a
// workflow argument), so it must be set before makeExec's layer is built and restored afterwards so
// later layer builds in this process get the real default.
const withIdleOverride = <A, E, R>(body: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const previous = process.env.OPENCODE_SESSION_IDLE_TIMEOUT
    process.env.OPENCODE_SESSION_IDLE_TIMEOUT = "2 seconds"
    try {
      return yield* body
    } finally {
      if (previous === undefined) delete process.env.OPENCODE_SESSION_IDLE_TIMEOUT
      else process.env.OPENCODE_SESSION_IDLE_TIMEOUT = previous
    }
  })

// The SessionExecution contract, parameterized over the driver factory. `makeExec` builds a
// SessionExecution graph the same way serve does, with the model/LLM mocked. Running the identical
// suite against a second factory is how the two modes are held to one behavior now that they no
// longer share a single coordination loop -- only the drain.
export const runContract = (label: string, makeExec: ReturnType<typeof makeExecutionFor>) => {
  // The nonce keeps workflow ids unique across runs: the Temporal driver derives durable workflow
  // ids from session ids, and on a shared dev server USE_EXISTING would otherwise route this run's
  // updates to a leftover workflow from an earlier one, parked on a task queue nobody polls.
  const slug = `${label.replace(/[^a-z0-9]+/gi, "_")}_${crypto.randomUUID().slice(0, 8)}`
  describe(`SessionExecution contract: ${label}`, () => {
    {
      const { requests, stream } = countingModel()
      const sessionID = SessionV2.ID.make(`ses_${slug}_wake`)
      it.live("wake drives a turn to settlement, then the idle executor retires", () =>
        withIdleOverride(
          Effect.gen(function* () {
            yield* seedSession(sessionID)
            yield* seedPrompt(sessionID)
            const exec = Context.get(yield* Layer.build(makeExec(stream)), SessionExecution.Service)
            yield* exec.wake(sessionID)
            const store = yield* SessionStore.Service
            yield* until(store.context(sessionID), (context) => {
              const assistant = context.findLast((message) => message.type === "assistant")
              return assistant?.type === "assistant" && Boolean(assistant.time.completed)
            })
            expect(requests).toHaveLength(1)
            // Idle self-termination: the executor retires without an interrupt. How long a settled
            // session lingers in `active` is the executor's business (the local coordinator retires
            // on settlement, the Temporal workflow serves until its idle timeout); the contract only
            // demands it eventually leaves.
            yield* until(exec.active, (active) => !active.has(sessionID))
          }),
        ),
        60000,
      )
    }

    {
      const { requests, stream } = countingModel()
      const sessionID = SessionV2.ID.make(`ses_${slug}_resume_ok`)
      it.live("resume forces a healthy turn to completion and resolves", () =>
        Effect.gen(function* () {
          yield* seedSession(sessionID)
          yield* seedPrompt(sessionID)
          const exec = Context.get(yield* Layer.build(makeExec(stream)), SessionExecution.Service)
          // resume is request/response: it awaits the forced drain and resolves on success.
          const exit = yield* exec.resume(sessionID).pipe(Effect.exit)
          expect(Exit.isSuccess(exit)).toBe(true)
          expect(requests).toHaveLength(1)
          const store = yield* SessionStore.Service
          const context = yield* store.context(sessionID)
          const assistant = context.findLast((message) => message.type === "assistant")
          expect(assistant?.type === "assistant" && Boolean(assistant.time.completed)).toBe(true)
        }),
        60000,
      )
    }

    {
      const sessionID = SessionV2.ID.make(`ses_${slug}_error`)
      const failingModels = SessionRunnerModel.layerWith(() => Effect.fail(new ModelNotSelectedError({ sessionID })))
      it.live("resume surfaces the exact tagged RunError through the shared codec", () =>
        Effect.gen(function* () {
          yield* seedSession(sessionID)
          const { stream } = countingModel()
          const exec = Context.get(yield* Layer.build(makeExec(stream, failingModels)), SessionExecution.Service)
          const exit = yield* exec.resume(sessionID).pipe(Effect.exit)
          expect(Exit.isFailure(exit)).toBe(true)
          const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
          // The same encode -> details -> decode path the Temporal boundary uses, so the caller gets
          // the identical tagged instance in both modes.
          expect(error).toBeInstanceOf(ModelNotSelectedError)
        }),
        60000,
      )
    }

    {
      const sessionID = SessionV2.ID.make(`ses_${slug}_interrupt`)
      it.live("interrupt cancels an in-flight turn and the session leaves the active set", () =>
        withIdleOverride(
          Effect.gen(function* () {
            yield* seedSession(sessionID)
            yield* seedPrompt(sessionID)
            // A model that never answers: the turn hangs until interrupted.
            const exec = Context.get(yield* Layer.build(makeExec(() => Stream.never)), SessionExecution.Service)
            yield* exec.wake(sessionID)
            yield* Effect.sleep(200)
            yield* until(exec.active, (active) => active.has(sessionID))
            yield* exec.interrupt(sessionID)
            // The Temporal supervisor keeps serving after an interrupt (a racing wake/resume must
            // not be lost); with nothing else queued it leaves the active set via idle retirement.
            yield* until(exec.active, (active) => !active.has(sessionID))
          }),
        ),
        60000,
      )
    }
  })
}
