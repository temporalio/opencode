// The model-only attempt (L2): runModelCall performs one provider attempt, records the tool calls
// it
// asked for, and stops. The caller dispatches each call as its own unit of work and seals the step
// afterwards, which is what puts the model-to-tools loop in a durable executor rather than inside a
// single activity. These tests pin the three properties that split depends on:
//   - the call is durable but not started, so its side effect has NOT run
//   - the provider-minted callID and the publisher's assistantMessageID are handed back, never
//     regenerated, so the dispatcher's result can be matched to the recorded call
//   - the step is left open (no Step.Ended), because the tools have not run yet
// The last test is the contrast: the same stream through runStep runs the tool, as L1 does today.
import { LLMClient, type LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { Database } from "@opencode-ai/core/database/database"
import { EventTable } from "@opencode-ai/core/event/sql"
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
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionRunDeclinedError } from "@opencode-ai/core/session/error"
import { DateTime } from "effect"
import { ConfigCompaction } from "@opencode-ai/core/config/compaction"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { createLLMEventPublisher } from "@opencode-ai/core/session/runner/publish-llm-event"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { eq } from "drizzle-orm"
import { Location } from "@opencode-ai/core/location"
import { SystemContextRegistry } from "@opencode-ai/core/system-context/registry"
import { SystemContext } from "@opencode-ai/core/system-context"
import { SkillGuidance } from "@opencode-ai/core/skill/guidance"
import { ReferenceGuidance } from "@opencode-ai/core/reference/guidance"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Auth } from "@opencode-ai/llm/route"
import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Layer, Schema, Stream } from "effect"
import { testEffect } from "./lib/effect"

const model = OpenAIChat.route
  .with({ endpoint: { baseURL: "https://api.openai.com/v1" }, auth: Auth.bearer("fixture") })
  .model({ id: "gpt-4o-mini" })
const models = SessionRunnerModel.layerWith(() => Effect.succeed(model))
const systemContext = AppNodeBuilder.build(SystemContextRegistry.node)
const skillGuidance = Layer.mock(SkillGuidance.Service, {
  load: () => Effect.succeed(SystemContext.empty),
})
const referenceGuidance = Layer.mock(ReferenceGuidance.Service, {
  load: () => Effect.succeed(SystemContext.empty),
})
const config = Layer.succeed(
  Config.Service,
  Config.Service.of({ entries: () => Effect.succeed([]) }),
)
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
// Calls the tool that declines.
const callsDecliningTool: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_declines", input: {} }),
    LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  ])
// A tool call, then the provider dies mid-stream. The calls are recorded but the turn is over.
const callsToolThenFails: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_write", input: {} }),
    LLMEvent.providerError({ message: "upstream exploded" }),
  ])
// The same, for the tool that declares itself repeatable.
const callsIdempotentTool: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_read", input: {} }),
    LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  ])
// Calls the tools that die with their side effect in flight, which is what a worker crash leaves
// behind for the next dispatch to read.
const callsCrashingTool: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_crashes", input: {} }),
    LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  ])
const callsCrashingIdempotentTool: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_crashes_read", input: {} }),
    LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  ])
// A provider turn that publishes nothing at all: no text, no reasoning, no tool call. The publisher
// mints the assistant message lazily on first content, so after this stream there is no message in
// the log for a seal to find. The whole-step path survives it because Step.Ended mints one on the
// way past; a seal running in another process has no publisher to mint with.
const silent: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.stepFinish({ index: 0, reason: "stop" }),
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

// A store that cannot keep what a tool produced. The tool itself still ran by then, which is the
// case a dispatch has to report honestly rather than as a call that never finished.
const failingOutputStore = Layer.mock(ToolOutputStore.Service, {
  bound: () =>
    Effect.fail(
      new ToolOutputStore.StorageError({ operation: "write", cause: new Error("disk full") }),
    ),
})

const harness = (
  stream: LLMClientShape["stream"],
  outputStore:
    | typeof ToolOutputStore.nodeWithoutConfig
    | Layer.Layer<ToolOutputStore.Service> = ToolOutputStore.nodeWithoutConfig,
) =>
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
        [ToolOutputStore.node, outputStore],
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

// Probes that count their own executions, so "recorded but not run" and "not run twice" are
// checked against the tools themselves rather than only against the projection. The read probes
// declare themselves repeatable; the write probes do not, which is what decides whether a second
// dispatch runs the tool again.
const registerProbes = (ran: { write: number; read: number }) =>
  Effect.gen(function* () {
    yield* (yield* ApplicationTools.Service).register({
      probe_write: Tool.make({
        description: "write probe",
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () =>
          Effect.sync(() => {
            ran.write += 1
            return "wrote"
          }),
      }),
      // permission.assert declines by dying with this, so a tool that dies the same way exercises
      // the same classification without needing the permission service in the tool's context.
      probe_declines: Tool.make({
        description: "declines",
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () => Effect.die(new PermissionV2.DeclinedError()),
      }),
      probe_read: Tool.make({
        description: "read probe",
        idempotent: true,
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () =>
          Effect.sync(() => {
            ran.read += 1
            return "read"
          }),
      }),
      // Dying with the side effect already done is the case the log has to survive: the call is
      // left in flight and no later reader can say whether the write landed.
      probe_crashes: Tool.make({
        description: "crashing write probe",
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () =>
          Effect.gen(function* () {
            ran.write += 1
            return yield* Effect.die(new Error("worker died"))
          }),
      }),
      // The repeatable one, dying only on its first run so a second dispatch has a result to
      // produce.
      probe_crashes_read: Tool.make({
        description: "crashing read probe",
        idempotent: true,
        input: Schema.Struct({}),
        output: Schema.String,
        execute: () =>
          Effect.gen(function* () {
            ran.read += 1
            if (ran.read === 1) return yield* Effect.die(new Error("worker died"))
            return "read"
          }),
      }),
    })
  })

const counters = () => ({ write: 0, read: 0 })

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
      const ran = counters()
      yield* registerProbes(ran)
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
      expect(ran.write).toBe(0)

      const context = yield* store.context(sessionID)
      const part = toolPart(context, "call_probe")
      // Durably recorded and not started: whoever dispatches the call publishes Tool.Called, so
      // `running` in the log means a process was about to run the tool. Recording it here instead
      // would make every later dispatch of a crashed step report an unknown outcome for a tool
      // nobody had touched.
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("pending")
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
      // Sealing is uniform: even with nothing to dispatch, the step is closed by the seal, not
      // here,
      // so the answer is recorded but its message is still open.
      const message = assistant(yield* store.context(sessionID))
      expect(message?.type).toBe("assistant")
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : true).toBe(false)
    }),
  )

  harness(callsTool).effect("still runs the tool and closes the step through runStep", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      yield* runner.runStep({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })

      // The contrast that makes the split meaningful: the whole-step path dispatches and seals.
      expect(ran.write).toBe(1)
      const context = yield* store.context(sessionID)
      const part = toolPart(context, "call_probe")
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("completed")
      const message = assistant(context)
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : false).toBe(true)
    }),
  )
})

// Dispatching one recorded call on its own. The policy under test is what happens when a dispatch
// has already started the tool and died, so the side effect may have run and nothing can say:
// repeatable tools run again, the rest are reported unknown so the model decides. Same rule the
// crash-resume path already follows, off the same evidence.
describe("SessionRunner tool dispatch", () => {
  const deferOneCall = Effect.gen(function* () {
    const runner = yield* SessionRunner.Service
    const result = yield* runner.runModelCall({
      sessionID,
      step: 2,
      promotion: undefined,
      first: false,
      force: false,
    })
    if (result.kind !== "called" || !result.calls[0]) throw new Error("expected a deferred call")
    return result.calls[0]
  })

  harness(callsTool).effect("runs a deferred call and records its result", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const result = yield* runner.runToolCall({ sessionID, call })

      expect(result.outcome).toBe("settled")
      expect(ran.write).toBe(1)
      const part = toolPart(yield* store.context(sessionID), "call_probe")
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("completed")
    }),
  )

  harness(callsTool).effect("does nothing when the call already has a result", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service

      yield* runner.runToolCall({ sessionID, call })
      // A duplicate dispatch: at-least-once delivery means this happens, and it must not re-run.
      const second = yield* runner.runToolCall({ sessionID, call })

      expect(second.outcome).toBe("already-settled")
      expect(ran.write).toBe(1)
    }),
  )

  harness(callsCrashingTool).effect(
    "reports a side-effecting call whose dispatch already started as unknown",
    () =>
      Effect.gen(function* () {
        yield* seedSession
        const ran = counters()
        yield* registerProbes(ran)
        const call = yield* deferOneCall
        const runner = yield* SessionRunner.Service
        const store = yield* SessionStore.Service

        // A dispatch that died with the tool in flight. What it leaves in the log is the whole
        // evidence a later one gets: the call recorded as running, and no result.
        const crashed = yield* runner.runToolCall({ sessionID, call }).pipe(Effect.exit)
        expect(Exit.isFailure(crashed)).toBe(true)
        expect(ran.write).toBe(1)

        const result = yield* runner.runToolCall({ sessionID, call })

        expect(result.outcome).toBe("unknown")
        expect(ran.write).toBe(1)
        const part = toolPart(yield* store.context(sessionID), "call_probe")
        expect(part?.type === "tool" ? part.state.status : undefined).toBe("error")
      }),
  )

  harness(callsTool, failingOutputStore).effect(
    "tells the model why a dispatched call failed",
    () =>
      Effect.gen(function* () {
        yield* seedSession
        const ran = counters()
        yield* registerProbes(ran)
        const call = yield* deferOneCall
        const runner = yield* SessionRunner.Service
        const store = yield* SessionStore.Service

        const result = yield* runner.runToolCall({ sessionID, call })

        // The tool ran and only its output was lost. Letting that fail the dispatch would repeat
        // the write on the next attempt, and the step would finally close the call as interrupted:
        // a reason the model cannot act on, and not what happened to it.
        expect(result.outcome).toBe("failed")
        expect(ran.write).toBe(1)
        const part = toolPart(yield* store.context(sessionID), "call_probe")
        const failure =
          part?.type === "tool" && part.state.status === "error" ? part.state.error : undefined
        expect(failure?.message).toContain("disk full")
      }),
  )

  harness(callsCrashingIdempotentTool).effect(
    "re-runs a started call that declares itself repeatable",
    () =>
      Effect.gen(function* () {
        yield* seedSession
        const ran = counters()
        yield* registerProbes(ran)
        const call = yield* deferOneCall
        const runner = yield* SessionRunner.Service
        const store = yield* SessionStore.Service

        yield* runner.runToolCall({ sessionID, call }).pipe(Effect.exit)
        const result = yield* runner.runToolCall({ sessionID, call })

        expect(result.outcome).toBe("settled")
        expect(ran.read).toBe(2)
        const part = toolPart(yield* store.context(sessionID), "call_probe")
        expect(part?.type === "tool" ? part.state.status : undefined).toBe("completed")
      }),
  )
})

// Closing the step after its calls have been dispatched. This is the piece that cannot stay in the
// provider attempt: the end snapshot and the file diff have to be taken after the tools have run,
// and in a durable executor that is a different process.
describe("SessionRunner step seal", () => {
  const stepEndedCount = Effect.gen(function* () {
    const { db } = yield* Database.Service
    const rows = yield* db
      .select({ type: EventTable.type })
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, sessionID))
      .all()
      .pipe(Effect.orDie)
    return rows.filter((row) => row.type.includes("step.ended")).length
  })

  const deferOneCall = Effect.gen(function* () {
    const runner = yield* SessionRunner.Service
    const result = yield* runner.runModelCall({
      sessionID,
      step: 2,
      promotion: undefined,
      first: false,
      force: false,
    })
    if (result.kind !== "called") throw new Error("expected a deferred step")
    return result
  })

  harness(callsTool).effect("closes a dispatched step and keeps the turn going", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const model = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      yield* runner.runToolCall({ sessionID, call: model.calls[0]! })

      const result = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      // Local tool calls mean the model has results to look at, so the turn continues.
      expect(result.continue).toBe(true)
      expect(result.step).toBe(3)
      const message = assistant(yield* store.context(sessionID))
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : false).toBe(true)
      expect(yield* stepEndedCount).toBe(1)
    }),
  )

  harness(textOnly).effect("closes a text-only step and ends the turn", () =>
    Effect.gen(function* () {
      yield* seedSession
      const model = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const result = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      expect(result.continue).toBe(false)
      const message = assistant(yield* store.context(sessionID))
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : false).toBe(true)
    }),
  )

  harness(callsTool).effect("seals once and answers the same on a repeat", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const model = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      yield* runner.runToolCall({ sessionID, call: model.calls[0]! })

      const first = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })
      // A seal that published Step.Ended and then died is retried. The loop decision has to survive
      // that, or the turn would stop one step early.
      const second = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      expect(second).toEqual(first)
      expect(yield* stepEndedCount).toBe(1)
    }),
  )

  harness(callsTool).effect("closes a call the dispatcher never settled", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const model = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      // The tool activity exhausted its retries and never published a result. Sealing has to close
      // the call anyway: a request carrying a tool_use with no tool_result is rejected outright, so
      // leaving it open would poison every later attempt.
      const result = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      expect(result.continue).toBe(true)
      const part = toolPart(yield* store.context(sessionID), "call_probe")
      expect(part?.type === "tool" ? part.state.status : undefined).toBe("error")
      expect(ran.write).toBe(0)
    }),
  )
})

// Regression: a content-free provider turn must still close. This failed 2 of 4 conformance
// scenarios when the split first ran against them, and the turn hung open forever. The conformance
// suite catches it but needs a dev server and an opt-in env var, so nothing in CI would.
describe("SessionRunner seal of a silent turn", () => {
  harness(silent).effect("closes a turn that published no content of its own", () =>
    Effect.gen(function* () {
      yield* seedSession
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const model = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })
      expect(model.kind).toBe("called")
      if (model.kind !== "called") return
      // The attempt has to hand the seal a message id, because there is nothing in the projection
      // for it to find and it cannot mint one without a publisher.
      expect(model.assistantMessageID).toBeTruthy()

      const result = yield* runner.sealStep({
        sessionID,
        step: model.step,
        settlement: model.settlement,
        assistantMessageID: model.assistantMessageID,
      })

      expect(result.continue).toBe(false)
      const message = assistant(yield* store.context(sessionID))
      expect(message?.type).toBe("assistant")
      expect(message?.type === "assistant" ? Boolean(message.time.completed) : false).toBe(true)
    }),
  )
})

// Compaction restarts the provider attempt by dying with a transition defect that runTurn catches
// and re-enters. That recursion has to carry `deferTools`, or a step that happens to compact
// silently falls back to running its tools inline and the caller never gets the calls it was meant
// to dispatch. A first version of this dropped the flag, so this is a real regression test.
describe("SessionRunner model-only attempt under compaction", () => {
  // The summary request is the one with no tools. It needs text back, while the turn itself needs a
  // tool call, so the mock has to answer them differently.
  const compactingStream: LLMClientShape["stream"] = (request) =>
    request.tools.length === 0
      ? Stream.fromIterable([
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.textStart({ id: "sum" }),
          LLMEvent.textDelta({ id: "sum", text: "## Objective\n- keep going" }),
          LLMEvent.textEnd({ id: "sum" }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        ])
      : callsTool(request)

  const tightModel = SessionRunnerModel.layerWith(() =>
    Effect.succeed(
      OpenAIChat.route
        .with({ endpoint: { baseURL: "https://api.openai.com/v1" }, auth: Auth.bearer("fixture") })
        // Small enough that a seeded turn already overflows the headroom, so the attempt compacts.
        .with({ limits: { context: 4_000, output: 50 } })
        .model({ id: "gpt-4o-mini" }),
    ),
  )
  const compactingConfig = Layer.succeed(
    Config.Service,
    Config.Service.of({
      entries: () =>
        Effect.succeed([
          new Config.Document({
            type: "document",
            info: new Config.Info({
              compaction: new ConfigCompaction.Info({
                buffer: 3_000,
                keep: new ConfigCompaction.Keep({ tokens: 1_000 }),
              }),
            }),
          }),
        ]),
    }),
  )

  const compactHarness = testEffect(
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
        [LayerNodePlatform.llmClient, mockClient(compactingStream)],
        [PermissionV2.node, permission],
        [ToolOutputStore.node, ToolOutputStore.nodeWithoutConfig],
        [SessionRunnerModel.node, tightModel],
        [SystemContextRegistry.node, systemContext],
        [Location.node, Location.boundNode({ directory: AbsolutePath.make("/project") })],
        [SkillGuidance.node, skillGuidance],
        [ReferenceGuidance.node, referenceGuidance],
        [Config.node, compactingConfig],
        [Snapshot.node, Snapshot.noopLayer],
      ],
    ),
  )

  compactHarness.effect("still defers dispatch after a compaction restart", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      // The context epoch has to exist BEFORE the history is seeded. It records the sequence it was
      // created at, and the runner only reads entries after that baseline, so seeding first would
      // put the whole conversation behind the baseline and the request would come out empty.
      const { db } = yield* Database.Service
      yield* SessionContextEpoch.initialize(db, Effect.succeed(SystemContext.empty), sessionID)
      // A finished turn already in the log, sized into a narrow band. It has to exceed the request
      // headroom (context - buffer = 1000 tokens) so the attempt compacts at all, while the summary
      // prompt it produces has to stay under context - summaryOutput (3950) or compaction bails out
      // on its own guard and never restarts the attempt.
      const events = yield* EventV2.Service
      const seeder = createLLMEventPublisher(events, {
        sessionID,
        agent: "build",
        model: { id: ModelV2.ID.make("gpt-4o-mini"), providerID: ProviderV2.ID.make("openai") },
      })
      yield* seeder.publish(LLMEvent.textStart({ id: "old" }))
      yield* seeder.publish(LLMEvent.textDelta({ id: "old", text: "Earlier answer. ".repeat(500) }))
      yield* seeder.publish(LLMEvent.textEnd({ id: "old" }))
      yield* seeder.publish(LLMEvent.stepFinish({ index: 0, reason: "stop" }))

      const runner = yield* SessionRunner.Service
      const result = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })

      // Guard against a vacuous pass: without compaction actually firing this proves nothing about
      // the restart path.
      const rows = yield* db
        .select({ type: EventTable.type })
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, sessionID))
        .all()
        .pipe(Effect.orDie)
      expect(rows.filter((row) => row.type.includes("compaction")).length).toBeGreaterThan(0)

      // The point: whatever the attempt went through, dispatch is still the caller's.
      expect(result.kind).toBe("called")
      if (result.kind !== "called") return
      expect(result.calls.map((call) => call.id)).toEqual(["call_probe"])
      expect(ran.write).toBe(0)
    }),
  )
})

// A turn has to stop when the provider fails. Only the attempt knows that happened: the log shows a
// failed assistant with tool parts, which reads the same as a step that wants to keep going. If the
// seal re-derives the decision instead of being told, a hard provider failure loops on the durable
// path until a step ceiling catches it.
describe("SessionRunner provider failure in a stepped turn", () => {
  harness(callsToolThenFails).effect("ends the turn instead of asking for another step", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const runner = yield* SessionRunner.Service

      const model = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })

      expect(model.kind).toBe("called")
      if (model.kind !== "called") return
      expect(model.needsContinuation).toBe(false)
      // The attempt already failed them on the way out, so dispatching would only re-read settled
      // parts.
      expect(model.calls).toHaveLength(0)

      const result = yield* runner.sealStep({
        sessionID,
        step: model.step,
        settlement: model.settlement,
        assistantMessageID: model.assistantMessageID,
        needsContinuation: model.needsContinuation,
      })

      expect(result.continue).toBe(false)
      expect(ran.write).toBe(0)
    }),
  )
})

// A decline is the user stopping the turn, not one tool failing. If it reaches the dispatcher as an
// ordinary tool error it gets swallowed, the step seals, and the agent carries on past a refusal.
describe("SessionRunner declined permission in a stepped turn", () => {
  harness(callsDecliningTool).effect("halts the turn rather than reporting a failed tool", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const runner = yield* SessionRunner.Service
      const model = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })
      if (model.kind !== "called" || !model.calls[0]) throw new Error("expected a deferred call")

      const exit = yield* runner
        .runToolCall({ sessionID, call: model.calls[0] })
        .pipe(Effect.exit)

      // Named, not inferred: the activity boundary turns this error into a non-retryable halt,
      // where a plain tool failure reads as one bad tool and the turn continues. Raising a bare
      // interrupt instead would leave the boundary to guess the user's decision from the absence
      // of a cancellation.
      expect(Exit.isFailure(exit)).toBe(true)
      if (!Exit.isFailure(exit)) return
      expect(Cause.squash(exit.cause)).toBeInstanceOf(SessionRunDeclinedError)
    }),
  )
})

// A step's writers share one owner token, so the fence does not separate two seal attempts. The
// projector is what holds the line, and step.ended was the one event in that window with no guard.
describe("SessionRunner duplicate seal", () => {
  harness(callsTool).effect("keeps the first close when a late attempt lands", () =>
    Effect.gen(function* () {
      yield* seedSession
      const ran = counters()
      yield* registerProbes(ran)
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      const model = yield* runner.runModelCall({
        sessionID,
        step: 2,
        promotion: undefined,
        first: false,
        force: false,
      })
      if (model.kind !== "called" || !model.calls[0]) throw new Error("expected a deferred call")
      yield* runner.runToolCall({ sessionID, call: model.calls[0] })
      const seal = {
        sessionID,
        step: model.step,
        settlement: model.settlement,
        assistantMessageID: model.assistantMessageID,
        needsContinuation: model.needsContinuation,
      }
      yield* runner.sealStep(seal)
      const closedAt = assistant(yield* store.context(sessionID))
      const first = String(closedAt?.type === "assistant" ? closedAt.time.completed : undefined)

      // A zombie attempt publishing its own Step.Ended under the same token is admitted by the
      // fence, so the projection has to reject it.
      const events = yield* EventV2.Service
      yield* events.publish(SessionEvent.Step.Ended, {
        sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID: SessionMessage.ID.make(model.assistantMessageID!),
        finish: "stop",
        cost: 0,
        tokens: { input: 99, output: 99, reasoning: 0, cache: { read: 0, write: 0 } },
      })

      const after = assistant(yield* store.context(sessionID))
      expect(String(after?.type === "assistant" ? after.time.completed : undefined)).toBe(first)
      expect(after?.type === "assistant" ? after.tokens?.input : undefined).not.toBe(99)
    }),
  )
})
