// The model-only attempt: runModelCall performs one provider attempt, records the tool calls it
// asked for, and stops. The caller dispatches each call as its own unit of work and seals the step
// afterwards, which is what puts the model-to-tools loop in a durable executor rather than inside a
// single activity. These tests pin the three properties that split depends on:
//   - the call is durable but not started, so its side effect has NOT run
//   - the provider-minted callID and the publisher's assistantMessageID are handed back, never
//     regenerated, so the dispatcher's result can be matched to the recorded call
//   - the step is left open (no Step.Ended), because the tools have not run yet
// The contrast case is the same stream through runStep, which runs the tool, as a whole step does.
import type { LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { Database } from "@opencode-ai/core/database/database"
import { EventTable } from "@opencode-ai/core/event/sql"
import { EventV2 } from "@opencode-ai/core/event"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { Config } from "@opencode-ai/core/config"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { SessionRunnerModel } from "@opencode-ai/core/session/runner/model"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { Tool } from "@opencode-ai/core/tool/tool"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionRunDeclinedError } from "@opencode-ai/core/session/error"
import { ConfigCompaction } from "@opencode-ai/core/config/compaction"
import { SessionContextEpoch } from "@opencode-ai/core/session/context-epoch"
import { createLLMEventPublisher } from "@opencode-ai/core/session/runner/publish-llm-event"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { SystemContext } from "@opencode-ai/core/system-context"
import { eq } from "drizzle-orm"
import * as OpenAIChat from "@opencode-ai/llm/protocols/openai-chat"
import { Auth } from "@opencode-ai/llm/route"
import { describe, expect, spyOn } from "bun:test"
import { Cause, DateTime, Deferred, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect"
import { emptyStep, runnerHarness, seedSession } from "./lib/runner-harness"

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
// The two shapes a provider delivers arguments in, against a tool that actually wants some. The
// hand-off names the call and nothing else, so what the tool receives comes off the log, and both
// shapes have to leave the same thing there. Whole first: no input deltas at all, which is what the
// fragment buffer would otherwise record as an empty input.
const callsEchoWhole: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_echo", input: { text: "hello" } }),
    LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
  ])
// Streamed, in pieces, which is what a provider that emits partial JSON does.
const callsEchoStreamed: LLMClientShape["stream"] = () =>
  Stream.fromIterable([
    LLMEvent.stepStart({ index: 0 }),
    LLMEvent.toolInputStart({ id: "call_probe", name: "probe_echo" }),
    LLMEvent.toolInputDelta({ id: "call_probe", name: "probe_echo", text: '{"text":' }),
    LLMEvent.toolInputDelta({ id: "call_probe", name: "probe_echo", text: '"hello"}' }),
    LLMEvent.toolInputEnd({ id: "call_probe", name: "probe_echo" }),
    LLMEvent.toolCall({ id: "call_probe", name: "probe_echo", input: { text: "hello" } }),
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

// A store that cannot keep what a tool produced. The tool itself still ran by then, which is the
// case a dispatch has to report honestly rather than as a call that never finished.
const failingOutputStore = Layer.mock(ToolOutputStore.Service, {
  bound: () => Effect.fail(new ToolOutputStore.StorageError({ operation: "write", cause: new Error("disk full") })),
})

const sessionID = SessionV2.ID.make("ses_runner_model_call")

// Probes that count their own executions, so "recorded but not run" and "not run twice" are
// checked against the tools themselves rather than only against the projection. The read probes
// declare themselves repeatable; the write probes do not, which is what decides whether a second
// dispatch runs the tool again.
const registerProbes = (ran: { write: number; read: number; echoed?: string }) =>
  Effect.gen(function* () {
    yield* (yield* ApplicationTools.Service).register({
      // The one probe that wants an argument. Every other schema here is an empty struct, which
      // accepts anything, so none of them can tell whether a tool was handed what the model asked
      // for. This one records it.
      probe_echo: Tool.make({
        description: "echo probe",
        input: Schema.Struct({ text: Schema.String }),
        output: Schema.String,
        execute: (args: { readonly text: string }) =>
          Effect.sync(() => {
            ran.echoed = args.text
            return args.text
          }),
      }),
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

const counters = () => ({ write: 0, read: 0 }) as { write: number; read: number; echoed?: string }

// A seeded session with the probes registered, which every case starts from.
const ready = Effect.gen(function* () {
  yield* seedSession(sessionID)
  const ran = counters()
  yield* registerProbes(ran)
  return ran
})

const modelCall = (step = 2) =>
  Effect.flatMap(SessionRunner.Service, (runner) =>
    runner.runModelCall({ sessionID, step, promotion: undefined, first: false, force: false }),
  )

// The attempt that handed back its calls, or a failure: every dispatch and seal case starts here.
const deferStep = Effect.gen(function* () {
  const result = yield* modelCall()
  if (result.kind !== "called") throw new Error("expected a deferred step")
  return result
})
const deferOneCall = Effect.map(deferStep, (result) => {
  if (!result.calls[0]) throw new Error("expected a deferred call")
  return result.calls[0]
})

const toolPart = (messages: ReadonlyArray<SessionMessage.Message>, callID: string) => {
  for (const message of messages) {
    if (message.type !== "assistant") continue
    for (const part of message.content) if (part.type === "tool" && part.id === callID) return part
  }
  return undefined
}

const toolStatus = (messages: ReadonlyArray<SessionMessage.Message>, callID: string) => {
  const part = toolPart(messages, callID)
  return part?.type === "tool" ? part.state.status : undefined
}

const assistant = (messages: ReadonlyArray<SessionMessage.Message>) =>
  messages.find((message) => message.type === "assistant")

// Whether the step's assistant message is closed. Undefined when there is no message at all, so an
// assertion on either value fails rather than passing by accident.
const closed = (messages: ReadonlyArray<SessionMessage.Message>) => {
  const message = assistant(messages)
  return message?.type === "assistant" ? Boolean(message.time.completed) : undefined
}

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

describe("SessionRunner model-only attempt", () => {
  runnerHarness(callsTool).effect("records the tool call, hands it back, and does not run it", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const store = yield* SessionStore.Service

      const result = yield* deferStep

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
      // Durably recorded and not started: whoever dispatches the call publishes Tool.Called, so
      // `running` in the log means a process was about to run the tool. Recording it here instead
      // would make every later dispatch of a crashed step report an unknown outcome for a tool
      // nobody had touched.
      expect(toolStatus(context, "call_probe")).toBe("pending")
      // The step stays open, so no Step.Ended and no completed assistant message.
      expect(closed(context)).toBe(false)
    }),
  )

  runnerHarness(textOnly).effect("leaves a text-only step open with no calls to dispatch", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const store = yield* SessionStore.Service

      const result = yield* deferStep

      expect(result.calls).toHaveLength(0)
      expect(result.settlement?.finish).toBe("stop")
      // Sealing is uniform: even with nothing to dispatch, the step is closed by the seal, not here,
      // so the answer is recorded but its message is still open.
      expect(closed(yield* store.context(sessionID))).toBe(false)
    }),
  )

  runnerHarness(callsTool).effect("still runs the tool and closes the step through runStep", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      yield* runner.runStep({ sessionID, step: 2, promotion: undefined, first: false, force: false })

      // The contrast that makes the split meaningful: the whole-step path dispatches and seals.
      expect(ran.write).toBe(1)
      const context = yield* store.context(sessionID)
      expect(toolStatus(context, "call_probe")).toBe("completed")
      expect(closed(context)).toBe(true)
    }),
  )

  // A step ending is not a turn ending: a steer or a queued prompt continues the same turn through
  // another step, so the one place that decides publishes the turn's own ending.
  runnerHarness(textOnly).effect("says the turn ended, once, when nothing follows it", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const events = yield* EventV2.Service
      const ended = yield* events
        .subscribe(SessionEvent.Turn.Ended)
        .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped)
      const runner = yield* SessionRunner.Service

      yield* runner.runStep({ sessionID, step: 2, promotion: undefined, first: false, force: false })

      const seen = yield* Fiber.join(ended)
      expect(seen.length).toBe(1)
      expect(seen[0]?.data.sessionID).toBe(sessionID)
    }),
  )
})

// Dispatching one recorded call on its own. The policy under test is what happens when a dispatch
// has already started the tool and died, so the side effect may have run and nothing can say:
// repeatable tools run again, the rest are reported unknown so the model decides. Same rule the
// crash-resume path already follows, off the same evidence.
describe("SessionRunner tool dispatch", () => {
  runnerHarness(callsTool).effect("runs a deferred call and records its result", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const result = yield* runner.runToolCall({ sessionID, call })

      expect(result.outcome).toBe("settled")
      expect(ran.write).toBe(1)
      expect(toolStatus(yield* store.context(sessionID), "call_probe")).toBe("completed")
    }),
  )

  // The arguments come off the recorded call, not the hand-off, so a dispatch that reads them
  // wrongly hands the tool something its schema refuses, and the model spends a turn being told its
  // own input was not an object. Every other probe here takes an empty struct, which accepts that
  // silently.
  runnerHarness(callsEchoWhole).effect("hands the tool the arguments the model asked with", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const ran = counters()
      yield* registerProbes(ran)
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service

      const result = yield* runner.runToolCall({ sessionID, call })

      expect(result.outcome).toBe("settled")
      expect(ran.echoed).toBe("hello")
    }),
  )

  // The same call, streamed in pieces instead of delivered whole. Both shapes have to leave the
  // arguments in the log, because the dispatcher cannot tell which one produced the call.
  runnerHarness(callsEchoStreamed).effect("and the same when the provider streamed them", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const ran = counters()
      yield* registerProbes(ran)
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service

      const result = yield* runner.runToolCall({ sessionID, call })

      expect(result.outcome).toBe("settled")
      expect(ran.echoed).toBe("hello")
    }),
  )

  runnerHarness(callsTool).effect("executes one non-idempotent call once under overlapping dispatches", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const ran = counters()
      yield* registerProbes(ran)
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      const read = store.message
      const gate = yield* Deferred.make<void>()
      let readers = 0
      const spy = spyOn(store, "message").mockImplementation((id) =>
        read(id).pipe(
          Effect.tap((value) =>
            Effect.gen(function* () {
              if (readers >= 2) return
              expect(toolPart(value ? [value.message] : [], call.id)?.state.status).toBe("pending")
              readers++
              if (readers === 2) yield* Deferred.succeed(gate, undefined)
              yield* Deferred.await(gate)
            }),
          ),
        ),
      )
      const outcomes = yield* Effect.all(
        [runner.runToolCall({ sessionID, call }), runner.runToolCall({ sessionID, call })].map(Effect.exit),
        { concurrency: "unbounded" },
      ).pipe(Effect.ensuring(Effect.sync(() => spy.mockRestore())))

      expect(readers).toBe(2)
      expect(ran.write).toBe(1)
      expect(outcomes.filter(Exit.isSuccess)).toHaveLength(1)
    }),
  )

  runnerHarness(callsTool).effect("does nothing when the call already has a result", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service

      yield* runner.runToolCall({ sessionID, call })
      // A duplicate dispatch: at-least-once delivery means this happens, and it must not re-run.
      const second = yield* runner.runToolCall({ sessionID, call })

      expect(second.outcome).toBe("already-settled")
      expect(ran.write).toBe(1)
    }),
  )

  runnerHarness(callsCrashingTool).effect(
    "reports a side-effecting call whose dispatch already started as unknown",
    () =>
      Effect.gen(function* () {
        const ran = yield* ready
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
        expect(toolStatus(yield* store.context(sessionID), "call_probe")).toBe("error")
      }),
  )

  runnerHarness(callsTool, { outputStore: failingOutputStore }).effect(
    "tells the model why a dispatched call failed",
    () =>
      Effect.gen(function* () {
        const ran = yield* ready
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
        const failure = part?.type === "tool" && part.state.status === "error" ? part.state.error : undefined
        expect(failure?.message).toContain("disk full")
      }),
  )

  runnerHarness(callsCrashingTool).effect("closes a started call that a stop cut short", () =>
    Effect.gen(function* () {
      yield* ready
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      const recorded = Effect.map(store.context(sessionID), (context) => {
        const part = toolPart(context, "call_probe")
        return part?.type === "tool" ? part.state : undefined
      })

      // Nothing has started this call, so it is left for the next turn's entry check: closing it
      // here would report a tool the model asked for as having been cut short.
      yield* runner.failToolCall({ sessionID, call })
      expect((yield* recorded)?.status).toBe("pending")

      // A dispatch died with the tool in flight, which is what the log shows after a stop lands
      // mid-tool: the call recorded as running, with no result.
      yield* runner.runToolCall({ sessionID, call }).pipe(Effect.exit)
      expect((yield* recorded)?.status).toBe("running")

      yield* runner.failToolCall({ sessionID, call })
      const closedCall = yield* recorded
      expect(closedCall?.status).toBe("error")
      const reason = closedCall?.status === "error" ? closedCall.error.message : ""
      expect(reason).toBe("Tool execution interrupted")

      // A call that is already terminal keeps what it has, whatever lands afterwards.
      yield* runner.failToolCall({ sessionID, call })
      expect(yield* recorded).toEqual(closedCall)
    }),
  )

  runnerHarness(callsCrashingIdempotentTool).effect("re-runs a started call that declares itself repeatable", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      yield* runner.runToolCall({ sessionID, call }).pipe(Effect.exit)
      const result = yield* runner.runToolCall({ sessionID, call })

      expect(result.outcome).toBe("settled")
      expect(ran.read).toBe(2)
      expect(toolStatus(yield* store.context(sessionID), "call_probe")).toBe("completed")
    }),
  )
})

// Closing the step after its calls have been dispatched. This is the piece that cannot stay in the
// provider attempt: the end snapshot and the file diff have to be taken after the tools have run,
// and in a durable executor that is a different process.
describe("SessionRunner step seal", () => {
  runnerHarness(callsTool).effect("closes a dispatched step and keeps the turn going", () =>
    Effect.gen(function* () {
      yield* ready
      const model = yield* deferStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      yield* runner.runToolCall({ sessionID, call: model.calls[0]! })

      const result = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      // Local tool calls mean the model has results to look at, so the turn continues.
      expect(result.continue).toBe(true)
      expect(result.step).toBe(3)
      expect(closed(yield* store.context(sessionID))).toBe(true)
      expect(yield* stepEndedCount).toBe(1)
    }),
  )

  runnerHarness(textOnly).effect("closes a text-only step and ends the turn", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const model = yield* deferStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const result = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      expect(result.continue).toBe(false)
      expect(closed(yield* store.context(sessionID))).toBe(true)
    }),
  )

  runnerHarness(callsTool).effect("seals once and answers the same on a repeat", () =>
    Effect.gen(function* () {
      yield* ready
      const model = yield* deferStep
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

  runnerHarness(callsTool).effect("closes a call the dispatcher never settled", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const model = yield* deferStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      // The tool activity exhausted its retries and never published a result. Sealing has to close
      // the call anyway: a request carrying a tool_use with no tool_result is rejected outright, so
      // leaving it open would poison every later attempt.
      const result = yield* runner.sealStep({ sessionID, step: 2, settlement: model.settlement })

      expect(result.continue).toBe(true)
      expect(toolStatus(yield* store.context(sessionID), "call_probe")).toBe("error")
      expect(ran.write).toBe(0)
    }),
  )
})

// A content-free provider turn must still close. The publisher mints the assistant message lazily
// on first content, so after such a stream there is no message in the log for a seal to find. The
// whole-step path survives it because Step.Ended mints one on the way past; a seal running in
// another process has no publisher to mint with, so the attempt hands it the id.
describe("SessionRunner seal of a silent turn", () => {
  runnerHarness(emptyStep).effect("closes a turn that published no content of its own", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service

      const model = yield* deferStep
      expect(model.assistantMessageID).toBeTruthy()

      const result = yield* runner.sealStep({
        sessionID,
        step: model.step,
        settlement: model.settlement,
        assistantMessageID: model.assistantMessageID,
      })

      expect(result.continue).toBe(false)
      expect(closed(yield* store.context(sessionID))).toBe(true)
    }),
  )
})

// Compaction restarts the provider attempt by dying with a transition defect that runTurn catches
// and re-enters. That recursion has to carry `deferTools`, or a step that happens to compact
// silently falls back to running its tools inline and the caller never gets the calls it was meant
// to dispatch.
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

  runnerHarness(compactingStream, { model: tightModel, config: compactingConfig }).effect(
    "still defers dispatch after a compaction restart",
    () =>
      Effect.gen(function* () {
        const ran = yield* ready
        // The context epoch has to exist BEFORE the history is seeded. It records the sequence it
        // was created at, and the runner only reads entries after that baseline, so seeding first
        // would put the whole conversation behind the baseline and the request would come out empty.
        const { db } = yield* Database.Service
        yield* SessionContextEpoch.initialize(db, Effect.succeed(SystemContext.empty), sessionID)
        // A finished turn already in the log, sized into a narrow band. It has to exceed the request
        // headroom (context - buffer = 1000 tokens) so the attempt compacts at all, while the
        // summary prompt it produces has to stay under context - summaryOutput (3950) or compaction
        // bails out on its own guard and never restarts the attempt.
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

        const result = yield* modelCall()

        // Guard against a vacuous pass: without compaction actually firing this proves nothing
        // about the restart path.
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
  runnerHarness(callsToolThenFails).effect("ends the turn instead of asking for another step", () =>
    Effect.gen(function* () {
      const ran = yield* ready
      const runner = yield* SessionRunner.Service

      const model = yield* deferStep
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
  runnerHarness(callsDecliningTool).effect("halts the turn rather than reporting a failed tool", () =>
    Effect.gen(function* () {
      yield* ready
      const call = yield* deferOneCall
      const runner = yield* SessionRunner.Service

      const exit = yield* runner.runToolCall({ sessionID, call }).pipe(Effect.exit)

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
  runnerHarness(callsTool).effect("keeps the first close when a late attempt lands", () =>
    Effect.gen(function* () {
      yield* ready
      const model = yield* deferStep
      const runner = yield* SessionRunner.Service
      const store = yield* SessionStore.Service
      yield* runner.runToolCall({ sessionID, call: model.calls[0]! })
      yield* runner.sealStep({
        sessionID,
        step: model.step,
        settlement: model.settlement,
        assistantMessageID: model.assistantMessageID,
        needsContinuation: model.needsContinuation,
      })
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
