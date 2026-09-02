// Deterministic unit tests for the stepped turn body (src/l2-step.ts) driven by fake activities --
// no Temporal, no DB. This is the piece that turns one step into three units of work, so what is
// pinned here is the orchestration: the owner token reaches every writer, a settled step dispatches
// nothing, a failed tool still lets the step close, and an interrupt is not swallowed.
import { describe, it, expect } from "bun:test"
import { isHaltFailure, makeSteppedTurn, type SteppedActivities } from "../src/l2-step"
import { runAtBoundary } from "../src/boundary"
import { SessionRunDeclinedError } from "@opencode-ai/core/session/error"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { Effect } from "effect"
import {
  ActivityFailure,
  ApplicationFailure,
  CancelledFailure,
  TimeoutFailure,
} from "@temporalio/workflow"
import type { StepDrainInput, StepDrainResult } from "../src/activities"
import type {
  ModelCallDrainResult,
  SealDrainInput,
  ToolCallDrainInput,
  ToolCallDrainResult,
} from "../src/l2-drain"

class FakeCancel extends Error {}
class FakeHalt extends Error {}
const isCancellation = (e: unknown) => e instanceof FakeCancel
const isHalt = (e: unknown) => e instanceof FakeHalt

const INPUT: StepDrainInput = {
  sessionID: "ses_1",
  step: 2,
  promotion: null,
  first: false,
  force: false,
}
const SEALED: StepDrainResult = { ran: true, continue: true, step: 3, promotion: "steer" }
const call = (id: string, name = "probe_write") => ({ id, name, input: {}, assistantMessageID: "msg_1" })

const fakes = (
  model: ModelCallDrainResult,
  onTool: (input: ToolCallDrainInput) => Promise<ToolCallDrainResult> = async () => ({
    outcome: "settled",
  }),
) => {
  const tools: ToolCallDrainInput[] = []
  const seals: SealDrainInput[] = []
  const activities: SteppedActivities = {
    runModelCall: async () => model,
    runToolCall: async (input) => {
      tools.push(input)
      return onTool(input)
    },
    sealStep: async (input) => {
      seals.push(input)
      return SEALED
    },
  }
  return { activities, tools, seals }
}

describe("stepped turn", () => {
  it("dispatches nothing and does not seal when the step is already settled", async () => {
    const settled: StepDrainResult = { ran: false, continue: false, step: 2, promotion: null }
    const { activities, tools, seals } = fakes({ kind: "settled", result: settled })

    const result = await makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    // A crashed step finalized from the log, or the recovery gate finding no work: there is nothing
    // to run and nothing to close.
    expect(result).toEqual(settled)
    expect(tools).toHaveLength(0)
    expect(seals).toHaveLength(0)
  })

  it("runs every call under the attempt's owner and then seals", async () => {
    const { activities, tools, seals } = fakes({
      kind: "called",
      step: 2,
      calls: [call("call_a"), call("call_b")],
      settlement: {
        finish: "tool-calls",
        tokens: { input: 1, output: 2, reasoning: 0, cache: { read: 0, write: 0 } },
      },
      owner: "run-1:model-1:1",
    })

    const result = await makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    expect(tools.map((t) => t.call.id)).toEqual(["call_a", "call_b"])
    // Every writer in the step publishes under the token the attempt claimed. A token minted per
    // activity would make them fence each other out of the log.
    expect(tools.map((t) => t.owner)).toEqual(["run-1:model-1:1", "run-1:model-1:1"])
    expect(seals).toHaveLength(1)
    expect(seals[0]?.owner).toBe("run-1:model-1:1")
    // The finish reason lives only in the attempt's memory, so the seal has to be handed it.
    expect(seals[0]?.settlement?.finish).toBe("tool-calls")
    expect(result).toEqual(SEALED)
  })

  it("reports the calls it did not settle, and says nothing when they all settled", async () => {
    const lines: Array<{ message: string; attributes: Record<string, unknown> }> = []
    const log = (message: string, attributes: Record<string, unknown>) =>
      lines.push({ message, attributes })
    const model: ModelCallDrainResult = {
      kind: "called",
      step: 2,
      calls: [call("call_a"), call("call_b", "probe_read")],
      owner: "own",
    }
    const settling = fakes(model)
    await makeSteppedTurn({ ...settling, isCancellation, isHalt, log })(INPUT)
    // Nothing surprising happened, so nothing is said about it.
    expect(lines).toHaveLength(0)

    const skipping = fakes(model, async (input) => ({
      outcome: input.call.id === "call_a" ? "unknown" : "settled",
    }))
    await makeSteppedTurn({ ...skipping, isCancellation, isHalt, log })(INPUT)

    // Which call was skipped, and why, is the dispatch's own knowledge: the log records what the
    // model was told, and the workflow's history records an activity that succeeded.
    expect(lines).toHaveLength(1)
    expect(lines[0]?.attributes).toEqual({
      step: 2,
      calls: [{ call: "call_a", tool: "probe_write", outcome: "unknown" }],
    })
  })

  it("seals even when a tool call fails outright", async () => {
    const { activities, tools, seals } = fakes(
      { kind: "called", step: 2, calls: [call("call_a"), call("call_b")], owner: "own" },
      async (input) => {
        if (input.call.id === "call_a") throw new Error("activity exhausted its retries")
        return { outcome: "settled" }
      },
    )

    const result = await makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    // One broken tool must not take the turn with it: the seal closes that call as an error and the
    // model gets to react, which beats losing the step.
    expect(tools).toHaveLength(2)
    expect(seals).toHaveLength(1)
    expect(result).toEqual(SEALED)
  })

  // Each tool ships the project tree from the host that ran it, so two on two hosts each publish a
  // tree without the other's work. Serial is what moving files between hosts costs.
  it("runs a step's tools one at a time when told to", async () => {
    let inFlight = 0
    let overlapped = false
    const { activities, tools } = fakes(
      { kind: "called", step: 2, calls: [call("a"), call("b"), call("c")], owner: "own" },
      async () => {
        inFlight += 1
        if (inFlight > 1) overlapped = true
        await new Promise((resolve) => setTimeout(resolve, 5))
        inFlight -= 1
        return { outcome: "settled" }
      },
    )

    await makeSteppedTurn({ activities, isCancellation, isHalt, serial: true })(INPUT)
    expect(tools).toHaveLength(3)
    expect(overlapped).toBe(false)
  })

  // And still overlap when nothing is moving, which is the case the split was measured on.
  it("runs them together when it is not", async () => {
    let inFlight = 0
    let overlapped = false
    const { activities } = fakes(
      { kind: "called", step: 2, calls: [call("a"), call("b"), call("c")], owner: "own" },
      async () => {
        inFlight += 1
        if (inFlight > 1) overlapped = true
        await new Promise((resolve) => setTimeout(resolve, 5))
        inFlight -= 1
        return { outcome: "settled" }
      },
    )

    await makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)
    expect(overlapped).toBe(true)
  })

  it("closes an interrupted step without letting it ask for another", async () => {
    const { activities, seals } = fakes(
      { kind: "called", step: 2, calls: [call("call_a")], owner: "own" },
      async () => {
        throw new FakeCancel("interrupted")
      },
    )

    const run = makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    // A cancellation is not a failed tool, so it still ends the turn. The step is closed on the way
    // out all the same: a stop landing here used to publish no step event at all, and a follower
    // waiting on the turn hung. What the seal must not do is decide the turn keeps going.
    await expect(run).rejects.toBeInstanceOf(FakeCancel)
    expect(seals).toHaveLength(1)
    expect(seals[0]?.needsContinuation).toBe(false)
  })

  it("closes a halted step without carrying on past the refusal", async () => {
    const { activities, seals } = fakes(
      { kind: "called", step: 2, calls: [call("call_a")], owner: "own" },
      async () => {
        throw new FakeHalt("declined")
      },
    )

    const run = makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    // A decline crosses the activity boundary as an ordinary failure, not a cancel. The halt is
    // still what ends the turn, and the seal is told not to continue, which is what once let the
    // agent run on past the user's refusal.
    await expect(run).rejects.toBeInstanceOf(FakeHalt)
    expect(seals).toHaveLength(1)
    expect(seals[0]?.needsContinuation).toBe(false)
  })
})

// The bug this predicate exists for was a mismatch between what `boundary.ts` throws and what the
// dispatcher recognises. Injecting a fake predicate cannot catch that, so match against the real
// failure shapes. The negative cases are the point: a predicate that answered true for everything
// would pass the positive one alone.
describe("halt predicate", () => {
  const wrap = (cause?: Error) =>
    new ActivityFailure("activity failed", "runToolCall", "1", 1 as never, undefined, cause)

  const throwsFrom = <A>(
    body: Effect.Effect<A, unknown, never>,
    options?: { declineIsInterrupt: true },
  ) =>
    runAtBoundary("ses_1", new AbortController().signal, body, options).then(
      () => undefined,
      (error: unknown) => error,
    )

  it("recognises what the activity boundary throws for a named refusal", async () => {
    // Built by running the boundary rather than by hand, so the two sides cannot drift: a dispatch
    // reports a decline as this error, and whatever the boundary makes of it is what a dispatcher
    // has to recognise.
    const thrown = await throwsFrom(
      Effect.fail(new SessionRunDeclinedError({ sessionID: SessionSchema.ID.make("ses_1") })),
    )
    expect(thrown).toBeInstanceOf(ApplicationFailure)
    expect(isHaltFailure(wrap(thrown as Error))).toBe(true)
  })

  it("recognises a whole step's refusal, which arrives as an interrupt", async () => {
    const thrown = await throwsFrom(Effect.interrupt, { declineIsInterrupt: true })
    expect(isHaltFailure(wrap(thrown as Error))).toBe(true)
  })

  it("does not read an unexplained interrupt as a refusal", async () => {
    // The stepped path names its refusals, so an interrupt with nothing cancelling it is the
    // runner stopping for another reason. Calling that a decline would report a user decision
    // nobody made.
    const thrown = await throwsFrom(Effect.interrupt)
    expect(thrown).toBeInstanceOf(ApplicationFailure)
    expect((thrown as ApplicationFailure).type).toBe("SessionRunInterrupted")
    expect(isHaltFailure(wrap(thrown as Error))).toBe(false)
  })

  it("says no to everything else that can come back from an activity", () => {
    expect(isHaltFailure(wrap(new CancelledFailure("cancelled")))).toBe(false)
    expect(isHaltFailure(wrap(new TimeoutFailure("timed out", undefined, 1 as never)))).toBe(false)
    expect(isHaltFailure(wrap(ApplicationFailure.create({ type: "SessionRunError" })))).toBe(false)
    expect(isHaltFailure(wrap())).toBe(false)
    // Unwrapped, so not what a dispatcher ever sees.
    expect(isHaltFailure(ApplicationFailure.create({ type: "SessionRunDeclined" }))).toBe(false)
    expect(isHaltFailure(new Error("plain"))).toBe(false)
  })
})
