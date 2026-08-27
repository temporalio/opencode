// Deterministic unit tests for the stepped turn body (src/l2-step.ts) driven by fake activities --
// no Temporal, no DB. This is the piece that turns one step into three units of work, so what is
// pinned here is the orchestration: the owner token reaches every writer, a settled step dispatches
// nothing, a failed tool still lets the step close, and an interrupt is not swallowed.
import { describe, it, expect } from "bun:test"
import { isHaltFailure, makeSteppedTurn, type SteppedActivities } from "../src/l2-step"
import { runAtBoundary } from "../src/boundary"
import { Effect } from "effect"
import { ActivityFailure, ApplicationFailure, CancelledFailure, TimeoutFailure } from "@temporalio/workflow"
import type { StepDrainInput, StepDrainResult } from "../src/activities"
import type { ModelCallDrainResult, SealDrainInput, ToolCallDrainInput } from "../src/l2-drain"

class FakeCancel extends Error {}
class FakeHalt extends Error {}
const isCancellation = (e: unknown) => e instanceof FakeCancel
const isHalt = (e: unknown) => e instanceof FakeHalt

const INPUT: StepDrainInput = { sessionID: "ses_1", step: 2, promotion: null, first: false, force: false }
const SEALED: StepDrainResult = { ran: true, continue: true, step: 3, promotion: "steer" }
const call = (id: string, name = "probe_write") => ({ id, name, input: {}, assistantMessageID: "msg_1" })

const fakes = (
  model: ModelCallDrainResult,
  onTool: (input: ToolCallDrainInput) => Promise<{ outcome: "settled" }> = async () => ({ outcome: "settled" }),
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
      settlement: { finish: "tool-calls", tokens: { input: 1, output: 2, reasoning: 0, cache: { read: 0, write: 0 } } },
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

  it("lets an interrupt end the turn instead of sealing it", async () => {
    const { activities, seals } = fakes(
      { kind: "called", step: 2, calls: [call("call_a")], owner: "own" },
      async () => {
        throw new FakeCancel("interrupted")
      },
    )

    const run = makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    // A cancellation is not a failed tool. Swallowing it would close a step the user stopped.
    await expect(run).rejects.toBeInstanceOf(FakeCancel)
    expect(seals).toHaveLength(0)
  })

  it("lets a user halt end the turn instead of sealing it", async () => {
    const { activities, seals } = fakes(
      { kind: "called", step: 2, calls: [call("call_a")], owner: "own" },
      async () => {
        throw new FakeHalt("declined")
      },
    )

    const run = makeSteppedTurn({ activities, isCancellation, isHalt })(INPUT)

    // A decline crosses the activity boundary as an ordinary failure, not a cancel, so without a
    // separate test for it the dispatcher would seal the step and the turn would carry on past the
    // user's refusal.
    await expect(run).rejects.toBeInstanceOf(FakeHalt)
    expect(seals).toHaveLength(0)
  })
})

// The bug this predicate exists for was a mismatch between what `boundary.ts` throws and what the
// dispatcher recognises. Injecting a fake predicate cannot catch that, so match against the real
// failure shapes. The negative cases are the point: a predicate that answered true for everything
// would pass the positive one alone.
describe("halt predicate", () => {
  const wrap = (cause?: Error) =>
    new ActivityFailure("activity failed", "runToolCall", "1", 1 as never, undefined, cause)

  it("recognises what the activity boundary throws for a user halt", async () => {
    // Built by running the boundary rather than by hand, so the two sides cannot drift: an interrupt
    // with no abort is how a decline leaves the runner, and whatever that produces is what a
    // dispatcher has to recognise.
    const thrown = await runAtBoundary("ses_1", new AbortController().signal, Effect.interrupt).then(
      () => undefined,
      (error: unknown) => error,
    )
    expect(thrown).toBeInstanceOf(ApplicationFailure)
    expect(isHaltFailure(wrap(thrown as Error))).toBe(true)
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
