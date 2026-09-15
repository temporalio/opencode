// A turn that never stops stepping is a bug in the loop above this one: a model asking for the same
// tool forever, or a step that keeps handing itself back because its host keeps dying. Every one of
// those steps is a model call somebody pays for, and each of them succeeds, so nothing below the
// supervisor can see it. Driven by a fake runtime whose steps always ask for another.
//
// The ceiling is a runaway guard with a fixed number. The budget below it is policy: what an
// operator decided this turn may spend, which is the thing the workflow having the loop is for.
import { it, expect } from "bun:test"
import { makeSupervisor, type SupervisorRuntime } from "../src/supervisor"
import type { StepDrainResult } from "../src/activities"

const MORE: StepDrainResult = { continue: true, step: 1, promotion: null }
const DONE: StepDrainResult = { continue: false, step: 1, promotion: null }

class LoopingRuntime implements SupervisorRuntime {
  steps = 0
  warnings: string[] = []
  // A wake for the first turn and an idle timeout after it, which is how the supervisor gets to run
  // one turn and then return rather than waiting for a signal this test never sends.
  private woken = false
  condition = async (predicate: () => boolean) => {
    if (!this.woken) {
      this.woken = true
      this.wake?.()
      return predicate()
    }
    return false
  }
  private wake: (() => void) | undefined
  setSignalHandler = (name: string, handler: () => void) => {
    if (name === "wake") this.wake = handler
  }
  setUpdateHandler = () => {}
  // Always another step, up to a bound of its own: without one a ceiling that failed to hold would
  // hang this test rather than fail it.
  // As expensive and as slow as the case wants, and never finished unless it says so.
  tokensPerStep = 0
  secondsPerStep = 0
  clock = 0
  now = () => this.clock
  finishAfter = 50
  // What the record says the session has been billed, which the drains read off the session's own
  // row. Unset means a host that cannot say, and then the supervisor's own count is what is used.
  recorded: number | undefined = undefined
  runTurnStep = async () => {
    if (this.realTimePerStep) await new Promise((r) => setTimeout(r, this.realTimePerStep))
    if (this.cancelled) return DONE
    this.clock += this.secondsPerStep * 1000
    const session = this.recorded === undefined ? {} : { session: { tokens: this.recorded } }
    return ++this.steps < this.finishAfter
      ? { ...MORE, spent: { tokens: this.tokensPerStep }, ...session }
      : { ...DONE, ...session }
  }
  // Real time rather than the workflow clock, for the one bound that is a timer.
  realTimePerStep = 0
  runInDrainScope = <A>(fn: () => Promise<A>) => fn()
  // What a cancellation does here: the step loop is a plain loop, so the fake ends it by refusing
  // to take another step, which is what the scope's cancellation does to the real one.
  cancelled = false
  cancelCurrentScope = () => {
    this.cancelled = true
  }
  sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))
  isCancellation = () => false
  isRootCancelled = () => false
  warn = (message: string) => {
    this.warnings.push(message)
  }
}

it("stops driving a turn that keeps asking for another step", async () => {
  const rt = new LoopingRuntime()
  const supervisor = makeSupervisor(rt, { maxStepsPerTurn: 5, idleTimeout: "1 millisecond" })
  await supervisor.sessionTurn("ses_ceiling")

  expect(rt.steps).toBe(5)
  expect(rt.warnings).toEqual(["turn hit the step ceiling and was left where it stopped"])
})

it("stops a turn that has spent the tokens the operator allowed it", async () => {
  const rt = new LoopingRuntime()
  rt.tokensPerStep = 100
  // Three steps at 100 is 300, which is over 250. The bound is read after a step, because what a
  // step spent is not known until it has been taken.
  await makeSupervisor(rt, { budget: { tokens: 250 }, idleTimeout: "1 millisecond" }).sessionTurn("ses_tokens")

  expect(rt.steps).toBe(3)
  expect(rt.warnings).toEqual(["turn stopped where it was: it is out of budget"])
})

it("stops a turn that has run for the time the operator allowed it", async () => {
  const rt = new LoopingRuntime()
  rt.secondsPerStep = 1
  await makeSupervisor(rt, { budget: { seconds: 2 }, idleTimeout: "1 millisecond" }).sessionTurn("ses_seconds")

  expect(rt.steps).toBe(3)
  expect(rt.warnings).toEqual(["turn stopped where it was: it is out of budget"])
})

it("leaves a turn inside its bound alone", async () => {
  const inside = new LoopingRuntime()
  inside.tokensPerStep = 1
  await makeSupervisor(inside, { budget: { tokens: 250 }, idleTimeout: "1 millisecond" }).sessionTurn("ses_inside")
  expect(inside.steps).toBe(50)
  expect(inside.warnings).toEqual([])
})

it("bounds what a whole session spends, not only what one turn does", async () => {
  // A per-turn bound says what one answer may cost. This is the number somebody is billed for, and
  // a session of cheap turns can pass every per-turn bound and still run all night.
  const rt = new LoopingRuntime()
  rt.tokensPerStep = 100
  rt.finishAfter = 2
  const supervisor = makeSupervisor(rt, {
    budget: { tokens: 10_000, sessionTokens: 250 },
    idleTimeout: "1 millisecond",
  })
  await supervisor.sessionTurn("ses_session_budget")

  // Two steps at 100 is inside the turn's own bound and inside the session's, so the turn answers.
  expect(rt.steps).toBe(2)
  expect(rt.warnings).toEqual([])
})

it("carries what a session spent across a rollover", async () => {
  // Without the carry a busy session gets its allowance back every time it outgrows a run's
  // history, which is the one thing a long session is sure to do.
  const rt = new LoopingRuntime()
  rt.tokensPerStep = 100
  await makeSupervisor(rt, {
    budget: { sessionTokens: 250 },
    spent: { tokens: 200, seconds: 0 },
    idleTimeout: "1 millisecond",
  }).sessionTurn("ses_carried")

  // 200 already spent, so the first step's 100 crosses 250 and the second is not scheduled.
  expect(rt.steps).toBe(1)
  expect(rt.warnings).toEqual(["turn stopped where it was: it is out of budget"])
})

it("measures a session against what the record says it spent, not against this run's count", async () => {
  // A session that went idle and was woken again is a fresh run with an empty count of its own.
  // What it has been billed is in the session's own row, and the drains report it.
  const rt = new LoopingRuntime()
  rt.tokensPerStep = 10
  rt.recorded = 400
  await makeSupervisor(rt, { budget: { sessionTokens: 250 }, idleTimeout: "1 millisecond" }).sessionTurn("ses_recorded")

  // One step, whose ten tokens are nowhere near the bound: the record is what crosses it.
  expect(rt.steps).toBe(1)
  expect(rt.warnings).toEqual(["turn stopped where it was: it is out of budget"])
})

it("stops a turn at its deadline, inside the step that was running", async () => {
  // Every other bound is read between two steps, so a step that has started is left to finish. A
  // deadline is the one that fires while one is running, which is what a user pressing stop does.
  const rt = new LoopingRuntime()
  rt.realTimePerStep = 40
  await makeSupervisor(rt, { budget: { hardSeconds: 0.1 }, idleTimeout: "1 millisecond" }).sessionTurn("ses_deadline")

  expect(rt.cancelled).toBe(true)
  expect(rt.steps).toBeLessThan(50)
  expect(rt.warnings).toEqual(["turn stopped where it was: its deadline passed"])
})
