// A turn that never stops stepping is a bug in the loop above this one: a model asking for the same
// tool forever, or a step that keeps handing itself back because its host keeps dying. Every one of
// those steps is a model call somebody pays for, and each of them succeeds, so nothing below the
// supervisor can see it. Driven by a fake runtime whose steps always ask for another.
import { it, expect } from "bun:test"
import { makeSupervisor, type SupervisorRuntime } from "../src/supervisor"
import type { StepDrainResult } from "../src/activities"

const MORE: StepDrainResult = { ran: true, continue: true, step: 1, promotion: null }
const DONE: StepDrainResult = { ran: true, continue: false, step: 1, promotion: null }

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
  runTurnStep = async () => (++this.steps < 50 ? MORE : DONE)
  runInDrainScope = <A>(fn: () => Promise<A>) => fn()
  cancelCurrentScope = () => {}
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

it("leaves the ceiling off a run that was recorded before it existed", async () => {
  // The ceiling changes what the supervisor schedules, so a run that predates it would replay into
  // a step this code refuses to take. Those runs keep what they recorded, and their own bound is
  // the one the fake supplies.
  const rt = new LoopingRuntime()
  const supervisor = makeSupervisor(
    { ...rt, boundsStepsPerTurn: () => false, runTurnStep: () => rt.runTurnStep() },
    { maxStepsPerTurn: 5, idleTimeout: "1 millisecond" },
  )
  await supervisor.sessionTurn("ses_unbounded")

  expect(rt.steps).toBe(50)
  expect(rt.warnings).toEqual([])
})
