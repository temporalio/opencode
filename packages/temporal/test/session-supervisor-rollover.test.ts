// #4 regression: continue-as-new must count EVERY drain, including resume-driven ones (counting only
// wake-loop drains let a resume-heavy workflow grow history without rolling over). Plus the fix for
// the rollover-manufactures-a-wake bug: a rollover triggered by a resume drain must carry
// startWithWake=false into the successor, not re-arm a spurious wake. Driven by a fake runtime.
import { describe, it, expect } from "bun:test"
import { makeSupervisor, type SupervisorRuntime } from "../src/supervisor"
import type { StepDrainResult } from "../src/activities"

class ContinuedAsNew extends Error {}
const DONE: StepDrainResult = { ran: true, continue: false, step: 1, promotion: null }
const settle = () => new Promise((r) => setTimeout(r, 0))

class FakeRuntime implements SupervisorRuntime {
  steps = 0
  continued = 0
  continuedWith: boolean | undefined
  private waiters: { predicate: () => boolean; resolve: (b: boolean) => void }[] = []
  private updates = new Map<string, () => Promise<void>>()

  condition = (predicate: () => boolean, _timeout?: string) =>
    new Promise<boolean>((resolve) => {
      this.waiters.push({ predicate, resolve })
      this.flush()
    })
  setSignalHandler = () => {}
  setUpdateHandler = (_name: "resume", handler: () => Promise<void>) => this.updates.set("resume", handler)
  runTurnStep = async () => {
    this.steps++
    return DONE
  }
  runInDrainScope = <A>(fn: () => Promise<A>) => fn()
  cancelCurrentScope = () => {}
  isCancellation = () => false // no interrupts in this test; continueAsNew propagates out
  isRootCancelled = () => false
  continueAsNew = async (_sessionID: string, startWithWake: boolean): Promise<never> => {
    this.continued++
    this.continuedWith = startWithWake
    throw new ContinuedAsNew()
  }

  private flush() {
    for (const w of [...this.waiters]) {
      if (!w.predicate()) continue
      this.waiters = this.waiters.filter((x) => x !== w)
      w.resolve(true)
    }
  }
  async resume(): Promise<void> {
    const p = this.updates.get("resume")!()
    this.flush()
    await p.catch(() => {})
    this.flush()
  }
}

describe("supervisor continue-as-new counting", () => {
  it("counts a resume-driven drain toward the bound and rolls over carrying no spurious wake", async () => {
    const rt = new FakeRuntime()
    // Bound of 2: the initial wake drain is #1; a single resume drain is #2 and must roll over.
    const supervisor = makeSupervisor(rt, { maxDrainsPerRun: 2 }).sessionTurn("ses_rollover")
    let ended = false
    supervisor.then(
      () => (ended = true),
      () => (ended = true),
    )
    await settle() // initial wake drain (#1)
    expect(rt.steps).toBe(1)
    expect(rt.continued).toBe(0)
    await rt.resume() // resume drain (#2) crosses the bound
    await settle()
    expect(rt.steps).toBe(2)
    expect(rt.continued).toBe(1) // continue-as-new fired because the resume drain was counted
    expect(rt.continuedWith).toBe(false) // no wake was pending, so none is carried (no spurious drain)
    expect(ended).toBe(true)
  })
})
