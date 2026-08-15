// Deterministic unit tests for the Temporal supervisor (execution/workflow-core.ts) driven by a fake
// WorkflowRuntime -- no Temporal, no DB. Covers the coordination redesign: resume joins the single
// in-flight drain (never a second forced drain), a wake that only joins a resume drain still gets a
// follow-up, an interrupt stops the current turn but the supervisor keeps serving, and a fresh
// resume-with-start (startWithWake=false) does exactly one drain (no spurious wake drain).
import { describe, it, expect } from "bun:test"
import { makeWorkflows, type WorkflowRuntime } from "@opencode-ai/core/session/execution/workflow-core"
import type { StepDrainResult } from "@opencode-ai/core/session/execution/temporal-activities"

class FakeCancel extends Error {}
const DONE: StepDrainResult = { ran: true, continue: false, step: 1, promotion: null }
const settle = () => new Promise((r) => setTimeout(r, 0))

// runTurnStep either resolves immediately (ungated) or parks until released (gated), so a turn can
// be held in flight while resumes/wakes/interrupts are delivered. cancelCurrentScope rejects the
// in-flight step with FakeCancel, modelling an interrupt aborting the active drain's scope.
class FakeRuntime implements WorkflowRuntime {
  steps = 0
  gated = false
  private waiters: { predicate: () => boolean; resolve: (b: boolean) => void; isTimeout: boolean }[] = []
  private signals = new Map<string, () => void>()
  private updates = new Map<string, () => Promise<void>>()
  private pending: { resolve: (r: StepDrainResult) => void; reject: (e: unknown) => void }[] = []

  condition = (predicate: () => boolean, timeout?: string) =>
    new Promise<boolean>((resolve) => {
      this.waiters.push({ predicate, resolve, isTimeout: timeout !== undefined })
      this.flush()
    })
  setSignalHandler = (name: "wake" | "interrupt", handler: () => void) => {
    this.signals.set(name, handler)
  }
  setUpdateHandler = (_name: "resume", handler: () => Promise<void>) => this.updates.set("resume", handler)
  runTurnStep = () => {
    this.steps++
    if (!this.gated) return Promise.resolve(DONE)
    return new Promise<StepDrainResult>((resolve, reject) => this.pending.push({ resolve, reject }))
  }
  runInDrainScope = <A>(fn: () => Promise<A>) => fn()
  cancelCurrentScope = () => {
    for (const p of this.pending.splice(0)) p.reject(new FakeCancel())
  }
  isCancellation = (e: unknown) => e instanceof FakeCancel
  rootCancelled = false
  isRootCancelled = () => this.rootCancelled

  private flush() {
    for (const w of [...this.waiters]) {
      if (!w.predicate()) continue
      this.waiters = this.waiters.filter((x) => x !== w)
      w.resolve(true)
    }
  }
  deliver(name: "wake" | "interrupt") {
    this.signals.get(name)?.()
    this.flush()
  }
  resume(): Promise<void> {
    const p = this.updates.get("resume")!()
    this.flush()
    return p
  }
  fireIdle() {
    const expiring = [...this.waiters]
    this.waiters = []
    for (const w of expiring) w.resolve(false)
  }
  releaseStep(result: StepDrainResult = DONE) {
    this.pending.shift()?.resolve(result)
  }
  get pendingSteps() {
    return this.pending.length
  }
}

const start = (rt: FakeRuntime, startWithWake = true) => {
  let ended = false
  makeWorkflows(rt)
    .sessionTurn("ses_test", startWithWake)
    .then(
      () => (ended = true),
      () => (ended = true),
    )
  return { isDone: () => ended }
}

describe("Temporal supervisor (workflow-core)", () => {
  it("concurrent resumes join a single drain (no duplicate forced turns)", async () => {
    const rt = new FakeRuntime()
    rt.gated = true
    start(rt, false) // fresh resume-with-start: no initial wake drain
    const r1 = rt.resume()
    const r2 = rt.resume()
    await settle()
    expect(rt.steps).toBe(1) // joined
    rt.releaseStep(DONE)
    await Promise.all([r1, r2])
    expect(rt.steps).toBe(1)
  })

  it("a fresh resume-with-start does exactly one drain (no spurious wake drain)", async () => {
    const rt = new FakeRuntime()
    rt.gated = true
    start(rt, false)
    const resumed = rt.resume()
    await settle()
    expect(rt.steps).toBe(1)
    rt.releaseStep(DONE)
    await resumed
    await settle()
    expect(rt.steps).toBe(1) // no extra drain manufactured by an initial pendingWake
  })

  it("interrupt stops the current turn but the supervisor keeps serving", async () => {
    const rt = new FakeRuntime()
    rt.gated = true
    const sup = start(rt, true) // initial wake drain
    await settle()
    expect(rt.steps).toBe(1)
    rt.deliver("interrupt") // cancels the in-flight drain
    await settle()
    expect(sup.isDone()).toBe(false) // supervisor did NOT exit
    rt.deliver("wake") // a new prompt after the interrupt
    await settle()
    expect(rt.steps).toBe(2) // drives a fresh turn on the same supervisor
    rt.releaseStep(DONE)
    await settle()
    rt.fireIdle()
    await settle()
    expect(sup.isDone()).toBe(true) // ends only when idle
  })

  it("a root cancellation (not a per-turn interrupt) stops the supervisor", async () => {
    const rt = new FakeRuntime()
    rt.gated = true
    const sup = start(rt, true)
    await settle()
    expect(rt.steps).toBe(1)
    // A real workflow (root) cancellation, not a per-turn interrupt: the root scope is cancelled and
    // it propagates into the drain. The supervisor must end -- not keep serving (the per-turn-
    // interrupt behavior tested above).
    rt.rootCancelled = true
    rt.cancelCurrentScope()
    await settle()
    expect(sup.isDone()).toBe(true)
    rt.deliver("wake") // a wake after root cancellation must NOT drive a new turn
    await settle()
    expect(rt.steps).toBe(1)
  })

  it("a wake that only joins a resume drain still gets its own follow-up", async () => {
    const rt = new FakeRuntime()
    rt.gated = true
    start(rt, false)
    const resumed = rt.resume()
    await settle()
    expect(rt.steps).toBe(1)
    rt.deliver("wake") // arrives while the resume drain is in flight -> can only join
    await settle()
    expect(rt.steps).toBe(1)
    rt.releaseStep(DONE) // resume drain ends -> the joined wake triggers a follow-up
    await resumed
    await settle()
    expect(rt.steps).toBe(2)
    rt.releaseStep(DONE)
    await settle()
    expect(rt.steps).toBe(2)
  })
})
