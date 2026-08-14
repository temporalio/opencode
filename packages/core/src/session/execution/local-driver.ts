export * as SessionExecutionLocalDriver from "./local-driver"

// The in-process SessionExecution: local mode as its own product. It is a native async coordinator
// (a per-session task over a mutex, a latch, and an AbortController), NOT the Temporal supervisor
// run through a shim. The two modes share the part where a subtle bug would actually corrupt state
// -- the step body in drain.ts (fencing, error encoding, tool re-drive) -- and nothing else. The
// coordination loop here is written for this runtime: no polled `condition`, no signal/update
// handler maps, no ports, no server; durability comes from the engine's event log.
//
// Parity with the Temporal loop (temporal-workflow.ts + workflow-core.ts) is guaranteed by the
// shared drain and by the driver-contract test (session-execution-local-driver.test.ts), not by a
// single shared loop. See packages/temporal/README.md "Two modes, one drain".

import { Effect, Layer } from "effect"
import { randomUUID } from "node:crypto"
import { LocationServiceMap } from "../../location-service-map"
import { EventV2 } from "../../event"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { makeDrains } from "./drain"
import { WorktreeMaterializer } from "./worktree"
import { toRunError } from "./run-error-codec"

const UNITS: Record<string, number> = {
  ms: 1,
  millisecond: 1,
  milliseconds: 1,
  second: 1_000,
  seconds: 1_000,
  minute: 60_000,
  minutes: 60_000,
  hour: 3_600_000,
  hours: 3_600_000,
}

const parseDuration = (value: string): number => {
  const match = /^\s*([\d.]+)\s*([a-z]+)\s*$/i.exec(value)
  const unit = match?.[2] ? UNITS[match[2].toLowerCase()] : undefined
  if (!match?.[1] || unit === undefined) throw new Error(`Unsupported duration: ${value}`)
  return Number(match[1]) * unit
}

// A drain that is interrupted (by an explicit stop or the backstop) throws this. The drain body
// rethrows the AbortSignal's reason on cancellation, so a cancelled drain and a stop both surface
// the same type, and the loop treats either as a normal retire rather than a failure.
class LocalCancellation extends Error {}

const DEFAULT_IDLE = "5 minutes"
// Matches the Temporal activity's startToClose backstop: a hung tool must not pin a session's
// coordinator open forever. The abort reason is a LocalCancellation, so a timed-out drain looks
// like any other stop.
const BACKSTOP_MS = 12 * 60 * 60 * 1000

type Drains = ReturnType<typeof makeDrains>

// A single-consumer latch. Producers (wake, resume, interrupt) call `open`; the one coordinator
// loop calls `wait`. `open` is sticky: a wake that arrives while the loop is mid-drain is still
// observed on the next `wait`, so no prompt is stranded. This is the direct primitive the polled
// `condition(() => pendingWake)` was standing in for.
class Latch {
  private signalled = false
  private waiter?: (opened: boolean) => void

  get pending() {
    return this.signalled
  }

  open() {
    this.signalled = true
    const waiter = this.waiter
    this.waiter = undefined
    waiter?.(true)
  }

  reset() {
    this.signalled = false
  }

  // Resolve true when opened, false when the idle deadline expires first.
  wait(timeoutMs: number): Promise<boolean> {
    if (this.signalled) return Promise.resolve(true)
    return new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => {
        this.waiter = undefined
        resolve(false)
      }, timeoutMs)
      this.waiter = (opened) => {
        clearTimeout(timer)
        resolve(opened)
      }
    })
  }
}

// Serializes drains: the coordinator loop's drain and a concurrent `resume` never overlap (one
// owner fiber per session at a time, exactly the coordinator's guarantee). `active` counts queued
// AND running work, incremented synchronously on `run`, so the idle check cannot retire a session
// with a resume still waiting for the lock.
class Mutex {
  private tail: Promise<unknown> = Promise.resolve()
  private active = 0

  get idle() {
    return this.active === 0
  }

  run<A>(fn: () => Promise<A>): Promise<A> {
    this.active++
    const result = this.tail.then(fn)
    // Keep the chain alive across a rejected body so the next waiter still runs.
    this.tail = result.then(
      () => {},
      () => {},
    )
    return result.finally(() => {
      this.active--
    })
  }
}

// One coordinator per live session. It owns a task (`done`) that drains work until the session goes
// idle, then retires. `wake` registers work, `resume` forces one drain and awaits its result (so a
// run error reaches the caller), `interrupt` cancels the in-flight drain and retires the task.
class LocalSession {
  readonly done: Promise<void>
  // Set synchronously the instant the loop decides to retire, before the async `finally` runs, so a
  // wake racing the retirement is never accepted onto a dead loop (it starts a fresh coordinator).
  completed = false

  private readonly abort = new AbortController()
  private readonly wake = new Latch()
  private readonly drainLock = new Mutex()
  private stopping = false
  private forcedInFlight = 0
  // One token per coordinator instance. A wake that lands after this one retired starts a fresh
  // coordinator (a new token), so the retired one's late appends are fenced by the event log's
  // owner check -- the local mirror of a Temporal attempt claiming the log.
  private readonly owner = randomUUID()

  constructor(
    private readonly sessionID: SessionSchema.ID,
    private readonly drains: Drains,
    private readonly idleMs: number,
    onDone: () => void,
  ) {
    this.done = this.loop().finally(onDone)
  }

  // Register work. Returns false if this coordinator has already retired, so the caller can start a
  // fresh one instead of stranding the prompt.
  requestWake(): boolean {
    if (this.completed) return false
    this.wake.open()
    return true
  }

  // Force one drain and surface its outcome to the caller (a run error rejects). Mirrors
  // coordinator.run: the caller observes the run's error instead of it being swallowed.
  async resume(): Promise<void> {
    this.forcedInFlight++
    try {
      await this.drainLock.run(() => this.drain(true))
    } finally {
      this.forcedInFlight--
      // Nudge the loop so an idle retire can re-evaluate now that the forced drain has settled.
      this.wake.open()
    }
  }

  // Cancel the in-flight drain and any parked idle wait, and retire.
  interrupt() {
    this.stopping = true
    this.abort.abort(new LocalCancellation("session interrupted"))
    this.wake.open()
  }

  private async loop(): Promise<void> {
    // Constructed in response to a wake, so there is work to drain immediately.
    this.wake.open()
    try {
      for (;;) {
        const gotWork = await this.wake.wait(this.idleMs)
        if (this.stopping) return
        if (!gotWork) {
          // Idle deadline. A wake can race the timer; without this re-check it would be dropped.
          if (this.wake.pending) continue
          // Retire only when nothing is in flight. A later wake/resume starts a fresh coordinator.
          if (this.drainLock.idle && this.forcedInFlight === 0) return
          continue
        }
        this.wake.reset()
        try {
          await this.drainLock.run(() => this.drain(false))
        } catch (error) {
          // A wake-driven drain tolerates run errors (already recorded in the session log); only a
          // stop/cancellation ends the coordinator.
          if (error instanceof LocalCancellation) return
        }
      }
    } finally {
      this.completed = true
    }
  }

  // One turn, driven a step at a time, exactly like the Temporal loop: each step returns the next
  // loop state until it declines to continue. The step body is the shared drain.
  private async drain(force: boolean): Promise<void> {
    let step = 1
    let promotion: string | null = null
    let first = true
    for (;;) {
      const result = await this.withBackstop((signal) =>
        this.drains.stepDrain({ sessionID: this.sessionID, step, promotion, first, force, owner: this.owner }, signal),
      )
      if (!result.continue) break
      step = result.step
      promotion = result.promotion
      first = false
    }
  }

  // The drain shares this coordinator's abort signal so an interrupt cancels it; the timer only adds
  // an upper bound on a drain that hangs while the process stays alive.
  private async withBackstop<A>(run: (signal: AbortSignal) => Promise<A>): Promise<A> {
    const timer = setTimeout(() => this.abort.abort(new LocalCancellation("drain backstop")), BACKSTOP_MS)
    try {
      return await run(this.abort.signal)
    } finally {
      clearTimeout(timer)
    }
  }
}

/**
 * An in-process SessionExecution with no Temporal anywhere:
 *   - wake      -> register work, starting a coordinator if the session has none
 *   - resume    -> force one drain and await it, surfacing the exact RunError
 *   - interrupt -> cancel the in-flight drain and parked waits
 *   - active    -> the sessions with a live coordinator
 */
const layer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    const ctx = yield* Effect.context<SessionStore.Service | LocationServiceMap.Service>()
    const events = yield* EventV2.Service
    const worktrees = yield* WorktreeMaterializer.Service
    const drains = makeDrains({ store, locations, ctx, events, worktrees })
    const sessions = new Map<SessionSchema.ID, LocalSession>()
    // Read at layer build (not module load) so tests can set it before constructing the layer.
    const idleMs = parseDuration(process.env.OPENCODE_SESSION_IDLE_TIMEOUT ?? DEFAULT_IDLE)

    const ensure = (id: SessionSchema.ID): LocalSession => {
      const existing = sessions.get(id)
      if (existing && !existing.completed) return existing
      const session = new LocalSession(id, drains, idleMs, () => {
        if (sessions.get(id) === session) sessions.delete(id)
      })
      sessions.set(id, session)
      // The coordinator records its own failures in the session log; an unhandled rejection here
      // would crash the process instead.
      session.done.catch(() => {})
      return session
    }

    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        for (const session of sessions.values()) session.interrupt()
        await Promise.allSettled([...sessions.values()].map((session) => session.done))
      }),
    )

    yield* Effect.logInfo("SessionExecutionLocalDriver ready").pipe(Effect.annotateLogs({ coordinator: "local" }))

    return SessionExecution.Service.of({
      active: Effect.sync(
        () => new Set([...sessions].filter(([, session]) => !session.completed).map(([id]) => id)),
      ),
      wake: (id) =>
        Effect.sync(() => {
          // `completed` flips synchronously as the loop retires, and `ensure` replaces a retired
          // coordinator, so a second attempt always lands on a live one.
          if (!ensure(id).requestWake()) ensure(id).requestWake()
        }),
      resume: (id) =>
        Effect.tryPromise({
          try: () => ensure(id).resume(),
          catch: (error) => toRunError(id, error),
        }),
      interrupt: (id) =>
        Effect.sync(() => {
          sessions.get(id)?.interrupt()
        }),
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node, EventV2.node, WorktreeMaterializer.node],
})
