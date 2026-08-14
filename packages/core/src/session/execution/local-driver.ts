export * as SessionExecutionLocalDriver from "./local-driver"

// The in-process driver for the session supervisor (workflow-core.ts). It runs the SAME supervisor
// function the Temporal workflow runs, with the six runtime primitives implemented over plain
// promises: `condition` is a waiter re-checked on every signal and update delivery, the drains run
// directly (no activities), and cancellation is an AbortController whose reason satisfies the
// drain's cancellation contract. No Temporal server, no worker, no ports; durability comes from
// the engine's event log, exactly as in local coordinator mode. This is the "one supervisor, two
// drivers" shape: the factory picks the driver, the supervisor is written once. The primitives
// stay promise-shaped because the supervisor also runs inside a Temporal workflow, which cannot
// carry effect's runtime.

import { Duration, Effect, Layer } from "effect"
import { randomUUID } from "node:crypto"
import { LocationServiceMap } from "../../location-service-map"
import { EventV2 } from "../../event"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { makeDrains } from "./drain"
import { WorktreeMaterializer } from "./worktree"
import { makeWorkflows, type WorkflowRuntime } from "./workflow-core"
import { toRunError } from "./run-error-codec"


class LocalCancellation extends Error {}

const BACKSTOP_MS = 12 * 60 * 60 * 1000

interface Waiter {
  readonly predicate: () => boolean
  readonly resolve: (value: boolean) => void
  readonly reject: (error: unknown) => void
  timer?: ReturnType<typeof setTimeout>
}

type Drains = ReturnType<typeof makeDrains>

// One driver per live session. Temporal re-evaluates workflow conditions on every activation; the
// local equivalents are exactly the events that can change a predicate's inputs: signal delivery,
// an update starting or settling, and a new condition registering. The supervisor is one
// sequential coroutine, so nothing else mutates the state a predicate reads.
class SessionDriver {
  readonly done: Promise<void>
  completed = false
  private readonly signalHandlers = new Map<string, () => void>()
  private readonly updateHandlers = new Map<string, () => Promise<void>>()
  private waiters: Waiter[] = []
  private cancelled = false
  private readonly abort = new AbortController()
  // One token per driver instance. A wake that lands after a driver finished starts a fresh driver
  // (a new token), so the retired one's late appends are fenced, mirroring the Temporal attempt.
  private readonly owner = randomUUID()

  constructor(run: (rt: WorkflowRuntime) => Promise<void>, drains: Drains, onDone: () => void) {
    const rt: WorkflowRuntime = {
      condition: (predicate, timeout) => {
        if (this.cancelled) return Promise.reject(new LocalCancellation())
        const { promise, resolve, reject } = Promise.withResolvers<boolean>()
        const waiter: Waiter = { predicate, resolve, reject }
        if (timeout !== undefined)
          waiter.timer = setTimeout(() => {
            this.remove(waiter)
            resolve(false)
          }, Duration.toMillis(Duration.fromInputUnsafe(timeout as Duration.Input)))
        this.waiters.push(waiter)
        this.tick()
        return promise
      },
      setSignalHandler: (name, handler) => this.signalHandlers.set(name, handler),
      setUpdateHandler: (name, handler) => this.updateHandlers.set(name, handler),
      // Same 12 h backstop as the Temporal activity: a hung tool must not hold `draining` forever.
      // The abort reason is a LocalCancellation, so a timed-out drain looks like any other cancel.
      runTurnStep: (input) =>
        this.withBackstop((signal) => drains.stepDrain({ ...input, owner: this.owner }, signal)),
      cancelCurrentScope: () => this.cancel(),
      isCancellation: (error) => error instanceof LocalCancellation,
    }
    this.done = run(rt).finally(() => {
      this.completed = true
      onDone()
    })
  }

  signal(name: "wake" | "interrupt") {
    this.signalHandlers.get(name)?.()
    this.tick()
  }

  update(name: "resume"): Promise<void> {
    const handler = this.updateHandlers.get(name)
    if (!handler) return Promise.reject(new Error(`Update handler not registered: ${name}`))
    const result = handler()
    // The handler's synchronous prefix may have changed predicate inputs; check again on settle.
    this.tick()
    result.finally(() => this.tick()).catch(() => {})
    return result
  }

  // The drain shares the driver's abort signal so an interrupt still cancels it; the timer only
  // adds an upper bound.
  private async withBackstop<A>(run: (signal: AbortSignal) => Promise<A>): Promise<A> {
    const timer = setTimeout(() => this.abort.abort(new LocalCancellation("drain backstop")), BACKSTOP_MS)
    try {
      return await run(this.abort.signal)
    } finally {
      clearTimeout(timer)
    }
  }

  private cancel() {
    this.cancelled = true
    // The drain rethrows the signal's reason on cancellation, so the supervisor observes the same
    // LocalCancellation from a cancelled drain as from a rejected condition.
    this.abort.abort(new LocalCancellation("session interrupted"))
    for (const waiter of this.waiters.splice(0)) {
      if (waiter.timer) clearTimeout(waiter.timer)
      waiter.reject(new LocalCancellation())
    }
  }

  private remove(waiter: Waiter) {
    this.waiters = this.waiters.filter((entry) => entry !== waiter)
  }

  private tick() {
    for (const waiter of [...this.waiters]) {
      if (!waiter.predicate()) continue
      this.remove(waiter)
      if (waiter.timer) clearTimeout(waiter.timer)
      waiter.resolve(true)
    }
  }
}

/**
 * An in-process SessionExecution running the shared supervisor with no Temporal anywhere:
 *   - wake      -> deliver the wake signal (starting a driver if the session has none)
 *   - resume    -> run the resume update and await it, surfacing the exact RunError
 *   - interrupt -> deliver the interrupt signal (cancels the drain and parked waits)
 *   - active    -> the live drivers
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
    const drivers = new Map<SessionSchema.ID, SessionDriver>()
    // Read at layer build (not module load) so tests can set it before constructing the layer.
    // The idle override shortens the supervisor's 5-minute self-termination.
    const IDLE_TIMEOUT = process.env.OPENCODE_SESSION_IDLE_TIMEOUT

    const ensure = (id: SessionSchema.ID): SessionDriver => {
      const existing = drivers.get(id)
      if (existing && !existing.completed) return existing
      const driver = new SessionDriver(
        (rt) => {
          const workflows = makeWorkflows(rt, IDLE_TIMEOUT ? { idleTimeout: IDLE_TIMEOUT } : undefined)
          return workflows.sessionTurn(id)
        },
        drains,
        () => {
          if (drivers.get(id) === driver) drivers.delete(id)
        },
      )
      drivers.set(id, driver)
      // wake tolerates supervisor failures (they are already recorded in the session log); an
      // unhandled rejection here would crash the process instead.
      driver.done.catch(() => {})
      return driver
    }

    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        for (const driver of drivers.values()) driver.signal("interrupt")
        await Promise.allSettled([...drivers.values()].map((driver) => driver.done))
      }),
    )

    yield* Effect.logInfo("SessionExecutionLocalDriver ready").pipe(
      Effect.annotateLogs({ supervisor: "sessionTurn" }),
    )

    return SessionExecution.Service.of({
      active: Effect.sync(() => new Set(drivers.keys())),
      wake: (id) =>
        Effect.sync(() => {
          // A wake can land between the supervisor's return and its finally; retry onto a fresh
          // driver so the prompt is not stranded until the next wake.
          for (let tries = 0; tries < 3; tries++) {
            const driver = ensure(id)
            driver.signal("wake")
            if (!driver.completed) return
          }
        }),
      resume: (id) =>
        Effect.tryPromise({
          try: () => ensure(id).update("resume"),
          catch: (e) => toRunError(id, e),
        }),
      interrupt: (id) =>
        Effect.sync(() => {
          drivers.get(id)?.signal("interrupt")
        }),
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node, EventV2.node, WorktreeMaterializer.node],
})
