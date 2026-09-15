// The Temporal driver's per-session supervisor, expressed over a runtime interface so the SDK's
// condition/signals/updates/activities/cancellation plug in (workflow.ts) and it stays
// unit-testable off a live cluster (a fake runtime). Local mode does NOT run this loop: it uses the
// proven SessionRunCoordinator directly (execution/local.ts). The two modes share SessionRunner and
// the durable event log, not this supervisor.
//
// MUST stay pure: the Temporal driver bundles this into the workflow sandbox, so no `effect`, no
// `@opencode-ai/core` runtime imports, no Node builtins. Type-only imports are erased and safe.
//
// Semantics mirror SessionRunCoordinator (run-coordinator.ts): at most one drain runs at a time; a
// `wake` drives a drain and tolerates errors; `resume` JOINS the active drain (or forces one when
// idle) and surfaces its result; `interrupt` stops the CURRENT turn (not the session) and the
// long-lived supervisor keeps serving later wakes/resumes, terminating only after an idle period.

import type { StepDrainInput, StepDrainResult } from "./activities"

/** What a driver must provide; everything else is supervisor logic. */
export interface SupervisorRuntime {
  /** Wait until the predicate is true. With a timeout, resolve false when it expires first. */
  readonly condition: (predicate: () => boolean, timeout?: string) => Promise<boolean>
  readonly setSignalHandler: (name: "wake" | "interrupt", handler: () => void) => void
  readonly setUpdateHandler: (name: "resume", handler: () => Promise<void>) => void
  /** One step of a turn (SessionRunner.runStep). The Temporal driver runs it as an activity. */
  readonly runTurnStep: (input: StepDrainInput) => Promise<StepDrainResult>
  /**
   * Run one whole turn's drain inside a fresh cancellable scope. `cancelCurrentScope()` cancels the
   * scope of the drain currently running, which aborts its in-flight step; the supervisor stays
   * alive. Scoping the cancellation to the turn (not the workflow) is what lets an interrupt stop
   * the current turn without killing the session.
   */
  readonly runInDrainScope: <A>(fn: () => Promise<A>) => Promise<A>
  /** Cancel the drain scope currently running (interrupt the active turn). No-op when idle. */
  readonly cancelCurrentScope: () => void
  /** Whether an error is the driver's cancellation (a normal stop, not a failure). */
  readonly isCancellation: (error: unknown) => boolean
  /** Whether the WHOLE run (root scope) is cancelled -- a real workflow cancellation, as opposed to
   * a per-turn interrupt (which cancels only the current drain's child scope). A root cancellation
   * must stop the supervisor; it must never keep serving or continue-as-new. Reliable because the
   * timed wait no longer cancels any scope (so it cannot contaminate the root). */
  readonly isRootCancelled: () => boolean
  /** Whether every signal/update handler has fully finished (Temporal's own accounting). Gates
   * completion and continue-as-new so an in-flight update's result is never abandoned. Optional:
   * drivers without a handler protocol return true. */
  readonly allHandlersFinished?: () => boolean
  /** Restart the run with fresh history, carrying whether work is still pending. History-keeping
   * drivers only (Temporal). */
  readonly continueAsNew?: (
    sessionID: string,
    startWithWake: boolean,
    /** What the session has spent so far. Carried, or a session gets its allowance back every time
     * it outgrows a run's history. */
    spent?: Spent,
  ) => Promise<never>
  /** Whether the driver says this run's history is large enough to roll over. A drain count cannot
   * answer this: one drain is a whole turn, and a stepped turn of 200 steps is thousands of events,
   * so a handful of drains can cross the server's limit long before the count does. Optional:
   * drivers without a history return false. */
  readonly historyWantsRollover?: () => boolean
  /** The workflow's own clock, so a turn's elapsed time reads the same on a replay. Optional:
   * a driver without one is not asked for a wall-clock bound. */
  readonly now?: () => number
  /** Wait, in the driver's own time. Only used for a turn's deadline, which is the one bound that
   * has to fire while a step is running rather than between two of them. */
  readonly sleep?: (ms: number) => Promise<void>
  /** Where a turn says it stopped because it ran out of steps rather than because it finished. */
  readonly warn?: (message: string, attributes: Record<string, unknown>) => void
}

export interface WorkflowOptions {
  /** How long to stay alive with no work before self-terminating. */
  readonly idleTimeout?: string
  /** Drains per run before continue-as-new, when the driver supports it. */
  readonly maxDrainsPerRun?: number
  /** Steps one turn may take before the supervisor stops driving it. */
  readonly maxStepsPerTurn?: number
  /**
   * What one turn may spend before the supervisor stops driving it. Enforced between steps, never
   * inside one: a step that has started is left to finish, because stopping it would leave a tool
   * call the next attempt's transcript cannot be made from.
   *
   * This is what the workflow having the loop buys. The agent is not asked to keep to it and cannot
   * be: the model decides what to ask for, so the thing that says no has to be somewhere the model
   * does not reach. Off unless an operator sets it, because a bound that ends real work is worse
   * than none and only the operator knows which is which.
   */
  readonly budget?: {
    // Tokens the turn's provider attempts may spend, added up as each one reports.
    readonly tokens?: number
    // Wall clock for the whole turn, in seconds. A number rather than a duration string: the parser
    // for those is not something a workflow bundle should be reaching for, and a bound nobody can
    // be sure of the units of is worse than one that says them.
    readonly seconds?: number
    // Wall clock again, but as a deadline rather than a bound checked between steps. At `seconds`
    // the turn stops where it can; at this one it stops where it is, which is what a user pressing
    // stop does: what has results keeps them, what is in flight comes back as an outcome nobody can
    // vouch for, and the tools over there keep running until they are done. Opt-in on top.
    readonly hardSeconds?: number
    // And the same two for the session, across every turn it runs. A per-turn bound says what one
    // answer may cost; this says what the whole session may, which is the number somebody is
    // billed for.
    readonly sessionTokens?: number
    readonly sessionSeconds?: number
  }
  /** What the session had spent when this run started, from the run that rolled over. */
  readonly spent?: Spent
}

/** What a session has spent so far, carried between runs of its workflow. */
export interface Spent {
  readonly tokens: number
  // Wall clock its turns have used, rather than how long the session has existed: one sitting idle
  // overnight has spent nothing.
  readonly seconds: number
}

export const makeSupervisor = (rt: SupervisorRuntime, options?: WorkflowOptions) => {
  const IDLE_TIMEOUT = options?.idleTimeout ?? "5 minutes"
  // A continuously busy session never hits the idle return, so without a bound its history grows
  // until Temporal terminates the workflow. continue-as-new carries the pending-wake state, so no
  // queued work is lost across the boundary.
  const MAX_DRAINS_PER_RUN = options?.maxDrainsPerRun ?? 30
  // A turn that never stops stepping is a bug in the loop above this one: a model asking for the
  // same tool forever, or a step that keeps handing itself back because its host keeps dying. Only
  // the supervisor can see it, because each step is its own activity and each one succeeds. High
  // enough that real work never reaches it.
  const MAX_STEPS_PER_TURN = options?.maxStepsPerTurn ?? 200
  const BUDGET = options?.budget
  // What this session has spent, across every turn it has run, carried in from the run that rolled
  // over. A session does not get its allowance back by outgrowing a run's history.
  const spent = { tokens: options?.spent?.tokens ?? 0, seconds: options?.spent?.seconds ?? 0 }
  const outOfBudget = (turnTokens: number, turnSeconds: number, recorded?: number) =>
    BUDGET !== undefined &&
    ((BUDGET.tokens !== undefined && turnTokens > BUDGET.tokens) ||
      (BUDGET.seconds !== undefined && turnSeconds > BUDGET.seconds) ||
      (BUDGET.sessionTokens !== undefined && (recorded ?? spent.tokens + turnTokens) > BUDGET.sessionTokens) ||
      (BUDGET.sessionSeconds !== undefined && spent.seconds + turnSeconds > BUDGET.sessionSeconds))

  // Each step (one provider attempt + its tools) is its own activity; the step loop is supervisor
  // control flow (step / promotion / first mirror SessionRunner.run's loop). `startWithWake` is the
  // explicit start intent: a wake-with-start begins with pending work, a resume-with-start does not
  // (its forced drain comes from the resume update), so resume must not manufacture a spurious wake.
  async function sessionTurn(sessionID: string, startWithWake: boolean = true): Promise<void> {
    let pendingWake = startWithWake
    let drains = 0
    let rolloverPending = false
    let resumers = 0 // in-flight resume handlers awaiting a drain
    // The single in-flight drain, or null when idle. New callers JOIN it rather than starting a
    // second, mirroring SessionRunCoordinator.run: one execution per session at a time, and a resume
    // attaches to the running one instead of queueing a redundant forced drain.
    let inFlight: Promise<void> | null = null

    const drive = (force: boolean): Promise<void> => {
      if (inFlight) return inFlight
      const running = rt
        .runInDrainScope(async () => {
          drains++
          if (drains >= MAX_DRAINS_PER_RUN) rolloverPending = true
          if (rt.historyWantsRollover?.()) rolloverPending = true
          let step = 1
          let promotion: string | null = null
          let first = true
          // A deadline that ends the turn where it is. The wait lives in this drain's own scope, so
          // a turn that finishes first cancels it on the way out, and the rejection that follows is
          // the scope's own.
          if (BUDGET?.hardSeconds !== undefined && rt.sleep) {
            void rt.sleep(BUDGET.hardSeconds * 1000).then(
              () => {
                rt.warn?.("turn stopped where it was: its deadline passed", {
                  sessionID,
                  seconds: BUDGET.hardSeconds,
                })
                rt.cancelCurrentScope()
              },
              () => undefined,
            )
          }
          // What this turn has spent, and when it started spending it. Both are the supervisor's
          // own state: the session's log holds what every turn spent, and this bound is about one.
          const startedAt = rt.now?.() ?? 0
          let tokens = 0
          // What the record says the session has been billed, as of the last step that reported it.
          // The count below is about a run; this one is about the session, and it is what a
          // session's bound is measured against wherever the host can say it.
          let recorded: { readonly tokens: number } | undefined
          const turnSeconds = () => (rt.now ? (rt.now() - startedAt) / 1000 : 0)
          for (let taken = 0; ; taken++) {
            if (taken >= MAX_STEPS_PER_TURN) {
              rt.warn?.("turn hit the step ceiling and was left where it stopped", {
                sessionID,
                steps: taken,
              })
              break
            }
            // Between steps, never inside one, and after the step whose cost crossed the bound:
            // what a step spent is not known until it has been taken.
            if (outOfBudget(tokens, turnSeconds(), recorded?.tokens)) {
              rt.warn?.("turn stopped where it was: it is out of budget", {
                sessionID,
                steps: taken,
                turn: { tokens, seconds: Math.round(turnSeconds()) },
                session: {
                  tokens: recorded?.tokens ?? spent.tokens + tokens,
                  seconds: Math.round(spent.seconds + turnSeconds()),
                  recorded: recorded !== undefined,
                },
                budget: BUDGET,
              })
              break
            }
            const r: StepDrainResult = await rt.runTurnStep({ sessionID, step, promotion, first, force })
            tokens += r.spent?.tokens ?? 0
            recorded = r.session ?? recorded
            // Inside the loop as well, because one drain is a whole turn: a long one outgrows the
            // history without ever reaching the next drain's check.
            if (rt.historyWantsRollover?.()) rolloverPending = true
            if (!r.continue) break
            // A queued prompt continues this same drain as a fresh turn, so a session fed without a
            // gap never goes quiet and the rollover it is waiting for never happens. Stop at that
            // boundary instead and let the new run pick the queue up: the work is not lost, it is
            // one turn later. A steer is not a boundary, so it still rides this drain through.
            if (rolloverPending && r.promotion === "queue") {
              pendingWake = true
              break
            }
            step = r.step
            promotion = r.promotion
            first = false
          }
          // Whatever ended the turn, the provider billed for what it ran.
          spent.tokens += tokens
          spent.seconds += turnSeconds()
        })
        .finally(() => {
          inFlight = null
        })
      inFlight = running
      return running
    }

    rt.setSignalHandler("wake", () => {
      pendingWake = true
    })
    // interrupt stops the CURRENT turn, not the session: cancel the active drain's child scope. The
    // supervisor keeps serving, so a later wake/resume drives a fresh turn on this same long-lived
    // workflow and a prompt that races the interrupt is never stranded. A per-turn interrupt leaves
    // the root scope untouched, so isRootCancelled() distinguishes it from a real cancellation.
    rt.setSignalHandler("interrupt", () => {
      rt.cancelCurrentScope()
    })
    // resume = coordinator.run: join the active drain (or force one when idle) and surface its
    // result. A run error, or an interruption of the joined drain, rejects the update so the caller
    // observes it.
    rt.setUpdateHandler("resume", async () => {
      resumers++
      try {
        await drive(true)
      } finally {
        resumers--
      }
    })

    // "Idle" for completion/continue-as-new means: no drain in flight, no resume handler in our
    // accounting, AND Temporal reports every handler finished (so an update's result is never
    // abandoned by completing/continuing before the protocol records it).
    const quiescent = () => !inFlight && resumers === 0 && (rt.allHandlersFinished?.() ?? true)

    try {
      for (;;) {
        // Wake on a real wake, or on an ACTIONABLE rollover (bound crossed and quiescent). Gating
        // the rollover on quiescence keeps the loop from spinning while a resume drain is still in
        // flight, and -- crucially -- keeps a rollover from being mis-handled as a wake below.
        const woke = await rt.condition(() => pendingWake || (rolloverPending && quiescent()), IDLE_TIMEOUT)
        // A real root cancellation must stop the supervisor -- never keep serving or continue-as-new.
        // (Checked here too so the rollover short-circuit path can't continue-as-new a cancelled run.)
        if (rt.isRootCancelled()) return
        // continue-as-new: only from the main method, only when quiescent. Carry the pending wake so
        // queued work survives the boundary (a pure rollover carries false -> no spurious drain).
        if (rt.continueAsNew && rolloverPending && quiescent()) {
          await rt.continueAsNew(sessionID, pendingWake, spent)
        }
        if (pendingWake) {
          pendingWake = false
          // If a resume already started the drain we take, we only JOIN it -- and it may be past the
          // point where it could pick up the work this wake signals. Re-arm pendingWake for one
          // follow-up drain; a fresh drain we start ourselves already covers the wake.
          const joined = inFlight !== null
          try {
            await drive(false)
          } catch (e) {
            // A real root cancellation wins: propagate it to end the workflow. A per-turn interrupt
            // or a run error (already recorded in the log) is tolerated and the supervisor keeps
            // serving.
            if (rt.isRootCancelled()) throw e
          }
          if (joined) pendingWake = true
          continue
        }
        // No real wake. Retire only on a genuine idle timeout with nothing in flight; a rollover
        // wakeup was either handled above (continue-as-new) or is waiting for the drain to finish.
        if (!woke && quiescent()) return
      }
    } catch (e) {
      // A real root cancellation (workflow cancelled) or a defect reaches here. End the workflow: a
      // cancellation completes the run cleanly, a defect fails it.
      if (rt.isCancellation(e)) return
      throw e
    }
  }

  return { sessionTurn }
}
