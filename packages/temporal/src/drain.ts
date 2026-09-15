// The drain bodies the Temporal activities run: one whole step, or the three units a stepped turn
// is made of (the provider attempt, one tool call, the seal). Each wraps a SessionRunner call for
// the activity boundary: find the session, claim the event log for the attempt, bring the project
// tree forward, and run under the session's location. Local mode does not use these; it runs whole
// turns through SessionRunner.run on the coordinator. Both modes share SessionRunner and the log.

import { Context, Effect, type LayerMap } from "effect"
import type { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import type { Location } from "@opencode-ai/core/location"
import type { LocationError, LocationServices } from "@opencode-ai/core/location-services"
import { EventV2 } from "@opencode-ai/core/event"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionStore } from "@opencode-ai/core/session/store"
import type { SessionInput } from "@opencode-ai/core/session/input"
import { runAtBoundary } from "./boundary"

// One step of a turn, as any executor drives it. `promotion` is null (not undefined) so it
// serializes cleanly across an executor's process boundary.
export interface StepDrainInput {
  sessionID: string
  step: number
  promotion: string | null
  first: boolean
  force: boolean
  /** The attempt that owns the event log while this drain runs; set by the executor. */
  owner?: string
}

export interface StepDrainResult {
  continue: boolean
  step: number
  promotion: string | null
  /** What this step's provider attempt was billed for, as the attempt itself reported it. The
   * supervisor adds these up for the turn's budget. Absent from a step that made no attempt. */
  spent?: { readonly tokens: number }
  /** What the session has been billed in total, as its own row holds it. Read off the record rather
   * than added up by the supervisor, so it survives everything the supervisor does not: a run that
   * rolled over, a session that went idle and was woken again, a turn some other client ran. */
  session?: { readonly tokens: number }
}

/**
 * What the session has been billed in total, off its own row. Every count the record keeps, because
 * a provider bills for all of them and a bound that drops the cached ones is one a long context
 * walks straight through.
 *
 * Read before the step runs, so it lags by the step now running. A session's bound overshoots by
 * the same step a turn's does, and for the same reason: what a step cost is not known until it is
 * taken.
 */
export const sessionSpend = (session: SessionSchema.Info | undefined) => {
  const t = session?.tokens
  // Nothing rather than zero when the row has no counts: a bound that reads a missing number as
  // nothing spent is one that never stops a session.
  if (!t) return undefined
  return {
    tokens: (t.input ?? 0) + (t.output ?? 0) + (t.reasoning ?? 0) + (t.cache?.read ?? 0) + (t.cache?.write ?? 0),
  }
}

/** The step result as it crosses the activity boundary. A missing session reads as a step with
 * nothing left to do, which is what a resume waiting on it should see. */
export const toStepResult = (
  step: number,
  result?: SessionRunner.StepResult,
  session?: SessionSchema.Info,
): StepDrainResult => {
  if (result === undefined) return { continue: false, step, promotion: null }
  const total = sessionSpend(session)
  return {
    continue: result.continue,
    step: result.step,
    promotion: result.promotion ?? null,
    ...(result.spent ? { spent: result.spent } : {}),
    ...(total ? { session: total } : {}),
  }
}

export interface DrainDeps {
  readonly store: SessionStore.Interface
  readonly locations: LayerMap.LayerMap<Location.Ref, LocationServices, LocationError>
  /** The app context the drain runs in; providing it plus the per-location layer supplies
   * SessionRunner and all of its dependencies. */
  readonly ctx: Context.Context<SessionStore.Service | LocationServiceMap.Service>
  /** Used to claim the event log for the running attempt so a superseded one is fenced. */
  readonly events: EventV2.Interface
  /** Rebuilds a missing project worktree from stored snapshot packs before the run. */
  readonly worktrees: WorktreeMaterializer.Interface
}

export interface InSessionOptions {
  /** Take the event log for this attempt. True for the writer that supersedes the attempt before it;
   * the other units of a stepped turn ride that writer's token. */
  readonly claim?: boolean
  /** The call about to run, so the tree check can tell this step's writers from an earlier step's. */
  readonly current?: { readonly sessionID: string; readonly step: number; readonly callID: string }
  /** Leave the project tree alone. For a seal closing a step away from its host: rebuilding here
   * would put this host on the newest state while the one that ran the tools may still be writing,
   * and nothing the seal does needs the files. */
  readonly withoutTheTree?: boolean
}

/** Run `use` against a session's runner. Undefined when the session is gone, which is not a run
 * error: deleting a session while its workflow is alive is allowed, and a resume waiting on it
 * should resolve rather than reject. */
export type InSession = <A>(
  sessionID: string,
  owner: string | undefined,
  options: InSessionOptions,
  use: (runner: SessionRunner.Interface, session: SessionSchema.Info) => Effect.Effect<A, SessionRunner.RunError>,
) => Effect.Effect<A | undefined, SessionRunner.RunError>

export const makeInSession =
  ({ store, locations, ctx, events, worktrees }: DrainDeps): InSession =>
  (sessionID, owner, options, use) =>
    Effect.gen(function* () {
      const session = yield* store.get(SessionSchema.ID.make(sessionID))
      if (!session) return undefined
      // Take the event log before running so a superseded attempt's later appends are fenced.
      if (options.claim && owner) yield* events.claim(session.id, owner)
      // A worker taking this step on a host without the project tree rebuilds it from snapshot
      // packs, unless a call of an earlier step never came back on this host.
      if (!options.withoutTheTree)
        yield* worktrees.ensure(session.location.directory, options.current ? { current: options.current } : undefined)
      return yield* SessionRunner.Service.use((runner) => use(runner, session)).pipe(
        Effect.provide(locations.get(session.location)),
      )
    }).pipe(Effect.provideService(EventV2.EventOwner, owner), Effect.provide(ctx), Effect.scoped)

export const makeDrains = (deps: DrainDeps) => {
  const inSession = makeInSession(deps)

  // Run exactly one step of the turn (the supervisor loops it); returns the next loop state.
  const stepDrain = async (input: StepDrainInput, signal: AbortSignal): Promise<StepDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, { claim: input.owner !== undefined }, (runner, session) =>
        runner
          .runStep({
            sessionID: session.id,
            step: input.step,
            promotion: (input.promotion ?? undefined) as SessionInput.Delivery | undefined,
            first: input.first,
            force: input.force,
          })
          .pipe(Effect.map((result) => toStepResult(input.step, result, session))),
      ).pipe(Effect.map((result) => result ?? toStepResult(input.step))),
      // A whole step turns a declined permission into an interrupt to halt its own loop, so an
      // interrupt with nothing cancelling it is that refusal and nothing else.
      { declineIsInterrupt: true },
    )

  return { inSession, stepDrain }
}
