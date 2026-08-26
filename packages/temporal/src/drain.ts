// The per-step drain body for the Temporal layer: it runs inside the runTurnStep activity. It wraps
// one SessionRunner.runStep, claims the event log for the attempt, ensures the worktree, and encodes
// the error for the activity boundary. Local mode does not use this: it runs whole turns through
// SessionRunner.run on the SessionRunCoordinator (execution/local.ts). Both modes go through the
// same SessionRunner and the same durable event log.

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
  ran: boolean
  continue: boolean
  step: number
  promotion: string | null
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

export const makeDrains = ({ store, locations, ctx, events, worktrees }: DrainDeps) => {
  // Run exactly one step of the turn (the supervisor loops it); returns the next loop state.
  const stepDrain = async (input: StepDrainInput, signal: AbortSignal): Promise<StepDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      Effect.gen(function* () {
        const session = yield* store.get(SessionSchema.ID.make(input.sessionID))
        if (!session) return { ran: false, continue: false, step: input.step, promotion: null }
        // Take the event log before running so a superseded attempt's later appends are fenced.
        if (input.owner) yield* events.claim(session.id, input.owner)
        // A worker resuming on a host without the project tree rebuilds it from snapshot packs.
        yield* worktrees.ensure(session.location.directory)
        const r = yield* SessionRunner.Service.use((runner) =>
          runner.runStep({
            sessionID: session.id,
            step: input.step,
            promotion: (input.promotion ?? undefined) as SessionInput.Delivery | undefined,
            first: input.first,
            force: input.force,
          }),
        ).pipe(Effect.provide(locations.get(session.location)))
        return { ran: r.ran, continue: r.continue, step: r.step, promotion: r.promotion ?? null }
      }).pipe(Effect.provideService(EventV2.EventOwner, input.owner), Effect.provide(ctx), Effect.scoped),
    )

  return { stepDrain }
}
