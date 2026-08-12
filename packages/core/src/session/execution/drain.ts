// The drain bodies shared by every durable executor: the Temporal layer runs them inside
// activities, the in-process micro-driver calls them directly. One implementation, so the turn
// semantics and the error encoding cannot differ between drivers.

import { Cause, Context, Effect, Exit, type LayerMap } from "effect"
import { ApplicationFailure } from "@temporalio/activity"
import type { LocationServiceMap } from "../../location-service-map"
import type { Location } from "../../location"
import type { LocationError, LocationServices } from "../../location-services"
import { SessionRunner } from "../runner"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionRunDeclinedError } from "../error"
import type { SessionInput } from "../input"
import { encodeRunError } from "./run-error-codec"
import type { DrainInput, StepDrainInput, StepDrainResult } from "./temporal-activities"

export interface DrainDeps {
  readonly store: SessionStore.Interface
  readonly locations: LayerMap.LayerMap<Location.Ref, LocationServices, LocationError>
  /** The app context the drain runs in; providing it plus the per-location layer supplies
   * SessionRunner and all of its dependencies. */
  readonly ctx: Context.Context<SessionStore.Service | LocationServiceMap.Service>
}

export const makeDrains = ({ store, locations, ctx }: DrainDeps) => {
  const drain = async (input: DrainInput, signal: AbortSignal): Promise<void> => {
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const session = yield* store.get(SessionSchema.ID.make(input.sessionID))
        if (!session) return
        yield* SessionRunner.Service.use((runner) =>
          runner.run({ sessionID: session.id, force: input.force }),
        ).pipe(Effect.provide(locations.get(session.location)))
      }).pipe(Effect.provide(ctx), Effect.scoped),
      { signal },
    )
    if (Exit.isSuccess(exit)) return
    const cause = exit.cause
    if (Cause.hasInterruptsOnly(cause)) {
      // Two interrupt sources: driver cancellation (the AbortSignal fired -- rethrow its reason so
      // the attempt records Cancelled, not Failed) and an internal halt like a user declining a
      // permission (the signal did NOT fire). The latter must be non-retryable, or the supervisor
      // re-drives a turn the user explicitly stopped.
      if (signal.aborted)
        throw signal.reason instanceof Error ? signal.reason : new Error("session run interrupted")
      const declined = encodeRunError(
        new SessionRunDeclinedError({ sessionID: SessionSchema.ID.make(input.sessionID) }),
      )
      throw ApplicationFailure.create({
        message: "session run halted (user declined)",
        type: "SessionRunDeclined",
        nonRetryable: true,
        details: declined === undefined ? undefined : [declined],
      })
    }
    // A genuine run error is thrown non-retryable so Temporal surfaces it (to resume) rather than
    // retrying; only crashes / task timeouts (never thrown here) go through the retry policy. The
    // error is encoded faithfully in `details` so the caller can reconstruct the exact RunError.
    const squashed = Cause.squash(cause) as { _tag?: string; message?: string }
    const encoded = encodeRunError(squashed)
    throw ApplicationFailure.create({
      message: squashed?.message ?? Cause.pretty(cause),
      type: squashed?._tag ?? "SessionRunError",
      nonRetryable: true,
      details: encoded === undefined ? undefined : [encoded],
    })
  }

  // Per-step drain: run exactly one step of the turn (used by the per-step supervisor). Same
  // context and error encoding as the whole-turn drain; returns the next loop state.
  const stepDrain = async (input: StepDrainInput, signal: AbortSignal): Promise<StepDrainResult> => {
    const exit = await Effect.runPromiseExit(
      Effect.gen(function* () {
        const session = yield* store.get(SessionSchema.ID.make(input.sessionID))
        if (!session) return { ran: false, continue: false, step: input.step, promotion: null }
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
      }).pipe(Effect.provide(ctx), Effect.scoped),
      { signal },
    )
    if (Exit.isSuccess(exit)) return exit.value
    const cause = exit.cause
    if (Cause.hasInterruptsOnly(cause)) {
      // Same split as the whole-turn drain: cancellation rethrows its reason (records Cancelled),
      // an internal user-decline halt is non-retryable.
      if (signal.aborted)
        throw signal.reason instanceof Error ? signal.reason : new Error("session run interrupted")
      const declined = encodeRunError(
        new SessionRunDeclinedError({ sessionID: SessionSchema.ID.make(input.sessionID) }),
      )
      throw ApplicationFailure.create({
        message: "session run halted (user declined)",
        type: "SessionRunDeclined",
        nonRetryable: true,
        details: declined === undefined ? undefined : [declined],
      })
    }
    const squashed = Cause.squash(cause) as { _tag?: string; message?: string }
    const encoded = encodeRunError(squashed)
    throw ApplicationFailure.create({
      message: squashed?.message ?? Cause.pretty(cause),
      type: squashed?._tag ?? "SessionRunError",
      nonRetryable: true,
      details: encoded === undefined ? undefined : [encoded],
    })
  }

  return { drain, stepDrain }
}
