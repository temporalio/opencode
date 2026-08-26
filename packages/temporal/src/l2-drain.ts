// The three drain bodies a stepped turn is made of: the provider attempt, one tool call, and the
// seal. L1's drain runs a whole step (one attempt plus all of its tools) in a single activity, so
// nothing can sit between the model asking for a tool and the tool running. Splitting them is what
// gives each tool call its own retry policy, timeout and approval window.
//
// All three write to the same session log, so they publish under ONE owner token: the model call
// claims it and hands it back, the other two inherit it. Minting a token per activity execution (as
// L1 does, correctly, when the activity is the whole step) would make a step's writers fence each
// other out.

import { Context, Effect, type LayerMap } from "effect"
import type { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import type { Location } from "@opencode-ai/core/location"
import type { LocationError, LocationServices } from "@opencode-ai/core/location-services"
import { EventV2 } from "@opencode-ai/core/event"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import type { DeferredToolCall, ToolCallOutcome } from "@opencode-ai/core/session/runner"
import type { StepSettlement } from "@opencode-ai/core/session/runner/publish-llm-event"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionStore } from "@opencode-ai/core/session/store"
import type { SessionInput } from "@opencode-ai/core/session/input"
import { runAtBoundary } from "./boundary"
import type { StepDrainInput, StepDrainResult } from "./drain"

/** The provider attempt of one step. Same shape as a whole-step drain: the difference is what it
 * does with the tool calls, not what it needs to start. */
export type ModelCallDrainInput = StepDrainInput

export type ModelCallDrainResult =
  | { readonly kind: "settled"; readonly result: StepDrainResult }
  | {
      readonly kind: "called"
      readonly step: number
      readonly calls: ReadonlyArray<DeferredToolCall>
      readonly settlement?: StepSettlement
      /** The event-log token this attempt claimed. The tool and seal activities of this step must
       * publish under it, so it travels with the calls instead of being minted again. */
      readonly owner: string
    }

export interface ToolCallDrainInput {
  readonly sessionID: string
  readonly call: DeferredToolCall
  readonly owner: string
  /** Set by the executor from the activity attempt, not by the workflow: only the dispatcher knows
   * whether this call already had a run whose result never landed. */
  readonly retry?: boolean
}

export interface ToolCallDrainResult {
  readonly outcome: ToolCallOutcome
}

export interface SealDrainInput {
  readonly sessionID: string
  readonly step: number
  readonly settlement?: StepSettlement
  readonly owner: string
}

export interface L2DrainDeps {
  readonly store: SessionStore.Interface
  readonly locations: LayerMap.LayerMap<Location.Ref, LocationServices, LocationError>
  readonly ctx: Context.Context<SessionStore.Service | LocationServiceMap.Service>
  readonly events: EventV2.Interface
  readonly worktrees: WorktreeMaterializer.Interface
}

export const makeL2Drains = ({ store, locations, ctx, events, worktrees }: L2DrainDeps) => {
  // One session, one owner, a present project tree. `claim` is true only for the model call: it is
  // the writer that supersedes a previous attempt, and the rest of the step rides its token.
  const inSession = <A>(
    sessionID: string,
    owner: string,
    claim: boolean,
    use: (runner: SessionRunner.Interface, session: SessionSchema.Info) => Effect.Effect<A, SessionRunner.RunError>,
  ) =>
    Effect.gen(function* () {
      const session = yield* store.get(SessionSchema.ID.make(sessionID))
      if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
      if (claim) yield* events.claim(session.id, owner)
      // A worker taking this step on a host without the project tree rebuilds it from snapshot packs.
      yield* worktrees.ensure(session.location.directory)
      return yield* SessionRunner.Service.use((runner) => use(runner, session)).pipe(
        Effect.provide(locations.get(session.location)),
      )
    }).pipe(Effect.provideService(EventV2.EventOwner, owner), Effect.provide(ctx), Effect.scoped)

  const modelCallDrain = async (
    input: ModelCallDrainInput & { readonly owner: string },
    signal: AbortSignal,
  ): Promise<ModelCallDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, true, (runner, session) =>
        runner.runModelCall({
          sessionID: session.id,
          step: input.step,
          promotion: (input.promotion ?? undefined) as SessionInput.Delivery | undefined,
          first: input.first,
          force: input.force,
        }),
      ).pipe(
        Effect.map((result): ModelCallDrainResult =>
          result.kind === "settled"
            ? {
                kind: "settled",
                result: {
                  ran: result.result.ran,
                  continue: result.result.continue,
                  step: result.result.step,
                  promotion: result.result.promotion ?? null,
                },
              }
            : {
                kind: "called",
                step: result.step,
                calls: result.calls,
                settlement: result.settlement,
                owner: input.owner,
              },
        ),
      ),
    )

  const toolCallDrain = async (input: ToolCallDrainInput, signal: AbortSignal): Promise<ToolCallDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, false, (runner, session) =>
        runner.runToolCall({
          sessionID: session.id,
          call: input.call,
          retry: input.retry === true,
        }),
      ),
    )

  const sealDrain = async (input: SealDrainInput, signal: AbortSignal): Promise<StepDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, false, (runner, session) =>
        runner.sealStep({ sessionID: session.id, step: input.step, settlement: input.settlement }),
      ).pipe(
        Effect.map((result) => ({
          ran: result.ran,
          continue: result.continue,
          step: result.step,
          promotion: result.promotion ?? null,
        })),
      ),
    )

  return { modelCallDrain, toolCallDrain, sealDrain }
}
