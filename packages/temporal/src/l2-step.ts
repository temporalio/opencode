// One step as three units of work instead of one: the provider attempt, each tool call it asks for,
// and the seal that closes it. This is the whole point of the split, and it is workflow code, so
// the
// model-to-tools loop lives where retries, timers, approvals and budgets can sit between the two.
//
// MUST stay pure, like supervisor.ts: this is bundled into the workflow sandbox, so no `effect`, no
// `@opencode-ai/core` runtime imports, no Node builtins. Type-only imports are erased and safe.
//
// What this costs, stated plainly: a whole-step activity starts each tool the moment the model asks
// for it, while the stream is still going. Here the attempt has to return before any tool starts,
// because a workflow cannot consume a stream. The tools of one step still run concurrently with
// each
// other; what is lost is the overlap between the model and its own tools.

import { ActivityFailure, type ApplicationFailure } from "@temporalio/workflow"
import { HALTED_FAILURE_TYPE } from "./protocol"
import type { StepDrainInput, StepDrainResult } from "./drain"
import type {
  ModelCallDrainInput,
  ModelCallDrainResult,
  SealDrainInput,
  ToolCallDrainInput,
  ToolCallDrainResult,
} from "./l2-drain"

/**
 * A halt the user asked for, as it looks once it has crossed an activity boundary. The runner
 * raises
 * it as an interrupt, `boundary.ts` throws it as an `ApplicationFailure`, and the SDK wraps that in
 * one `ActivityFailure`. It is not a cancellation, so `isCancellation` says no, and a dispatcher
 * that checks only that would treat a refusal as one failed tool and carry on.
 *
 * A `TimeoutFailure` or a `CancelledFailure` cause has no `type` field, so neither matches.
 */
export const isHaltFailure = (error: unknown) =>
  error instanceof ActivityFailure &&
  (error.cause as ApplicationFailure | undefined)?.type === HALTED_FAILURE_TYPE

/** The three activities a stepped turn drives. */
export interface SteppedActivities {
  readonly runModelCall: (input: ModelCallDrainInput) => Promise<ModelCallDrainResult>
  readonly runToolCall: (input: ToolCallDrainInput) => Promise<ToolCallDrainResult>
  readonly sealStep: (input: SealDrainInput) => Promise<StepDrainResult>
}

export interface SteppedTurnDeps {
  readonly activities: SteppedActivities
  /** Whether an error is the driver's cancellation. An interrupt has to end the turn, so it must
   * not be swallowed the way a failed tool is. */
  readonly isCancellation: (error: unknown) => boolean
  /** Whether an error is the user stopping the turn, like a declined permission. It arrives as an
   * ordinary activity failure, so without this it reads as one bad tool and the turn carries on
   * past the refusal. */
  readonly isHalt: (error: unknown) => boolean
}

/**
 * Drives one step and reports the next loop state, so it drops straight into the supervisor in
 * place
 * of a whole-step activity.
 */
export const makeSteppedTurn =
  ({ activities, isCancellation, isHalt }: SteppedTurnDeps) =>
  async (input: StepDrainInput): Promise<StepDrainResult> => {
    const model = await activities.runModelCall(input)
    // A crashed step finalized from the log, or the recovery gate finding no work: the step is over
    // and there is nothing to dispatch or seal.
    if (model.kind === "settled") return model.result

    // Each call is its own unit of work. A tool that fails outright does not take the turn with it:
    // the seal closes its call as an error and the model gets to react, which is better than losing
    // the step. A cancel and a user halt are different, and both have to propagate.
    const dispatched = await Promise.allSettled(
      model.calls.map((call) =>
        activities.runToolCall({ sessionID: input.sessionID, call, owner: model.owner }),
      ),
    )
    for (const outcome of dispatched) {
      if (outcome.status !== "rejected") continue
      if (isCancellation(outcome.reason) || isHalt(outcome.reason)) throw outcome.reason
    }

    return activities.sealStep({
      sessionID: input.sessionID,
      step: model.step,
      settlement: model.settlement,
      assistantMessageID: model.assistantMessageID,
      needsContinuation: model.needsContinuation,
      owner: model.owner,
    })
  }
