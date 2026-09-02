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
  /** Where a step says what became of the calls it dispatched. A dispatch that decided not to run
   * a tool, or could not keep its result, is a step's most surprising outcome and the least
   * visible: it reads as an ordinary success everywhere else. */
  readonly log?: (message: string, attributes: Record<string, unknown>) => void
  /** Run the calls one at a time. Each tool ships the tree from the host that ran it, so two on two
   * hosts each publish a tree without the other's work and the second is refused, leaving its work
   * stranded there. Serial is what moving files between hosts costs. */
  readonly serial?: boolean
  /** Run something where the driver's cancellation cannot reach it. An interrupt landing during the
   * tool phase otherwise leaves the step with no ending published at all, so a follower waiting on
   * the turn never hears it stop. */
  readonly nonCancellable?: <A>(fn: () => Promise<A>) => Promise<A>
}

/**
 * Drives one step and reports the next loop state, so it drops straight into the supervisor in
 * place
 * of a whole-step activity.
 */
export const makeSteppedTurn =
  ({ activities, isCancellation, isHalt, log, serial, nonCancellable }: SteppedTurnDeps) =>
  async (input: StepDrainInput): Promise<StepDrainResult> => {
    const model = await activities.runModelCall(input)
    // A crashed step finalized from the log, or the recovery gate finding no work: the step is over
    // and there is nothing to dispatch or seal.
    if (model.kind === "settled") return model.result

    // Each call is its own unit of work. A tool that fails outright does not take the turn with it:
    // the seal closes its call as an error and the model gets to react, which is better than losing
    // the step. A cancel and a user halt are different, and both have to propagate.
    const dispatch = (call: (typeof model.calls)[number]) =>
      activities.runToolCall({ sessionID: input.sessionID, call, owner: model.owner })
    const dispatched: PromiseSettledResult<ToolCallDrainResult>[] = []
    if (serial) {
      // One at a time, and still settled rather than thrown, so a tool that fails does not take the
      // rest of the batch with it. The loop keeps going: the seal closes each call and the model
      // reacts to what it is told.
      for (const call of model.calls) {
        dispatched.push(
          await dispatch(call).then(
            (value) => ({ status: "fulfilled", value }) as const,
            (reason) => ({ status: "rejected", reason }) as const,
          ),
        )
      }
    } else {
      dispatched.push(...(await Promise.allSettled(model.calls.map(dispatch))))
    }
    const seal = (needsContinuation: boolean | undefined) =>
      activities.sealStep({
        sessionID: input.sessionID,
        step: model.step,
        settlement: model.settlement,
        assistantMessageID: model.assistantMessageID,
        needsContinuation,
        owner: model.owner,
      })

    for (const outcome of dispatched) {
      if (outcome.status !== "rejected") continue
      if (isCancellation(outcome.reason) || isHalt(outcome.reason)) {
        // Seal on the way out, out of reach of the cancellation, so the calls that did return keep
        // their results and the step is recorded as ended. Without it a stop landing during the
        // tools publishes no step event at all: the interrupt is only visible during the model
        // call, and a follower waiting on the turn hangs.
        //
        // Explicitly with no continuation. Letting the seal decide is what once carried the agent
        // on past a declined permission, because it re-derived "keep going" from the tool parts.
        // The reason the turn is stopping is rethrown either way, and a seal that fails here must
        // not replace it.
        await (nonCancellable ?? ((fn: () => Promise<unknown>) => fn()))(() => seal(false)).catch(
          (err) => log?.("could not seal an interrupted step", { step: model.step, error: String(err) }),
        )
        throw outcome.reason
      }
    }

    // A dispatch that settled its call needs no telling. The rest are what an operator is looking
    // for when a turn did something unexpected: a tool reported as unknown ran or did not, and
    // nothing else in this workflow's history says which call that was.
    const unsettled = model.calls
      .map((call, index) => {
        const result = dispatched[index]
        return {
          call: call.id,
          tool: call.name,
          outcome: result?.status === "fulfilled" ? result.value.outcome : "errored",
        }
      })
      .filter((entry) => entry.outcome !== "settled")
    if (unsettled.length > 0)
      log?.("step did not settle every call it dispatched", { step: model.step, calls: unsettled })

    return seal(model.needsContinuation)
  }
