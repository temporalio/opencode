export * as SessionRunner from "./index"

import type { LLMError } from "@opencode-ai/llm"
import { Context, Effect } from "effect"
import { SessionSchema } from "../schema"
import type { ContextSnapshotDecodeError, MessageDecodeError, SessionRunDeclinedError } from "../error"
import type { SessionInput } from "../input"
import { SessionRunnerModel } from "./model"
import type { StepSettlement } from "./publish-llm-event"
import type { SystemContext } from "../../system-context/index"
import type { ToolOutputStore } from "../../tool-output-store"

export type RunError =
  | LLMError
  | SessionRunnerModel.Error
  | MessageDecodeError
  | ContextSnapshotDecodeError
  | SessionRunDeclinedError
  | SystemContext.InitializationBlocked
  | ToolOutputStore.Error

/** Input for one step (one provider attempt + its tools) of a turn. */
export interface StepInput {
  readonly sessionID: SessionSchema.ID
  readonly step: number
  readonly promotion: SessionInput.Delivery | undefined
  readonly first: boolean
  readonly force: boolean
}

/** Result of one step: whether to continue, and the next loop state. */
export interface StepResult {
  readonly continue: boolean
  readonly step: number
  readonly promotion: SessionInput.Delivery | undefined
}

/** A tool call the provider asked for, recorded but not run, handed to the caller to dispatch. Every
 * id comes from the provider or the publisher and is carried, never regenerated: a second run of the
 * same step would mint different ones and the results would not match the log. The arguments stay
 * off it, because the record already holds them as the provider's raw JSON string, which the
 * dispatcher parses. */
export interface DeferredToolCall {
  readonly id: string
  readonly name: string
  readonly assistantMessageID: string
}

/** Closing one step whose tools have been dispatched. */
export interface SealStepInput {
  readonly sessionID: SessionSchema.ID
  readonly step: number
  /** The provider's finish reason and token counts, from the attempt that opened this step. They
   * live only in that process's memory, so they have to be carried here rather than read back. */
  readonly settlement?: StepSettlement
  /** The message the attempt opened. Absent only for a step that produced nothing to close. */
  readonly assistantMessageID?: string
  /** Whether the turn should keep going, decided by the attempt. Only the attempt knows it hit a
   * provider error, so a seal that re-derives this from the log would keep calling a provider that
   * just failed. Absent on a re-drive, where the log is all there is. */
  readonly needsContinuation?: boolean
  /** This step is being closed away from the host that ran it, so the files are not this seal's to
   * touch: it is standing in a directory that never saw the tools, and the host that did may still
   * be inside one of them. Writing the step down is the whole job here. */
  readonly withoutTheTree?: boolean
}

/** One recorded tool call, to run on its own. */
export interface ToolCallInput {
  readonly sessionID: SessionSchema.ID
  readonly call: DeferredToolCall
}

/** How a dispatch ended, for the executor's own record: the log says what the model was told, not
 * whether nothing ran (`already-settled`), the tool could not be settled (`failed`), or a repeat was
 * refused because the tool does not declare itself repeatable (`unknown`). */
export type ToolCallOutcome = "settled" | "already-settled" | "failed" | "unknown"

export interface ToolCallResult {
  readonly outcome: ToolCallOutcome
}

/** What one provider attempt produced. `calls` is empty unless the caller deferred dispatch, in
 * which case the step is left open and `settlement` is what seals it. */
export interface TurnAttemptResult {
  readonly needsContinuation: boolean
  readonly step: number
  readonly calls: ReadonlyArray<DeferredToolCall>
  readonly settlement?: StepSettlement
  /** The assistant message this attempt opened, minted here so a seal in another process closes the
   * right one even when the turn published no content of its own. */
  readonly assistantMessageID?: string
}

/** What a model-only attempt produced. `settled` means the step is already over (a crashed step was
 * finalized from the log, or the recovery gate found no work) and there is nothing to dispatch.
 * `called` hands back the recorded calls plus the settlement whoever seals the step will need. */
export type ModelCallResult =
  | { readonly kind: "settled"; readonly result: StepResult }
  | {
      readonly kind: "called"
      readonly step: number
      readonly calls: ReadonlyArray<DeferredToolCall>
      readonly settlement?: StepSettlement
      readonly assistantMessageID?: string
      readonly needsContinuation?: boolean
    }

/** Runs one local continuation from already-recorded Session history. */
export interface Interface {
  /** Drains eligible durable work. Explicit runs perform one provider attempt even when no work is eligible. */
  readonly run: (input: {
    readonly sessionID: SessionSchema.ID
    readonly force: boolean
  }) => Effect.Effect<void, RunError>
  /** Run exactly one step and report the next loop state, so a caller (e.g. a Temporal workflow)
   * can drive the turn one step at a time. Mirrors one iteration of `run`'s loop. */
  readonly runStep: (input: StepInput) => Effect.Effect<StepResult, RunError>
  /** Run the provider attempt of one step and stop, handing back the tool calls it asked for instead
   * of running them. The caller dispatches each one and then seals the step. This is what puts the
   * model-to-tools loop in a durable executor's hands rather than inside a single activity. */
  readonly runModelCall: (input: StepInput) => Effect.Effect<ModelCallResult, RunError>
  /** Run one recorded tool call and publish its result. Safe to call twice for the same call: the
   * second sees the settled result and does nothing, and a call whose first dispatch died after it
   * started is run again only if the tool declares itself repeatable. */
  readonly runToolCall: (input: ToolCallInput) => Effect.Effect<ToolCallResult, RunError>
  /** Close a call a stop cut short, so it does not sit in the log as running until the next turn.
   * A whole step closes the tools it opened on its way out; a dispatch that is its own unit of work
   * has to be told. A call that never started, and one that already settled, are left alone. */
  readonly failToolCall: (input: ToolCallInput) => Effect.Effect<void, RunError>
  /** Close a step once its calls have been dispatched: snapshot, file diff, Step.Ended, and the
   * loop decision. Safe to call twice: the second sees the step already closed and returns the same
   * answer without publishing again. The carried message id is what keeps a retried seal from
   * closing a different step. */
  readonly sealStep: (input: SealStepInput) => Effect.Effect<StepResult, RunError>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionRunner") {}
