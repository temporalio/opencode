export * as SessionRunner from "./index"

import type { LLMError } from "@opencode-ai/llm"
import { Context, Effect } from "effect"
import { SessionSchema } from "../schema"
import type { ContextSnapshotDecodeError, MessageDecodeError, SessionRunDeclinedError } from "../error"
import type { SessionInput } from "../input"
import { SessionRunnerModel } from "./model"
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

/** Result of one step: whether it ran, whether to continue, and the next loop state. */
export interface StepResult {
  readonly ran: boolean
  readonly continue: boolean
  readonly step: number
  readonly promotion: SessionInput.Delivery | undefined
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
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SessionRunner") {}
