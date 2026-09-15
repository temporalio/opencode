// The three drain bodies a stepped turn is made of: the provider attempt, one tool call, and the
// seal. A whole-step drain runs one attempt plus all of its tools in a single activity, so nothing
// can sit between the model asking for a tool and the tool running. Splitting them is what gives
// each tool call its own retry policy, timeout and approval window.
//
// All three write to the same session log, so they publish under one owner token: the model call
// claims it and hands it back, the other two inherit it. A token per activity execution, which is
// right when the activity is the whole step, would make a step's writers fence each other out.

import { Effect } from "effect"
import type { DeferredToolCall, ToolCallOutcome } from "@opencode-ai/core/session/runner"
import type { StepSettlement } from "@opencode-ai/core/session/runner/publish-llm-event"
import type { SessionInput } from "@opencode-ai/core/session/input"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { runAtBoundary } from "./boundary"
import { type InSession, type StepDrainInput, type StepDrainResult, sessionSpend, toStepResult } from "./drain"

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
      readonly assistantMessageID?: string
      readonly needsContinuation?: boolean
      /** The event-log token this attempt claimed. The tool and seal activities of this step must
       * publish under it, so it travels with the calls instead of being minted again. */
      readonly owner: string
      /** The queue this worker polls on its own, when it has one. The tools of this step write the
       * tree this worker is standing in, so sending them here keeps them on it. Absent when the
       * worker was not given a queue of its own, and never required: the step falls back to the
       * shared queue and the tree is rebuilt there. */
      readonly queue?: string
      /** What the session has been billed in total, off its own row. See `sessionSpend`. */
      readonly session?: { readonly tokens: number }
    }

export interface ToolCallDrainInput {
  readonly sessionID: string
  readonly call: DeferredToolCall
  readonly owner: string
  /** Which step this call belongs to, so a host can tell it from a call an earlier step left. */
  readonly step: number
}

export interface ToolCallDrainResult {
  readonly outcome: ToolCallOutcome
}

export interface SealDrainInput {
  readonly sessionID: string
  readonly step: number
  readonly settlement?: StepSettlement
  readonly assistantMessageID?: string
  readonly needsContinuation?: boolean
  readonly owner: string
  /** This step is being closed away from the host that was running it. The tree is not this seal's
   * to rebuild or to ship: the host that ran the tools is the only one holding what they did, and
   * it may still be inside one of them. Writing the step down is the whole job. */
  readonly withoutTheTree?: boolean
}

export interface SteppedDrainDeps {
  readonly inSession: InSession
  /** Records which call is inside its own execution on this host, and takes the record back. */
  readonly worktrees: Pick<WorktreeMaterializer.Interface, "beginWrite" | "endWrite">
  /** The queue this worker polls on its own, reported by the model call so the rest of the step can
   * be sent back to it. Absent when the worker has none. */
  readonly stepQueue?: string
}

// A call, as the host records it: enough to tell this step's writers from an earlier step's.
const writer = (input: ToolCallDrainInput) => ({
  sessionID: input.sessionID,
  step: input.step,
  callID: input.call.id,
})

export const makeSteppedDrains = ({ inSession, worktrees, stepQueue }: SteppedDrainDeps) => {
  // Only the model call claims the log: it is the writer that supersedes a previous attempt, and
  // the rest of the step rides its token.
  const modelCallDrain = async (
    input: ModelCallDrainInput & { readonly owner: string },
    signal: AbortSignal,
  ): Promise<ModelCallDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, { claim: true }, (runner, session) =>
        runner
          .runModelCall({
            sessionID: session.id,
            step: input.step,
            promotion: (input.promotion ?? undefined) as SessionInput.Delivery | undefined,
            first: input.first,
            force: input.force,
          })
          // The session's own totals, off the row this drain already loaded. The attempt says what
          // it cost; the row says what the session has cost, which is what outlives this run.
          .pipe(Effect.map((called) => ({ ...called, session: sessionSpend(session) }))),
      ).pipe(
        Effect.map(
          (result): ModelCallDrainResult =>
            result === undefined || result.kind === "settled"
              ? { kind: "settled", result: toStepResult(input.step, result?.result) }
              : {
                  ...result,
                  owner: input.owner,
                  ...(stepQueue === undefined ? {} : { queue: stepQueue }),
                  ...(result.session ? { session: result.session } : {}),
                },
        ),
      ),
    )

  const toolCallDrain = async (
    input: ToolCallDrainInput,
    signal: AbortSignal,
    /** Whether a cancellation means the turn is over rather than this attempt being handed on.
     * Only the first closes the call: a worker shutting down leaves it for the next attempt, which
     * has to be free to decide whether the tool may run again. */
    turnEnded: () => boolean = () => false,
  ): Promise<ToolCallDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, { current: writer(input) }, (runner, session) =>
        Effect.acquireUseRelease(
          // Said before the tool can touch anything, and taken back when its body returns. It is
          // the only record on this host of a call still inside its own execution, and what a
          // later step reads before it uses this directory: a timeout settles the workflow's
          // promise without stopping the process behind it.
          worktrees.beginWrite(session.location.directory, writer(input)),
          () =>
            runner.runToolCall({ sessionID: session.id, call: input.call }).pipe(
              // A stop landing mid-tool leaves the call recorded as running, where a whole step
              // closes the tools it opened before it returns. Nothing else closes it until the
              // next turn's entry check, so a transcript would show the call still going long
              // after the stop.
              Effect.onInterrupt(() =>
                turnEnded()
                  ? runner.failToolCall({ sessionID: session.id, call: input.call }).pipe(Effect.ignore)
                  : Effect.void,
              ),
            ),
          () => worktrees.endWrite(session.location.directory, input.call.id),
        ),
      ).pipe(Effect.map((result) => result ?? { outcome: "already-settled" as const })),
    )

  const sealDrain = async (input: SealDrainInput, signal: AbortSignal): Promise<StepDrainResult> =>
    runAtBoundary(
      input.sessionID,
      signal,
      inSession(input.sessionID, input.owner, { withoutTheTree: input.withoutTheTree }, (runner, session) =>
        runner
          .sealStep({
            sessionID: session.id,
            step: input.step,
            settlement: input.settlement,
            assistantMessageID: input.assistantMessageID,
            needsContinuation: input.needsContinuation,
            withoutTheTree: input.withoutTheTree,
          })
          // Only the loop decision: what the step spent was reported by its model call.
          .pipe(
            Effect.map((result) => ({
              continue: result.continue,
              step: result.step,
              promotion: result.promotion ?? null,
            })),
          ),
      ).pipe(Effect.map((result) => result ?? toStepResult(input.step))),
    )

  return { modelCallDrain, toolCallDrain, sealDrain }
}
