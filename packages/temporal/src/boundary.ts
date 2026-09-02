// Crossing from Effect into an activity result. Every drain in this package ends here, so the way a
// failure is classified is decided in one place:
//   - an interrupt caused by the driver's own cancellation rethrows the abort reason, so the
//     attempt records Cancelled rather than Failed
//   - a run the user refused (a declined permission) crosses non-retryable under its own type, or
//     the supervisor re-drives a turn the user explicitly stopped
//   - a genuine run error crosses non-retryable with the RunError encoded in `details`, so the
//     caller reconstructs the exact typed error instead of a string
// Only crashes and task timeouts (never thrown here) go through the activity retry policy.

import { Cause, Effect, Exit } from "effect"
import { ApplicationFailure } from "@temporalio/activity"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionRunDeclinedError } from "@opencode-ai/core/session/error"
import { encodeRunError } from "@opencode-ai/core/session/execution/run-error-codec"
import { HALTED_FAILURE_TYPE } from "./protocol"

export interface BoundaryOptions {
  /** Whether an interrupt the body raises on its own, with nothing cancelling it, means the user
   * refused the run. The whole-step path says yes: it turns a decline into an interrupt to halt
   * the loop, which is the local behaviour V1 defined and its own tests pin. The stepped path says
   * no, because its dispatch names the decline, so an interrupt there is the runner stopping for
   * some other reason and reading it as a refusal would put words in the user's mouth. */
  readonly declineIsInterrupt?: boolean
}

const halted = (sessionID: string, declined?: SessionRunDeclinedError) => {
  const encoded = encodeRunError(
    declined ?? new SessionRunDeclinedError({ sessionID: SessionSchema.ID.make(sessionID) }),
  )
  return ApplicationFailure.create({
    message: "session run halted (user declined)",
    type: HALTED_FAILURE_TYPE,
    nonRetryable: true,
    details: encoded === undefined ? undefined : [encoded],
  })
}

// Failures that say something about the moment rather than about the work: storage that was not
// reachable, a defect from a database call that `orDie` turned into one. Everything else stays
// non-retryable, because re-running a step whose input the model already answered is worse than
// failing it. Without this a libsql blip during a seal failed the step for good rather than moving
// it to another worker.
const TRANSIENT = new Set([
  "ToolOutputStore.StorageError",
  "SqlError",
  "SqliteError",
  // A rebuild that did not finish. git and the filesystem fail for reasons that pass, and the
  // alternative is a turn failing for good because one worker had a bad minute.
  "WorktreeMaterializer.MaterializeError",
])

export const runAtBoundary = async <A>(
  sessionID: string,
  signal: AbortSignal,
  body: Effect.Effect<A, unknown, never>,
  options?: BoundaryOptions,
): Promise<A> => {
  const exit = await Effect.runPromiseExit(body, { signal })
  if (Exit.isSuccess(exit)) return exit.value
  const cause = exit.cause
  if (Cause.hasInterruptsOnly(cause)) {
    if (signal.aborted)
      throw signal.reason instanceof Error ? signal.reason : new Error("session run interrupted")
    if (options?.declineIsInterrupt) throw halted(sessionID)
    // Nothing cancelled this run and nothing named a reason: the runner stopped itself, which a
    // session handed to a runner bound to another location does.
    throw ApplicationFailure.create({
      message: "session run interrupted with no cancellation",
      type: "SessionRunInterrupted",
      nonRetryable: true,
    })
  }
  const squashed = Cause.squash(cause) as { _tag?: string; message?: string }
  if (squashed instanceof SessionRunDeclinedError) throw halted(sessionID, squashed)
  const encoded = encodeRunError(squashed)
  throw ApplicationFailure.create({
    message: squashed?.message ?? Cause.pretty(cause),
    type: squashed?._tag ?? "SessionRunError",
    nonRetryable: !(squashed?._tag !== undefined && TRANSIENT.has(squashed._tag)),
    details: encoded === undefined ? undefined : [encoded],
  })
}
