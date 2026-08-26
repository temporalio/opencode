// Crossing from Effect into an activity result. Every drain in this package ends here, so the way a
// failure is classified is decided in one place:
//   - an interrupt caused by the driver's own cancellation rethrows the abort reason, so the attempt
//     records Cancelled rather than Failed
//   - an interrupt with no cancellation is an internal halt (a user declining a permission) and must
//     be non-retryable, or the supervisor re-drives a turn the user explicitly stopped
//   - a genuine run error crosses non-retryable with the RunError encoded in `details`, so the caller
//     reconstructs the exact typed error instead of a string
// Only crashes and task timeouts (never thrown here) go through the activity retry policy.

import { Cause, Effect, Exit } from "effect"
import { ApplicationFailure } from "@temporalio/activity"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionRunDeclinedError } from "@opencode-ai/core/session/error"
import { encodeRunError } from "@opencode-ai/core/session/execution/run-error-codec"

export const runAtBoundary = async <A>(
  sessionID: string,
  signal: AbortSignal,
  body: Effect.Effect<A, unknown, never>,
): Promise<A> => {
  const exit = await Effect.runPromiseExit(body, { signal })
  if (Exit.isSuccess(exit)) return exit.value
  const cause = exit.cause
  if (Cause.hasInterruptsOnly(cause)) {
    if (signal.aborted)
      throw signal.reason instanceof Error ? signal.reason : new Error("session run interrupted")
    const declined = encodeRunError(new SessionRunDeclinedError({ sessionID: SessionSchema.ID.make(sessionID) }))
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
