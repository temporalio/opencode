// What a refused directory costs the session is decided at the activity boundary, not in the
// materializer that raises it.
//
// Two things have to be true of it. It has to be retryable, or a host holding somebody's abandoned
// tool ends the turn for every host: the refusal is this host saying no, and the same dispatch runs
// fine on a host that is not holding one. And it has to carry its own retry delay, because the
// backoff a failing activity earns doubles per attempt, and the host that answers first and refuses
// fastest is exactly the one that would push the next attempt minutes out while a free host idles.

import { expect, it } from "bun:test"
import { Effect } from "effect"
import { ApplicationFailure } from "@temporalio/common"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { runAtBoundary } from "../src/boundary"

const crossing = (body: Effect.Effect<never, never, never>) =>
  runAtBoundary("ses_refused", new AbortController().signal, body).then(
    () => undefined,
    (err: unknown) => err as ApplicationFailure,
  )

it("hands a refused directory back as retryable work with its own delay", async () => {
  const refused = await crossing(
    Effect.die(
      new WorktreeMaterializer.WorktreeQuarantinedError({
        message: "not using /project: a call of an earlier step never returned",
      }),
    ),
  )
  expect(refused).toBeInstanceOf(ApplicationFailure)
  expect(refused?.type).toBe("WorktreeMaterializer.QuarantinedError")
  expect(refused?.nonRetryable).toBeFalsy()
  expect(refused?.nextRetryDelay).toBeDefined()
})

it("leaves an ordinary run error non-retryable and on the backoff", async () => {
  // The contrast that makes the above mean something: re-running a step whose input the model has
  // already answered is worse than failing it, so everything that is about the work stays as it was.
  const failed = await crossing(Effect.die(new Error("the tool blew up")))
  expect(failed?.nonRetryable).toBe(true)
  expect(failed?.nextRetryDelay).toBeUndefined()
})
