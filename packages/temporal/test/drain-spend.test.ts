// What a step reports it was billed for, which is what the turn's budget adds up.
//
// Two paths report it and they have to report the same number: the split step takes it off the
// settlement the attempt hands back, and the whole-step drain takes it off the result the runner
// returns. Before that second one existed, a deployment that never split a step had no bound on
// what a turn spends, only on how many steps it took.

import { expect, it } from "bun:test"
import { Context, Effect, Layer } from "effect"
import { billed } from "@opencode-ai/core/session/runner/publish-llm-event"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { makeDrains, type DrainDeps } from "../src/drain"
import { makeSteppedTurn, type SteppedActivities } from "../src/stepped-turn"

// One settlement, and both paths have to make the same number of it. The workflow file writes this
// arithmetic out again because it is bundled into the sandbox and may not import core, so nothing
// but this holds the two copies together.
const SETTLEMENT = {
  finish: "stop",
  tokens: { input: 100, output: 20, reasoning: 5, cache: { read: 1000, write: 30 } },
}
const BILLED = { tokens: 1155 }

it("counts everything the provider bills for, cache included", () => {
  // A long context is mostly cache reads. A bound that drops them is one it walks straight through.
  expect(billed(SETTLEMENT)).toEqual(BILLED)
})

it("makes the same number of it in the workflow, which cannot import that one", async () => {
  const activities: SteppedActivities = {
    runModelCall: async () => ({
      kind: "called",
      step: 1,
      calls: [],
      settlement: SETTLEMENT,
      owner: "run:1:1",
    }),
    runToolCall: async () => ({ outcome: "settled" }),
    sealStep: async () => ({ continue: false, step: 2, promotion: null }),
  }

  const result = await makeSteppedTurn({
    activities,
    isCancellation: () => false,
    isHalt: () => false,
  })({ sessionID: "ses_spend", step: 1, promotion: null, first: true, force: false })

  expect(result.spent).toEqual(BILLED)
})

it("says nothing, rather than nothing spent, when the attempt reported no counts", () => {
  expect(billed(undefined)).toBeUndefined()
})

// The session row carries its own running totals, which is where a session's bound reads from.
const SESSION_ROW = {
  id: "ses_spend",
  location: { directory: "/project" },
  tokens: { input: 40, output: 5, reasoning: 1, cache: { read: 200, write: 4 } },
}

const drainsOver = (runStep: SessionRunner.Interface["runStep"]) =>
  makeDrains({
    store: { get: () => Effect.succeed(SESSION_ROW) } as unknown as DrainDeps["store"],
    locations: {
      get: () => Layer.succeed(SessionRunner.Service, { runStep } as unknown as SessionRunner.Interface),
    } as unknown as DrainDeps["locations"],
    ctx: Context.empty() as DrainDeps["ctx"],
    events: { claim: () => Effect.void } as unknown as DrainDeps["events"],
    worktrees: {
      ensure: () => Effect.void,
      beginWrite: () => Effect.void,
      endWrite: () => Effect.void,
    } as unknown as DrainDeps["worktrees"],
  })

it("carries what a whole step spent out to the supervisor", async () => {
  const { stepDrain } = drainsOver((() =>
    Effect.succeed({
      continue: true,
      step: 2,
      promotion: undefined,
      spent: { tokens: 1155 },
    })) as unknown as SessionRunner.Interface["runStep"])

  const result = await stepDrain(
    { sessionID: "ses_spend", step: 1, promotion: null, first: true, force: false },
    new AbortController().signal,
  )

  expect(result.spent).toEqual({ tokens: 1155 })
  // And what the session has been billed in total, off its own row: 40 in, 5 out, 1 reasoning,
  // 200 cache read, 4 cache write. That number outlives this run, where the one above does not.
  expect(result.session).toEqual({ tokens: 250 })
})

it("carries nothing out when the step had nothing to say", async () => {
  const { stepDrain } = drainsOver((() =>
    Effect.succeed({
      continue: false,
      step: 2,
      promotion: undefined,
    })) as unknown as SessionRunner.Interface["runStep"])

  const result = await stepDrain(
    { sessionID: "ses_spend", step: 1, promotion: null, first: true, force: false },
    new AbortController().signal,
  )

  expect(result.spent).toBeUndefined()
  // The session's own total is not about this step, so it is reported either way.
  expect(result.session).toEqual({ tokens: 250 })
})
