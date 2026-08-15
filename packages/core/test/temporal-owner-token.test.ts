// Regression for the event-log owner-token collision: activity attempt numbers restart at 1 per
// step, so a run-id+attempt token repeats across steps and lets a zombie attempt from an earlier
// step re-match the current owner. The token must be unique per activity execution.
import { describe, it, expect } from "bun:test"
import { ownerTokenFrom } from "@opencode-ai/core/session/execution/temporal-activities"

describe("temporal event-log owner token", () => {
  const run = "run-1"

  it("distinguishes a retry of the same step from its prior attempt (retry fences prior)", () => {
    expect(ownerTokenFrom(run, "act-1", 1)).not.toBe(ownerTokenFrom(run, "act-1", 2))
  })

  it("distinguishes different steps that share an attempt number (no cross-step collision)", () => {
    // The bug: step 1 attempt 1 and step 2 attempt 1 both minted `run#1`. With the activity id in
    // the token they are disjoint, so a zombie from step 1 cannot re-authorize against step 2.
    expect(ownerTokenFrom(run, "act-1", 1)).not.toBe(ownerTokenFrom(run, "act-2", 1))
  })

  it("is stable for a given execution", () => {
    expect(ownerTokenFrom(run, "act-7", 3)).toBe(ownerTokenFrom(run, "act-7", 3))
  })
})
