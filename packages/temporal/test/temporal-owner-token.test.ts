// Activity attempt numbers restart at 1 on every step, so a token built from the run id and the
// attempt alone repeats across steps. The token has to be unique per activity execution, or a
// zombie attempt from an earlier step matches the current owner.
import { describe, it, expect } from "bun:test"
import { ownerTokenFrom } from "../src/activities"

describe("temporal event-log owner token", () => {
  const run = "run-1"

  it("distinguishes a retry of the same step from its prior attempt (retry fences prior)", () => {
    expect(ownerTokenFrom(run, "act-1", 1)).not.toBe(ownerTokenFrom(run, "act-1", 2))
  })

  it("distinguishes different steps that share an attempt number (no cross-step collision)", () => {
    expect(ownerTokenFrom(run, "act-1", 1)).not.toBe(ownerTokenFrom(run, "act-2", 1))
  })

  it("is stable for a given execution", () => {
    expect(ownerTokenFrom(run, "act-7", 3)).toBe(ownerTokenFrom(run, "act-7", 3))
  })
})
