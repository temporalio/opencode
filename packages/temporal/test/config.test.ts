// The budget reaches the supervisor through the workflow input, and the workflow input is built from
// what this module read. A bound that is set and read as nothing is the one failure an operator
// cannot see, so what is set but unreadable refuses to start rather than reading as none.
import { afterEach, expect, it } from "bun:test"
import { TemporalConfig } from "../src/config"

const VARS = [
  "OPENCODE_TEMPORAL_BUDGET_TOKENS",
  "OPENCODE_TEMPORAL_BUDGET_SECONDS",
  "OPENCODE_TEMPORAL_BUDGET_HARD_SECONDS",
  "OPENCODE_TEMPORAL_BUDGET_SESSION_TOKENS",
  "OPENCODE_TEMPORAL_BUDGET_SESSION_SECONDS",
]

afterEach(() => {
  for (const name of VARS) delete process.env[name]
})

it("reads no budget when nothing is set", () => {
  expect(TemporalConfig.fromEnv().budget).toBeUndefined()
})

it("reads the bounds that are set and leaves the rest off", () => {
  process.env.OPENCODE_TEMPORAL_BUDGET_TOKENS = "250000"
  process.env.OPENCODE_TEMPORAL_BUDGET_SESSION_SECONDS = "3600"

  const config = TemporalConfig.fromEnv()

  // Off means absent, not zero: the supervisor reads a missing bound as no bound.
  expect(config.budget).toEqual({ tokens: 250000, sessionSeconds: 3600 })
  expect(TemporalConfig.preflight(config)).toEqual([])
  expect(TemporalConfig.describe(config).budget).toBe("tokens=250000 sessionSeconds=3600")
})

it("refuses a bound it cannot read as a positive number", () => {
  process.env.OPENCODE_TEMPORAL_BUDGET_SECONDS = "ten minutes"

  const config = TemporalConfig.fromEnv()

  expect(config.budget).toBeUndefined()
  expect(TemporalConfig.preflight(config).some((p) => p.includes("OPENCODE_TEMPORAL_BUDGET_SECONDS"))).toBe(true)
})
