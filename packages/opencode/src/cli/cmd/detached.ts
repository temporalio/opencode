// Commands about the deployment rather than about a session in front of you. `doctor` loads the
// Temporal driver only when it runs, so the rest of the `session` command never pays for it.

import type { Argv } from "yargs"
import { cmd } from "./cmd"
import { UI } from "../ui"

// What this process resolved, and what is wrong with it. Deploying was a handful of variables that
// have to agree, with no way to ask whether they did: every mistake in them fails as something
// else, hours later, on whoever prompted the session rather than on whoever deployed it.
export const SessionDoctorCommand = cmd({
  command: "doctor",
  describe: "what this deployment resolved, and what is wrong with it",
  builder: (yargs: Argv) => yargs,
  handler: async () => {
    const { TemporalConfig } = await import("@opencode-ai/temporal/config")
    const config = TemporalConfig.fromEnv()
    UI.println("opencode, temporal execution")
    for (const [name, value] of Object.entries(TemporalConfig.describe(config))) {
      UI.println(`  ${name}: ${value}`)
    }
    for (const note of TemporalConfig.notes(config)) UI.println(`note: ${note}`)
    const problems = TemporalConfig.preflight(config)
    for (const problem of problems) UI.println(`problem: ${problem}`)
    if (problems.length > 0) {
      process.exitCode = 1
      return
    }
    UI.println("this deployment looks consistent")
  },
})
