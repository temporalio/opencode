import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260812000509_permission_request_status_idx",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`CREATE INDEX \`permission_request_status_idx\` ON \`permission_request\` (\`status\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
