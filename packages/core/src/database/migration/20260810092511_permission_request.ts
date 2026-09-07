import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260810092511_permission_request",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`permission_request\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`agent\` text,
          \`payload\` text NOT NULL,
          \`status\` text DEFAULT 'pending' NOT NULL,
          \`message\` text,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`permission_request_session_status_idx\` ON \`permission_request\` (\`session_id\`,\`status\`);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration
