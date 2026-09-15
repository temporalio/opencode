import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260817200644_question_request",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`question_request\` (
          \`id\` text PRIMARY KEY,
          \`session_id\` text NOT NULL,
          \`payload\` text NOT NULL,
          \`status\` text DEFAULT 'pending' NOT NULL,
          \`answers\` text,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`question_request_session_status_idx\` ON \`question_request\` (\`session_id\`,\`status\`);`,
      )
      yield* tx.run(`CREATE INDEX \`question_request_status_idx\` ON \`question_request\` (\`status\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
