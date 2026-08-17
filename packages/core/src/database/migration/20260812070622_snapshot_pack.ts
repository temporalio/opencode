import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "20260812070622_snapshot_pack",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`snapshot_pack\` (
          \`id\` text PRIMARY KEY,
          \`directory\` text NOT NULL,
          \`worktree\` text NOT NULL,
          \`tree\` text NOT NULL,
          \`base\` text,
          \`pack\` blob NOT NULL,
          \`time_created\` integer NOT NULL,
          \`time_updated\` integer NOT NULL
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`snapshot_pack_directory_idx\` ON \`snapshot_pack\` (\`directory\`,\`time_created\`);`,
      )
      yield* tx.run(`CREATE INDEX \`snapshot_pack_worktree_idx\` ON \`snapshot_pack\` (\`worktree\`,\`time_created\`);`)
    })
  },
} satisfies DatabaseMigration.Migration
