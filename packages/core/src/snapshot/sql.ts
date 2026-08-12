import { blob, index, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Timestamps } from "../database/schema.sql"

// A captured snapshot tree shipped as a git pack, so a worker on another host can rebuild the
// project worktree from the shared store before it drains a session. `id` is the sync commit that
// wraps the tree; `base` is the sync commit the pack was built against (null means a full pack).
// `directory` is the session location the capture ran in (the materializer's lookup key);
// `worktree` is the project root the tree checks out into.
export const SnapshotPackTable = sqliteTable(
  "snapshot_pack",
  {
    id: text().primaryKey(),
    directory: text().notNull(),
    worktree: text().notNull(),
    tree: text().notNull(),
    base: text(),
    pack: blob({ mode: "buffer" }).notNull(),
    ...Timestamps,
  },
  (table) => [
    index("snapshot_pack_directory_idx").on(table.directory, table.time_created),
    index("snapshot_pack_worktree_idx").on(table.worktree, table.time_created),
  ],
)
