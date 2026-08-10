import { index, sqliteTable, text, uniqueIndex } from "drizzle-orm/sqlite-core"
import { Timestamps } from "../database/schema.sql"
import { ProjectV2 } from "../project"
import { ProjectTable } from "../project/sql"
import type { PermissionSaved } from "./saved"

export const PermissionTable = sqliteTable(
  "permission",
  {
    id: text().$type<PermissionSaved.ID>().primaryKey(),
    project_id: text()
      .$type<ProjectV2.ID>()
      .notNull()
      .references(() => ProjectTable.id, { onDelete: "cascade" }),
    action: text().notNull(),
    resource: text().notNull(),
    ...Timestamps,
  },
  (table) => [uniqueIndex("permission_project_action_resource_idx").on(table.project_id, table.action, table.resource)],
)

// A durable pending-approval record, so an ask raised by one process (a standalone worker) can be
// listed and replied to from another (the HTTP server) via the shared store, and survives the asking
// process. `payload` is the JSON-encoded PermissionV2.Request; status transitions
// pending -> approved | declined | corrected | expired.
export const PermissionRequestTable = sqliteTable(
  "permission_request",
  {
    id: text().primaryKey(),
    session_id: text().notNull(),
    agent: text(),
    payload: text().notNull(),
    status: text().notNull().default("pending"),
    message: text(),
    ...Timestamps,
  },
  (table) => [index("permission_request_session_status_idx").on(table.session_id, table.status)],
)
