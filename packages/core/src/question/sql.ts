import { index, sqliteTable, text } from "drizzle-orm/sqlite-core"
import { Timestamps } from "../database/schema.sql"

// A durable pending-question record, so an ask raised by one process (a standalone worker's
// activity) can be listed and answered from another (the HTTP server) via the shared store, and
// survives the asking process. `payload` is the JSON-encoded QuestionV2.Request; `answers` holds
// the JSON-encoded reply once one lands; status transitions pending -> answered | rejected | expired.
export const QuestionRequestTable = sqliteTable(
  "question_request",
  {
    id: text().primaryKey(),
    session_id: text().notNull(),
    payload: text().notNull(),
    status: text().notNull().default("pending"),
    answers: text(),
    ...Timestamps,
  },
  (table) => [
    index("question_request_session_status_idx").on(table.session_id, table.status),
    // The no-session list() and the TTL sweep both filter by status alone.
    index("question_request_status_idx").on(table.status),
  ],
)
