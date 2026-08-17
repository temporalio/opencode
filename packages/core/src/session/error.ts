import { Schema } from "effect"
import { SessionMessage } from "./message"
import { SessionSchema } from "./schema"

export class MessageDecodeError extends Schema.TaggedErrorClass<MessageDecodeError>()("Session.MessageDecodeError", {
  sessionID: SessionSchema.ID,
  messageID: SessionMessage.ID,
}) {
  override get message() {
    return `Failed to decode message ${this.messageID} in session ${this.sessionID}`
  }
}

export class ContextSnapshotDecodeError extends Schema.TaggedErrorClass<ContextSnapshotDecodeError>()(
  "Session.ContextSnapshotDecodeError",
  {
    sessionID: SessionSchema.ID,
    details: Schema.String,
  },
) {
  override get message() {
    return `Failed to decode context snapshot for session ${this.sessionID}: ${this.details}`
  }
}

// The user stopped the run (declined a permission, rejected a question). Distinct from a decode
// failure so callers across the durable boundary can tell a deliberate halt from a fault.
export class SessionRunDeclinedError extends Schema.TaggedErrorClass<SessionRunDeclinedError>()(
  "Session.SessionRunDeclinedError",
  {
    sessionID: SessionSchema.ID,
  },
) {
  override get message() {
    return `Session run halted by the user: ${this.sessionID}`
  }
}
