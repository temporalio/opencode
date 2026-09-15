// Admitting a prompt to a session that already exists, without waking anything. This is what a
// start with no client is made of: a prompt is a durable row before it is work, and writing that
// row needs the store, which a workflow cannot reach. Waking the session is the workflow's own job.
// Separating the two is what lets a schedule fire into a deployment where nothing runs but workers.

import { Effect } from "effect"
import type { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionInput } from "@opencode-ai/core/session/input"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { Prompt } from "@opencode-ai/schema/prompt"

export interface PromptDrainInput {
  readonly sessionID: string
  /** Derived by the workflow from the firing, so a re-driven activity admits nothing twice. */
  readonly messageID: string
  readonly text: string
}

export const makeScheduleDrains = ({
  db,
  events,
}: {
  readonly db: Database.Interface["db"]
  readonly events: EventV2.Interface
}) => ({
  promptDrain: async (input: PromptDrainInput) =>
    SessionInput.admit(db, events, {
      id: SessionMessage.ID.make(input.messageID),
      sessionID: SessionSchema.ID.make(input.sessionID),
      prompt: Prompt.make({ text: input.text }),
      delivery: "queue",
    }).pipe(
      Effect.asVoid,
      // Admission is control-plane input: it must leave the active drain's ownership unchanged.
      Effect.provideService(EventV2.EventOwner, undefined),
      Effect.scoped,
      Effect.runPromise,
    ),
})
