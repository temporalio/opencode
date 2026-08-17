// Faithful round-trip of a SessionRunner.RunError across the Temporal boundary. Every member of the
// union is a Schema.TaggedErrorClass, so we can encode the error to JSON in the activity and decode
// it back into the exact tagged instance in the layer, instead of flattening it to a carrier.

import { Schema } from "effect"
import { LLMError } from "@opencode-ai/llm"
import { Integration } from "../../integration"
import { SystemContext } from "../../system-context/index"
import { ToolOutputStore } from "../../tool-output-store"
import type { SessionSchema } from "../schema"
import { ContextSnapshotDecodeError, MessageDecodeError, SessionRunDeclinedError } from "../error"
import {
  ModelNotSelectedError,
  ModelUnavailableError,
  UnsupportedApiError,
  VariantUnavailableError,
} from "../runner/model"
import type { SessionRunner } from "../runner"

const RunErrorSchema = Schema.Union([
  LLMError,
  ModelNotSelectedError,
  ModelUnavailableError,
  VariantUnavailableError,
  UnsupportedApiError,
  Integration.AuthorizationError,
  MessageDecodeError,
  ContextSnapshotDecodeError,
  SessionRunDeclinedError,
  SystemContext.InitializationBlocked,
  ToolOutputStore.StorageError,
])

const encode = Schema.encodeSync(RunErrorSchema)
const decode = Schema.decodeUnknownSync(RunErrorSchema)

// Returns the JSON encoding of a RunError, or undefined if the value is not a known member (e.g. a
// defect); the caller then falls back to a plain message.
export function encodeRunError(error: unknown): unknown | undefined {
  try {
    return (encode as (e: unknown) => unknown)(error)
  } catch {
    return undefined
  }
}

// Reconstructs the exact tagged RunError from its JSON encoding, or undefined if it does not decode.
export function decodeRunError(payload: unknown): SessionRunner.RunError | undefined {
  try {
    return decode(payload) as SessionRunner.RunError
  } catch {
    return undefined
  }
}

// Walk a failure chain (WorkflowUpdateFailedError -> ActivityFailure -> ApplicationFailure, or a
// bare ApplicationFailure from the in-process driver) to the encoded run error the drain attached,
// and reconstruct the exact tagged error. Falls back to a ContextSnapshotDecodeError carrying the
// text, since the RunError union has no generic member.
export function toRunError(sessionID: SessionSchema.ID, e: unknown): SessionRunner.RunError {
  let node = e as { details?: unknown; cause?: unknown; message?: string } | undefined
  for (let depth = 0; node && depth < 6; depth++) {
    if (Array.isArray(node.details) && node.details.length > 0) {
      const decoded = decodeRunError(node.details[0])
      if (decoded) return decoded
      return new ContextSnapshotDecodeError({ sessionID, details: `session run failed: ${node.message}` })
    }
    node = node.cause as typeof node
  }
  return new ContextSnapshotDecodeError({
    sessionID,
    details: `session run failed: ${(e as { message?: string })?.message ?? String(e)}`,
  })
}
