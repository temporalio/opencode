// Faithful round-trip of a SessionRunner.RunError across the Temporal boundary. Every member of the
// union is a Schema.TaggedErrorClass, so we can encode the error to JSON in the activity and decode
// it back into the exact tagged instance in the layer, instead of flattening it to a carrier.

import { Schema } from "effect"
import { LLMError } from "@opencode-ai/llm"
import { Integration } from "../../integration"
import { SystemContext } from "../../system-context/index"
import { ToolOutputStore } from "../../tool-output-store"
import { ContextSnapshotDecodeError, MessageDecodeError } from "../error"
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
