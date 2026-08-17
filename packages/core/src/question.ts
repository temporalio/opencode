export * as QuestionV2 from "./question"

import { createHash } from "node:crypto"
import { and, eq } from "drizzle-orm"
import { makeLocationNode } from "./effect/app-node"
import { Context, Deferred, Effect, Layer, Schema } from "effect"
import { Question } from "@opencode-ai/schema/question"
import { Database } from "./database/database"
import { EventV2 } from "./event"
import { QuestionRequestTable } from "./question/sql"
import { SessionSchema } from "./session/schema"

export const ID = Question.ID
export type ID = typeof ID.Type

export const Option = Question.Option
export type Option = typeof Option.Type

export const Info = Question.Info
export type Info = typeof Info.Type

export const Prompt = Question.Prompt
export type Prompt = typeof Prompt.Type

export const Tool = Question.Tool
export type Tool = typeof Tool.Type

export const Request = Question.Request
export type Request = typeof Request.Type

export const Answer = Question.Answer
export type Answer = typeof Answer.Type

export const Reply = Question.Reply
export type Reply = typeof Reply.Type

export const Event = Question.Event

// A pending question older than this is treated as abandoned (the turn that raised it was
// interrupted or crashed, so nothing is waiting for the answer). Long enough that a human
// deliberating never trips it.
const PENDING_TTL_MS = 24 * 60 * 60 * 1000

const Answers = Schema.Array(Question.Answer)

export class RejectedError extends Schema.TaggedErrorClass<RejectedError>()("QuestionV2.RejectedError", {}) {
  override get message() {
    return "The user dismissed this question"
  }
}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("QuestionV2.NotFoundError", {
  requestID: ID,
}) {}

export interface AskInput {
  readonly sessionID: SessionSchema.ID
  readonly questions: ReadonlyArray<Info>
  readonly tool?: Tool
}

export interface ReplyInput {
  readonly requestID: ID
  readonly answers: ReadonlyArray<Answer>
}

export interface Interface {
  readonly ask: (input: AskInput) => Effect.Effect<ReadonlyArray<Answer>, RejectedError>
  readonly reply: (input: ReplyInput) => Effect.Effect<void, NotFoundError>
  readonly reject: (requestID: ID) => Effect.Effect<void, NotFoundError>
  readonly list: () => Effect.Effect<ReadonlyArray<Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/Question") {}

interface Pending {
  readonly request: Request
  readonly deferred: Deferred.Deferred<ReadonlyArray<Answer>, RejectedError>
}

// A tool-originated ask gets a DETERMINISTIC id (session + callID + question texts), so a re-driven
// activity resolves to the same durable row instead of filing a duplicate: answers that landed
// while the asker was dead are honored on the retry, and a still-pending row is adopted rather
// than re-asked. Asks without a tool source keep random ids.
function deterministicID(input: AskInput) {
  if (!input.tool?.callID) return undefined
  const digest = createHash("sha256")
    .update([input.sessionID, input.tool.callID, ...input.questions.map((item) => item.question)].join("\u0000"))
    .digest("hex")
  return ID.ascending(`que_${digest.slice(0, 26)}`)
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const db = (yield* Database.Service).db
    const pending = new Map<ID, Pending>()

    // Pending questions are durable rows in the shared store, so an ask raised in one process (a
    // standalone worker's activity) is visible and answerable from another (the HTTP server), and
    // an answer lands even after the asking process restarted. The in-memory deferred stays as the
    // same-process fast path; a cross-process answer is observed by polling the row.
    const readRow = (id: string) =>
      db
        .select()
        .from(QuestionRequestTable)
        .where(eq(QuestionRequestTable.id, id))
        .all()
        .pipe(
          Effect.orDie,
          Effect.map((rows) => rows[0]),
        )
    // Status moves are compare-and-set on the stated `from`, so a reply that lost a race, or a
    // shutdown racing an answer, cannot overwrite a landed outcome.
    const transitionRow = (id: string, from: string, to: string, answers?: string) =>
      db
        .update(QuestionRequestTable)
        .set({ status: to, answers: answers ?? null })
        .where(and(eq(QuestionRequestTable.id, id), eq(QuestionRequestTable.status, from)))
        .run()
        .pipe(Effect.orDie)
    // An interrupted turn leaves its questions pending with nothing waiting for the answers; a row
    // untouched past the TTL is treated as abandoned. Reads sweep them lazily.
    const pendingRows = () =>
      Effect.gen(function* () {
        const cutoff = Date.now() - PENDING_TTL_MS
        const rows = yield* db
          .select()
          .from(QuestionRequestTable)
          .where(eq(QuestionRequestTable.status, "pending"))
          .all()
          .pipe(Effect.orDie)
        const fresh: typeof rows = []
        for (const row of rows) {
          if (row.time_updated < cutoff) yield* transitionRow(row.id, "pending", "expired")
          else fresh.push(row)
        }
        return fresh
      })
    const decodeRow = (row: { payload: string }) =>
      Schema.decodeUnknownSync(Request)(JSON.parse(row.payload)) as Request
    const decodeAnswers = (raw: string | null): ReadonlyArray<Answer> =>
      raw ? (Schema.decodeUnknownSync(Answers)(JSON.parse(raw)) as ReadonlyArray<Answer>) : []

    yield* Effect.addFinalizer(() =>
      Effect.forEach(
        pending.values(),
        (item) =>
          // Graceful shutdown: an answer may have landed on the row before the local poll saw it.
          // Honor it, or the shutdown rejects a question the user already answered. Only a row that
          // is still pending gets retired.
          Effect.gen(function* () {
            const row = yield* readRow(item.request.id)
            if (row?.status === "answered") {
              yield* Deferred.succeed(item.deferred, decodeAnswers(row.answers))
              return
            }
            yield* transitionRow(item.request.id, "pending", "expired")
            yield* Deferred.fail(item.deferred, new RejectedError())
          }).pipe(Effect.catch(() => Effect.void)),
        { discard: true },
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            pending.clear()
          }),
        ),
      ),
    )

    const create = (request: Request) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          // A retry can land in this process while the prior attempt's waiter is still parked
          // (deterministic ids make them the same ask). Share the waiter instead of dying.
          const parked = pending.get(request.id)
          if (parked) return parked
          const deferred = yield* Deferred.make<ReadonlyArray<Answer>, RejectedError>()
          const item: Pending = { request, deferred }
          pending.set(request.id, item)
          yield* db
            .insert(QuestionRequestTable)
            .values({
              id: request.id,
              session_id: request.sessionID,
              payload: JSON.stringify(Schema.encodeSync(Request)(request)),
            })
            .onConflictDoNothing()
            .run()
            .pipe(
              Effect.orDie,
              Effect.onError(() => Effect.sync(() => pending.delete(request.id))),
            )
          // A deterministic id can collide with its own expired row (a prior attempt shut down
          // gracefully); revive it so the reply path and pollers see one pending ask again.
          yield* db
            .update(QuestionRequestTable)
            .set({ status: "pending", answers: null })
            .where(and(eq(QuestionRequestTable.id, request.id), eq(QuestionRequestTable.status, "expired")))
            .run()
            .pipe(Effect.orDie)
          yield* events
            .publish(Event.Asked, request)
            .pipe(Effect.onError(() => Effect.sync(() => pending.delete(request.id))))
          return item
        }),
      )

    // Observe a cross-process answer: the replying process updates the row, not our deferred.
    const awaitRow = (id: ID): Effect.Effect<ReadonlyArray<Answer>, RejectedError> =>
      Effect.gen(function* () {
        for (;;) {
          const row = yield* readRow(id)
          if (row && row.status !== "pending") {
            if (row.status === "answered") return decodeAnswers(row.answers)
            return yield* new RejectedError()
          }
          yield* Effect.sleep(500)
        }
      })

    const ask = Effect.fn("QuestionV2.ask")((input: AskInput) =>
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const request: Request = { id: deterministicID(input) ?? ID.ascending(), ...input }
          // A deterministic id may already have a settled or in-flight row from a prior attempt of
          // the same call: honor answers that landed while the asker was dead, and adopt a pending
          // row instead of duplicating the ask.
          const existing = yield* readRow(request.id)
          if (existing) {
            if (existing.status === "answered") return decodeAnswers(existing.answers)
            if (existing.status === "rejected") return yield* new RejectedError()
            // pending or expired: fall through; create adopts (insert no-ops) or revives the row.
          }
          const item = yield* create(request)
          return yield* restore(Effect.raceFirst(Deferred.await(item.deferred), awaitRow(item.request.id))).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                pending.delete(item.request.id)
              }),
            ),
          )
        }),
      ),
    )

    // Complete the local waiter if the ask was raised in this process; a cross-process waiter
    // observes the row update through its poll.
    const settleLocal = (id: ID, complete: (deferred: Pending["deferred"]) => Effect.Effect<boolean>) =>
      Effect.suspend(() => {
        const item = pending.get(id)
        if (!item) return Effect.void
        pending.delete(id)
        return Effect.asVoid(complete(item.deferred))
      })

    // The durable row is the source of truth, so a reply works from any process (the HTTP server
    // answering a question raised inside a standalone worker's activity), not just the asking one.
    const reply = Effect.fn("QuestionV2.reply")((input: ReplyInput) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const row = yield* readRow(input.requestID)
          if (!row || row.status !== "pending") return yield* new NotFoundError({ requestID: input.requestID })
          const existing = decodeRow(row)
          yield* events.publish(Event.Replied, {
            sessionID: existing.sessionID,
            requestID: existing.id,
            answers: input.answers.map((answer) => [...answer]),
          })
          yield* transitionRow(
            existing.id,
            "pending",
            "answered",
            JSON.stringify(Schema.encodeSync(Answers)(input.answers)),
          )
          yield* settleLocal(existing.id, (deferred) => Deferred.succeed(deferred, input.answers))
        }),
      ),
    )

    const reject = Effect.fn("QuestionV2.reject")((requestID: ID) =>
      Effect.uninterruptible(
        Effect.gen(function* () {
          const row = yield* readRow(requestID)
          if (!row || row.status !== "pending") return yield* new NotFoundError({ requestID })
          const existing = decodeRow(row)
          yield* events.publish(Event.Rejected, {
            sessionID: existing.sessionID,
            requestID: existing.id,
          })
          yield* transitionRow(existing.id, "pending", "rejected")
          yield* settleLocal(existing.id, (deferred) => Deferred.fail(deferred, new RejectedError()))
        }),
      ),
    )

    // Reads come from the durable rows, so serve can list questions raised by any worker.
    const list = Effect.fn("QuestionV2.list")(function* () {
      return (yield* pendingRows()).map(decodeRow)
    })

    return Service.of({ ask, reply, reject, list })
  }),
)

export const locationLayer = layer

export const node = makeLocationNode({ service: Service, layer, deps: [EventV2.node, Database.node] })
