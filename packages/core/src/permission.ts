export * as PermissionV2 from "./permission"

import { createHash } from "node:crypto"
import { and, eq } from "drizzle-orm"
import { makeLocationNode } from "./effect/app-node"
import { Context, Deferred, Effect as EffectRuntime, Layer, Schema } from "effect"
import { Permission } from "@opencode-ai/schema/permission"
import { Database } from "./database/database"
import { EventV2 } from "./event"
import { Location } from "./location"
import { AgentV2 } from "./agent"
import { SessionV2 } from "./session"
import { SessionStore } from "./session/store"
import { Wildcard } from "./util/wildcard"
import { PermissionSaved } from "./permission/saved"
import { PermissionRequestTable } from "./permission/sql"

export { Effect, Rule, Ruleset } from "@opencode-ai/schema/permission"
const missingAgentPermissions: Permission.Ruleset = [{ action: "*", resource: "*", effect: "deny" }]

export const ID = Permission.ID
export type ID = typeof ID.Type

export const Source = Permission.Source
export type Source = typeof Source.Type

const RequestFields = {
  sessionID: Permission.Request.fields.sessionID,
  action: Permission.Request.fields.action,
  resources: Permission.Request.fields.resources,
  save: Permission.Request.fields.save,
  metadata: Permission.Request.fields.metadata,
  source: Permission.Request.fields.source,
}

export const Request = Permission.Request
export type Request = typeof Request.Type

export const Reply = Permission.Reply
export type Reply = typeof Reply.Type

export const AssertInput = Schema.Struct({
  id: ID.pipe(Schema.optional),
  ...RequestFields,
  agent: AgentV2.ID.pipe(Schema.optional),
}).annotate({ identifier: "PermissionV2.AssertInput" })
export type AssertInput = typeof AssertInput.Type

export const ReplyInput = Schema.Struct({
  requestID: ID,
  reply: Reply,
  message: Schema.String.pipe(Schema.optional),
}).annotate({ identifier: "PermissionV2.ReplyInput" })
export type ReplyInput = typeof ReplyInput.Type

export const AskResult = Schema.Struct({
  id: ID,
  effect: Permission.Effect,
}).annotate({ identifier: "PermissionV2.AskResult" })
export type AskResult = typeof AskResult.Type

export const Event = Permission.Event

export class DeclinedError extends Schema.TaggedErrorClass<DeclinedError>()("PermissionV2.DeclinedError", {}) {}

export class CorrectedError extends Schema.TaggedErrorClass<CorrectedError>()("PermissionV2.CorrectedError", {
  feedback: Schema.String,
}) {}

export class BlockedError extends Schema.TaggedErrorClass<BlockedError>()("PermissionV2.BlockedError", {
  rules: Permission.Ruleset,
}) {}

export class NotFoundError extends Schema.TaggedErrorClass<NotFoundError>()("PermissionV2.NotFoundError", {
  requestID: ID,
}) {}

export type Error = BlockedError | CorrectedError

export function evaluate(action: string, resource: string, ...rulesets: Permission.Ruleset[]): Permission.Rule {
  return (
    rulesets
      .flat()
      .findLast((rule) => Wildcard.match(action, rule.action) && Wildcard.match(resource, rule.resource)) ?? {
      action,
      resource: "*",
      effect: "ask",
    }
  )
}

export function merge(...rulesets: Permission.Ruleset[]): Permission.Ruleset {
  return rulesets.flat()
}

export interface Interface {
  readonly ask: (input: AssertInput) => EffectRuntime.Effect<AskResult, SessionV2.NotFoundError>
  readonly assert: (input: AssertInput) => EffectRuntime.Effect<void, Error | SessionV2.NotFoundError>
  readonly reply: (input: ReplyInput) => EffectRuntime.Effect<void, NotFoundError>
  readonly get: (id: ID) => EffectRuntime.Effect<Request | undefined>
  readonly forSession: (sessionID: SessionV2.ID) => EffectRuntime.Effect<ReadonlyArray<Request>>
  readonly list: () => EffectRuntime.Effect<ReadonlyArray<Request>>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/Permission") {}

interface Pending {
  readonly request: Request
  readonly agent?: AgentV2.ID
  readonly deferred: Deferred.Deferred<void, DeclinedError | CorrectedError>
}

const layer = Layer.effect(
  Service,
  EffectRuntime.gen(function* () {
    const events = yield* EventV2.Service
    const location = yield* Location.Service
    const agents = yield* AgentV2.Service
    const sessions = yield* SessionStore.Service
    const saved = yield* PermissionSaved.Service
    const db = (yield* Database.Service).db
    const pending = new Map<ID, Pending>()

    // Pending asks are durable rows in the shared store, so an ask raised in one process (a
    // standalone worker's activity) is visible and replyable from another (the HTTP server), and a
    // reply lands even after the asking process restarted. The in-memory deferred stays as the
    // same-process fast path; a cross-process reply is observed by polling the row.
    const readRow = (id: string) =>
      db
        .select()
        .from(PermissionRequestTable)
        .where(eq(PermissionRequestTable.id, id))
        .all()
        .pipe(
          EffectRuntime.orDie,
          EffectRuntime.map((rows) => rows[0]),
        )
    const pendingRows = (sessionID?: SessionV2.ID) =>
      db
        .select()
        .from(PermissionRequestTable)
        .where(
          sessionID
            ? and(eq(PermissionRequestTable.session_id, sessionID), eq(PermissionRequestTable.status, "pending"))
            : eq(PermissionRequestTable.status, "pending"),
        )
        .all()
        .pipe(EffectRuntime.orDie)
    const updateRow = (id: string, status: string, message?: string) =>
      db
        .update(PermissionRequestTable)
        .set({ status, message: message ?? null })
        .where(eq(PermissionRequestTable.id, id))
        .run()
        .pipe(EffectRuntime.orDie)
    const decodeRow = (row: { payload: string }) =>
      Schema.decodeUnknownSync(Request)(JSON.parse(row.payload)) as Request

    yield* EffectRuntime.addFinalizer(() =>
      EffectRuntime.forEach(
        pending.values(),
        (item) =>
          // Graceful shutdown: unblock the local waiter and retire the row. The waiting fiber dies
          // with this process either way, so a later reply would have nothing to resume.
          Deferred.fail(item.deferred, new DeclinedError()).pipe(
            EffectRuntime.andThen(updateRow(item.request.id, "expired")),
            EffectRuntime.catch(() => EffectRuntime.void),
          ),
        { discard: true },
      ).pipe(
        EffectRuntime.ensuring(
          EffectRuntime.sync(() => {
            pending.clear()
          }),
        ),
      ),
    )

    const savedRules = EffectRuntime.fnUntraced(function* () {
      return (yield* saved.list({ projectID: location.project.id })).map(
        (item): Permission.Rule => ({ action: item.action, resource: item.resource, effect: "allow" }),
      )
    })

    const configured = EffectRuntime.fn("PermissionV2.configured")(function* (
      sessionID: SessionV2.ID,
      agentID?: AgentV2.ID,
    ) {
      const session = yield* sessions.get(sessionID)
      if (!session) return yield* new SessionV2.NotFoundError({ sessionID })
      const agent = yield* agents.resolve(agentID ?? session.agent)
      return agent?.permissions ?? missingAgentPermissions
    })

    function denied(input: AssertInput, rules: Permission.Ruleset) {
      return input.resources.some((resource) => evaluate(input.action, resource, rules).effect === "deny")
    }

    function relevant(input: AssertInput, rules: Permission.Ruleset) {
      return rules.filter((rule) => Wildcard.match(input.action, rule.action))
    }

    const evaluateInput = EffectRuntime.fnUntraced(function* (input: AssertInput) {
      const rules = yield* configured(input.sessionID, input.agent)
      if (denied(input, rules)) return { effect: "deny" as const, rules }
      const all = [...rules, ...(yield* savedRules())]
      const effects = input.resources.map((resource) => evaluate(input.action, resource, all).effect)
      const effect: Permission.Effect = effects.includes("deny") ? "deny" : effects.includes("ask") ? "ask" : "allow"
      return { effect, rules: all }
    })

    // A tool-originated ask gets a DETERMINISTIC id (session + callID + action + resources), so a
    // re-driven activity resolves to the same durable row instead of filing a duplicate: a reply
    // that landed while the asker was dead is honored on the retry, and a still-pending row is
    // adopted rather than re-asked. Asks without a tool source keep random ids.
    function deterministicID(input: AssertInput) {
      if (!input.source?.callID) return undefined
      const digest = createHash("sha256")
        .update([input.sessionID, input.source.callID, input.action, ...[...input.resources].sort()].join(" "))
        .digest("hex")
      return ID.create(`per_${digest.slice(0, 26)}`)
    }

    function request(input: AssertInput): Request {
      return {
        id: input.id ?? deterministicID(input) ?? ID.create(),
        sessionID: input.sessionID,
        action: input.action,
        resources: input.resources,
        save: input.save,
        metadata: input.metadata,
        source: input.source,
      }
    }

    const create = (request: Request, agent?: AgentV2.ID) =>
      EffectRuntime.uninterruptible(
        EffectRuntime.gen(function* () {
          const deferred = yield* Deferred.make<void, DeclinedError | CorrectedError>()
          const item = { request, agent, deferred }
          if (pending.has(request.id)) return yield* EffectRuntime.die(`Duplicate pending permission ID: ${request.id}`)
          pending.set(request.id, item)
          yield* db
            .insert(PermissionRequestTable)
            .values({
              id: request.id,
              session_id: request.sessionID,
              agent: agent ?? null,
              payload: JSON.stringify(Schema.encodeSync(Request)(request)),
            })
            .onConflictDoNothing()
            .run()
            .pipe(
              EffectRuntime.orDie,
              EffectRuntime.onError(() => EffectRuntime.sync(() => pending.delete(request.id))),
            )
          // A deterministic id can collide with its own expired row (a prior attempt shut down
          // gracefully); revive it so the reply path and pollers see one pending ask again.
          yield* db
            .update(PermissionRequestTable)
            .set({ status: "pending", message: null })
            .where(and(eq(PermissionRequestTable.id, request.id), eq(PermissionRequestTable.status, "expired")))
            .run()
            .pipe(EffectRuntime.orDie)
          yield* events
            .publish(Event.Asked, request)
            .pipe(EffectRuntime.onError(() => EffectRuntime.sync(() => pending.delete(request.id))))
          return item
        }),
      )

    // Observe a cross-process reply: the replying process updates the row, not our deferred.
    const awaitRow = (id: ID): EffectRuntime.Effect<void, DeclinedError | CorrectedError> =>
      EffectRuntime.gen(function* () {
        for (;;) {
          const row = yield* readRow(id)
          if (row && row.status !== "pending") {
            if (row.status === "approved") return
            if (row.status === "corrected") return yield* new CorrectedError({ feedback: row.message ?? "" })
            return yield* new DeclinedError()
          }
          yield* EffectRuntime.sleep(500)
        }
      })

    const ask = EffectRuntime.fn("PermissionV2.ask")(function* (input: AssertInput) {
      const result = yield* evaluateInput(input)
      const value = request(input)
      if (result.effect === "ask") yield* create(value, input.agent)
      return { id: value.id, effect: result.effect }
    })

    const assert = EffectRuntime.fn("PermissionV2.assert")((input: AssertInput) =>
      EffectRuntime.uninterruptibleMask((restore) =>
        EffectRuntime.gen(function* () {
          const result = yield* evaluateInput(input)
          if (result.effect === "deny") {
            return yield* new BlockedError({
              rules: relevant(input, result.rules),
            })
          }
          if (result.effect === "allow") return
          const value = request(input)
          // A deterministic id may already have a settled or in-flight row from a prior attempt of
          // the same call: honor a reply that landed while the asker was dead, and adopt a pending
          // row instead of duplicating the ask.
          const existing = yield* readRow(value.id)
          if (existing) {
            if (existing.status === "approved") return
            if (existing.status === "corrected")
              return yield* new CorrectedError({ feedback: existing.message ?? "" })
            if (existing.status === "declined") return yield* EffectRuntime.die(new DeclinedError())
            // pending or expired: fall through; create adopts (insert no-ops) or revives the row.
          }
          const item = yield* create(value, input.agent)
          return yield* restore(
            EffectRuntime.raceFirst(Deferred.await(item.deferred), awaitRow(item.request.id)),
          ).pipe(
            EffectRuntime.catchTag("PermissionV2.DeclinedError", (error) => EffectRuntime.die(error)),
            EffectRuntime.ensuring(
              EffectRuntime.sync(() => {
                pending.delete(item.request.id)
              }),
            ),
          )
        }),
      ),
    )

    // Complete the local waiter if the ask was raised in this process; a cross-process waiter
    // observes the row update through its poll.
    const settleLocal = (
      id: ID,
      complete: (deferred: Pending["deferred"]) => EffectRuntime.Effect<boolean>,
    ) =>
      EffectRuntime.suspend(() => {
        const item = pending.get(id)
        if (!item) return EffectRuntime.void
        pending.delete(id)
        return EffectRuntime.asVoid(complete(item.deferred))
      })

    // The durable row is the source of truth, so a reply works from any process (the HTTP server
    // replying to an ask raised inside a standalone worker's activity), not just the asking one.
    const reply = EffectRuntime.fn("PermissionV2.reply")((input: ReplyInput) =>
      EffectRuntime.uninterruptible(
        EffectRuntime.gen(function* () {
          const row = yield* readRow(input.requestID)
          if (!row || row.status !== "pending") return yield* new NotFoundError({ requestID: input.requestID })
          const existing = decodeRow(row)
          yield* events.publish(Event.Replied, {
            sessionID: existing.sessionID,
            requestID: existing.id,
            reply: input.reply,
          })

          if (input.reply === "reject") {
            yield* updateRow(existing.id, input.message ? "corrected" : "declined", input.message)
            yield* settleLocal(existing.id, (deferred) =>
              Deferred.fail(
                deferred,
                input.message ? new CorrectedError({ feedback: input.message }) : new DeclinedError(),
              ),
            )
            // A decline cascades to every other pending ask in the session, wherever it was raised.
            for (const other of yield* pendingRows(existing.sessionID)) {
              if (other.id === existing.id) continue
              yield* events.publish(Event.Replied, {
                sessionID: existing.sessionID,
                requestID: ID.make(other.id),
                reply: "reject",
              })
              yield* updateRow(other.id, "declined")
              yield* settleLocal(ID.make(other.id), (deferred) => Deferred.fail(deferred, new DeclinedError()))
            }
            return
          }

          if (input.reply === "always" && existing.save?.length) {
            yield* saved.add({
              projectID: location.project.id,
              action: existing.action,
              resources: existing.save,
            })
          }
          yield* updateRow(existing.id, "approved")
          yield* settleLocal(existing.id, (deferred) => Deferred.succeed(deferred, undefined))
          if (input.reply !== "always" || !existing.save?.length) return

          // An always-rule can retro-approve other pending asks it now covers.
          const rememberedRules = yield* savedRules()
          for (const otherRow of yield* pendingRows()) {
            if (otherRow.id === existing.id) continue
            const other = decodeRow(otherRow)
            const agent = otherRow.agent === null ? undefined : AgentV2.ID.make(otherRow.agent)
            const rules = yield* configured(other.sessionID, agent).pipe(
              EffectRuntime.catchTag("Session.NotFoundError", () => EffectRuntime.succeed(undefined)),
            )
            if (!rules) continue
            if (denied(other, rules)) continue
            const effective = [...rules, ...rememberedRules]
            if (!other.resources.every((resource) => evaluate(other.action, resource, effective).effect === "allow"))
              continue
            yield* events.publish(Event.Replied, {
              sessionID: other.sessionID,
              requestID: other.id,
              reply: "always",
            })
            yield* updateRow(other.id, "approved")
            yield* settleLocal(other.id, (deferred) => Deferred.succeed(deferred, undefined))
          }
        }),
      ),
    )

    // Reads come from the durable rows, so serve can list and inspect asks raised by any worker.
    const list = EffectRuntime.fn("PermissionV2.list")(function* () {
      return (yield* pendingRows()).map(decodeRow)
    })

    const get = EffectRuntime.fn("PermissionV2.get")(function* (id: ID) {
      const row = yield* readRow(id)
      return row && row.status === "pending" ? decodeRow(row) : undefined
    })

    const forSession = EffectRuntime.fn("PermissionV2.forSession")(function* (sessionID: SessionV2.ID) {
      return (yield* pendingRows(sessionID)).map(decodeRow)
    })

    return Service.of({ ask, assert, reply, get, forSession, list })
  }),
)

export const locationLayer = layer.pipe(Layer.provideMerge(AgentV2.locationLayer))

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [EventV2.node, Location.node, AgentV2.node, SessionStore.node, PermissionSaved.node, Database.node],
})
