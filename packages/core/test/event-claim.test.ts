// The compare-and-set in `claim`, under the interleaving it exists for.
//
// Two attempts of one activity claim the log from two processes, so both can read the current owner
// before either writes. A pair of claims started together in one process never does that: they run
// to completion one after the other, which is why the concurrent test beside this one passes with
// the fix reverted. The seam here is at the database, not in `claim`: reads of the sequence table
// wait for each other while the barrier is armed, and `claim` itself is untouched.
import { describe, expect } from "bun:test"
import { Deferred, Effect, Exit, Layer } from "effect"
import { eq } from "drizzle-orm"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable } from "@opencode-ai/core/event/sql"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { WorkspaceV2 } from "@opencode-ai/core/workspace"
import { Session } from "@opencode-ai/schema/session"
import { SessionV1 } from "@opencode-ai/schema/session-v1"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const locationLayer = Layer.succeed(
  Location.Service,
  Location.Service.of(
    location({ directory: AbsolutePath.make("project"), workspaceID: WorkspaceV2.ID.make("wrk_test") }),
  ),
)

// While armed, the first `want` reads wait for each other and are then released together.
const barrier = {
  held: 0,
  want: 0,
  gate: undefined as Deferred.Deferred<void> | undefined,
}

const hold = () =>
  Effect.gen(function* () {
    const gate = barrier.gate
    if (!gate || barrier.want === 0) return
    barrier.held++
    if (barrier.held >= barrier.want) {
      barrier.want = 0
      yield* Deferred.succeed(gate, void 0)
      return
    }
    yield* Deferred.await(gate)
  })

// Waits after the read rather than before it. What has to interleave is two claims that both saw
// the same owner; holding before the read would serialize them and prove nothing.
const gated = (db: any): any => {
  const wrap = (node: any): any =>
    new Proxy(node, {
      get(target, prop, recv) {
        const value = Reflect.get(target, prop, recv)
        if (typeof value !== "function") return value
        if (prop === "get" || prop === "all")
          return (...args: any[]) => value.apply(target, args).pipe(Effect.tap(() => hold()))
        return (...args: any[]) => {
          const out = value.apply(target, args)
          return out && typeof out === "object" ? wrap(out) : out
        }
      },
    })
  return new Proxy(db, {
    get(target, prop, recv) {
      const value = Reflect.get(target, prop, recv)
      if (prop !== "select") return typeof value === "function" ? value.bind(target) : value
      return (...args: any[]) => wrap(value.apply(target, args))
    },
  })
}

const gatedDatabase = Layer.effect(
  Database.Service,
  Effect.gen(function* () {
    const real = yield* Database.Service
    return { db: gated(real.db) }
  }),
).pipe(Layer.provide(Database.layerFromPath(":memory:")))

const it = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, EventV2.node, Location.node]), [
    [Location.node, locationLayer],
    [Database.node, gatedDatabase],
  ]),
)

const DurableMessage = SessionV1.Event.MessageRemoved

describe("claim under a real interleaving", () => {
  it.effect("only one of two claims that read the same owner is told it won", () =>
    Effect.gen(function* () {
      const events = yield* EventV2.Service
      const aggregateID = Session.ID.create()
      yield* events.publish(DurableMessage, {
        sessionID: aggregateID,
        messageID: SessionV1.MessageID.ascending("msg_seed"),
      })
      yield* events.claim(aggregateID, "run:11:1")

      barrier.gate = yield* Deferred.make<void>()
      barrier.held = 0
      barrier.want = 2

      const outcomes = yield* Effect.all(
        ["run:11:2", "run:11:3"].map((token) => events.claim(aggregateID, token).pipe(Effect.exit)),
        { concurrency: "unbounded" },
      )
      barrier.gate = undefined
      barrier.want = 0

      const { db } = yield* Database.Service
      const row = yield* db
        .select({ ownerID: EventSequenceTable.owner_id })
        .from(EventSequenceTable)
        .where(eq(EventSequenceTable.aggregate_id, aggregateID))
        .get()

      // Asserted first, because without it the rest proves nothing: it says both claims really did
      // read the same owner before either wrote.
      expect(barrier.held).toBe(2)
      // The one that loses must be told so. Two winners means the loser goes on to publish under a
      // token the log has already fenced, and its tools die on a step that is running.
      expect(outcomes.filter(Exit.isSuccess).length).toBe(1)
      expect(["run:11:2", "run:11:3"]).toContain(row?.ownerID ?? "")
    }),
  )
})
