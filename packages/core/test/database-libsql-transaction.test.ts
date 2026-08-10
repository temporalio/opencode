import { describe, expect } from "bun:test"
import path from "path"
import { Effect, Exit, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { testEffect } from "./lib/effect"
import { tmpdir } from "./fixture/tmpdir"

// The libSQL backend runs each statement as its own request, so a transaction is only atomic if the
// backend pins one interactive libSQL transaction for the BEGIN..COMMIT span. This is the write
// shape the event store uses (event_sequence upsert + event insert must commit together). Exercised
// against an embedded `file:` store; the interactive-tx API is identical for a remote URL, whose
// crash-atomicity still needs a live sqld/Turso to test.
const withLibsqlDb = <A, E, R>(body: (db: Database.Interface["db"]) => Effect.Effect<A, E, R>) =>
  Effect.acquireUseRelease(
    Effect.promise(() => tmpdir()),
    (tmp) =>
      Effect.gen(function* () {
        const { db } = yield* Database.Service
        return yield* body(db)
      }).pipe(Effect.provide(Database.layerFromLibsql(`file:${path.join(tmp.path, "events.db")}`))),
    (tmp) => Effect.promise(() => tmp[Symbol.asyncDispose]()),
  )

const it = testEffect(Layer.empty)

describe("Database libSQL backend", () => {
  it.live("commits a multi-statement transaction atomically", () =>
    withLibsqlDb((db) =>
      Effect.gen(function* () {
        yield* db.run("CREATE TABLE tx_probe (id TEXT PRIMARY KEY, n INTEGER NOT NULL)")
        yield* db.transaction(
          () =>
            Effect.gen(function* () {
              yield* db.run("INSERT INTO tx_probe (id, n) VALUES ('a', 1)")
              yield* db.run("INSERT INTO tx_probe (id, n) VALUES ('b', 2)")
            }),
          { behavior: "immediate" },
        )
        const row = yield* db.get<{ c: number }>("SELECT COUNT(*) AS c FROM tx_probe")
        expect(Number(row?.c ?? 0)).toBe(2)
      }),
    ),
  )

  it.live("rolls back every write when a statement in the transaction fails", () =>
    withLibsqlDb((db) =>
      Effect.gen(function* () {
        yield* db.run("CREATE TABLE tx_probe (id TEXT PRIMARY KEY, n INTEGER NOT NULL)")
        yield* db.run("INSERT INTO tx_probe (id, n) VALUES ('seed', 0)")
        const exit = yield* db
          .transaction(
            () =>
              Effect.gen(function* () {
                yield* db.run("INSERT INTO tx_probe (id, n) VALUES ('a', 1)")
                yield* Effect.fail(new Error("boom"))
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        // The seed survives; the in-transaction insert was rolled back with the failure.
        const row = yield* db.get<{ c: number }>("SELECT COUNT(*) AS c FROM tx_probe")
        expect(Number(row?.c ?? 0)).toBe(1)
      }),
    ),
  )
})
