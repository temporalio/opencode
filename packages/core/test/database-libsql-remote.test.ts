// Networked-libSQL integration check: the same commit/rollback atomicity assertions as
// database-libsql-transaction.test.ts, but against a REAL libSQL server over the network (sqld /
// `turso dev`), where each statement is its own HTTP request and only the interactive-transaction
// routing makes BEGIN..COMMIT atomic. Needs a live server, so it runs only when
// OPENCODE_LIBSQL_TEST_URL is set (e.g. `turso dev --port 8888` then
// OPENCODE_LIBSQL_TEST_URL=http://127.0.0.1:8888 bun test database-libsql-remote`); otherwise the
// suite is skipped, not silently passed.
import { describe, expect, test } from "bun:test"
import { Effect, Exit, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { testEffect } from "./lib/effect"

const url = process.env.OPENCODE_LIBSQL_TEST_URL

const withRemoteDb = <A, E, R>(body: (db: Database.Interface["db"]) => Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    return yield* body(db)
  }).pipe(Effect.provide(Database.layerFromLibsql(url!, process.env.OPENCODE_LIBSQL_TEST_AUTH_TOKEN)))

const it = testEffect(Layer.empty)

describe.skipIf(!url)("Database libSQL backend (remote server)", () => {
  it.live("commits a multi-statement transaction atomically over the network", () =>
    withRemoteDb((db) =>
      Effect.gen(function* () {
        yield* db.run("DROP TABLE IF EXISTS tx_probe_remote")
        yield* db.run("CREATE TABLE tx_probe_remote (id TEXT PRIMARY KEY, n INTEGER NOT NULL)")
        yield* db.transaction(
          () =>
            Effect.gen(function* () {
              yield* db.run("INSERT INTO tx_probe_remote (id, n) VALUES ('a', 1)")
              yield* db.run("INSERT INTO tx_probe_remote (id, n) VALUES ('b', 2)")
            }),
          { behavior: "immediate" },
        )
        const row = yield* db.get<{ c: number }>("SELECT COUNT(*) AS c FROM tx_probe_remote")
        expect(Number(row?.c ?? 0)).toBe(2)
      }),
    ),
  )

  it.live("rolls back every write when a statement fails over the network", () =>
    withRemoteDb((db) =>
      Effect.gen(function* () {
        yield* db.run("DROP TABLE IF EXISTS tx_probe_remote")
        yield* db.run("CREATE TABLE tx_probe_remote (id TEXT PRIMARY KEY, n INTEGER NOT NULL)")
        yield* db.run("INSERT INTO tx_probe_remote (id, n) VALUES ('seed', 0)")
        const exit = yield* db
          .transaction(
            () =>
              Effect.gen(function* () {
                yield* db.run("INSERT INTO tx_probe_remote (id, n) VALUES ('a', 1)")
                yield* Effect.fail(new Error("boom"))
              }),
            { behavior: "immediate" },
          )
          .pipe(Effect.exit)
        expect(Exit.isFailure(exit)).toBe(true)
        const row = yield* db.get<{ c: number }>("SELECT COUNT(*) AS c FROM tx_probe_remote")
        expect(Number(row?.c ?? 0)).toBe(1)
      }),
    ),
  )

  it.live("runs the schema migrations against the remote store", () =>
    withRemoteDb((db) =>
      Effect.gen(function* () {
        // layerFromLibsql already applied migrations on build; spot-check the core tables exist.
        const row = yield* db.get<{ name: string }>(
          "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event'",
        )
        expect(row?.name).toBe("event")
      }),
    ),
  )
})
