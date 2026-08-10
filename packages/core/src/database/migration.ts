export * as DatabaseMigration from "./migration"

import { sql } from "drizzle-orm"
import { Effect, Semaphore } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { migrations } from "./migration.gen"
import schema from "./schema.gen"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]
const lock = Semaphore.makeUnsafe(1)

export type Migration = {
  id: string
  up: (tx: Transaction) => Effect.Effect<void, unknown>
}

export function apply(db: Database) {
  // Serialize across processes, not just within one. N cold workers pointing at the same shared
  // store would otherwise race: both create the schema, or both insert the same migration id (the
  // table check was a read outside any lock, a TOCTOU). The process-local semaphore covers
  // same-process concurrency; the single BEGIN IMMEDIATE transaction makes check-and-apply atomic
  // and write-locked, so a concurrent start on another process waits and then observes the
  // migrations already applied and does nothing. The incremental branch mirrors `applyOnly` on the
  // shared transaction (keep the two in sync).
  return lock.withPermit(
    db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const tables = yield* tx.all<{ name: string }>(
            sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
          )
          const hasSession = tables.some((table) => table.name === "session")
          if (!hasSession && tables.length > 0)
            return yield* Effect.die("Database is not empty and has no session table")
          if (!hasSession) {
            yield* schema.up(tx)
            yield* tx.run(
              sql`CREATE TABLE ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
            )
            yield* Effect.forEach(migrations, (migration) =>
              tx.run(
                sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migration.id}, ${Date.now()})`,
              ),
            )
            return
          }
          yield* tx.run(
            sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
          )
          let completed = new Set(
            (yield* tx.all<{ id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
          )
          if (completed.size === 0) {
            if (
              yield* tx.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"__drizzle_migrations"}`)
            ) {
              yield* tx.run(sql`
                INSERT OR IGNORE INTO ${sql.identifier("migration")} (id, time_completed)
                SELECT name, ${Date.now()}
                FROM ${sql.identifier("__drizzle_migrations")}
                WHERE name IS NOT NULL
              `)
              completed = new Set(
                (yield* tx.all<{ id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
              )
            }
          }
          for (const migration of migrations) {
            if (completed.has(migration.id)) continue
            yield* migration.up(tx)
            yield* tx.run(
              sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migration.id}, ${Date.now()})`,
            )
          }
        }),
      { behavior: "immediate" },
    ),
  )
}

export function applyOnly(db: Database, input: Migration[]) {
  return Effect.gen(function* () {
    yield* db.run(
      sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
    )
    let completed = new Set(
      (yield* db.all<{ id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
    )
    if (completed.size === 0) {
      // Existing installs used Drizzle's migration journal. Seed the new
      // journal once so TypeScript migrations don't replay old SQL.
      if (
        yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${"__drizzle_migrations"}`)
      ) {
        yield* db.run(sql`
          INSERT OR IGNORE INTO ${sql.identifier("migration")} (id, time_completed)
          SELECT name, ${Date.now()}
          FROM ${sql.identifier("__drizzle_migrations")}
          WHERE name IS NOT NULL
        `)
        completed = new Set(
          (yield* db.all<{ id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
        )
      }
    }

    for (const migration of input) {
      if (completed.has(migration.id)) continue
      yield* db.transaction((tx) =>
        Effect.gen(function* () {
          yield* migration.up(tx)
          yield* tx.run(
            sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migration.id}, ${Date.now()})`,
          )
        }),
      )
    }
  })
}
