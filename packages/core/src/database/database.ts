export * as Database from "./database"

import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { layer as sqliteLayer } from "#sqlite"
import { layer as libsqlLayer } from "./sqlite.libsql"
import { Context, Effect, Layer } from "effect"
import { Global } from "../global"
import { Flag } from "../flag/flag"
import { isAbsolute, join } from "path"
import { DatabaseMigration } from "./migration"
import { InstallationChannel } from "../installation/version"
import { makeGlobalNode } from "../effect/app-node"

const makeDatabase = EffectDrizzleSqlite.makeWithDefaults()
type DatabaseShape = Effect.Success<typeof makeDatabase>

export interface Interface {
  db: DatabaseShape
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/storage/Database") {}

function makeServiceLayer(localPragmas: boolean) {
  return Layer.effect(
    Service,
    Effect.gen(function* () {
      const db = yield* makeDatabase

      // Local-file tuning. A shared/networked store (libSQL) manages journaling itself and may
      // reject these, so they are skipped there; the schema and migrations are identical either way.
      if (localPragmas) {
        yield* db.run("PRAGMA journal_mode = WAL")
        yield* db.run("PRAGMA synchronous = NORMAL")
        yield* db.run("PRAGMA busy_timeout = 5000")
        yield* db.run("PRAGMA cache_size = -64000")
        yield* db.run("PRAGMA foreign_keys = ON")
        yield* db.run("PRAGMA wal_checkpoint(PASSIVE)")
      }
      yield* DatabaseMigration.apply(db)

      return { db }
    }).pipe(Effect.orDie),
  )
}

export function layerFromPath(filename: string) {
  return makeServiceLayer(true).pipe(Layer.provide(sqliteLayer({ filename })))
}

// A shared/durable event store: point every worker at one libSQL URL (a self-hosted sqld or Turso)
// so any worker can resume any session from the same log. Same SQLite dialect, so nothing in the
// schema/migrations/queries changes.
export function layerFromLibsql(url: string, authToken?: string) {
  return makeServiceLayer(false).pipe(Layer.provide(libsqlLayer({ url, authToken })))
}

export function path() {
  if (Flag.OPENCODE_DB) {
    if (Flag.OPENCODE_DB === ":memory:" || isAbsolute(Flag.OPENCODE_DB)) return Flag.OPENCODE_DB
    return join(Global.Path.data, Flag.OPENCODE_DB)
  }
  if (
    ["latest", "beta", "prod"].includes(InstallationChannel) ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "1" ||
    process.env.OPENCODE_DISABLE_CHANNEL_DB === "true"
  )
    return join(Global.Path.data, "opencode.db")
  return join(Global.Path.data, `opencode-${InstallationChannel.replace(/[^a-zA-Z0-9._-]/g, "-")}.db`)
}

export const node = makeGlobalNode({
  service: Service,
  layer: Flag.OPENCODE_DB_URL
    ? layerFromLibsql(Flag.OPENCODE_DB_URL, Flag.OPENCODE_DB_AUTH_TOKEN)
    : layerFromPath(path()),
  deps: [],
})
