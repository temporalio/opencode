// A libSQL-backed SqlClient, so the event store can live in a shared/networked SQLite (Turso or a
// self-hosted sqld) that every worker points at -- the basis for any-worker resume across hosts.
// It speaks the same SQLite dialect as the bun/node drivers, so the schema and all migrations are
// unchanged; only the transport differs. `makeDatabase` needs only the generic `SqlClient`, so this
// layer provides just that (no Sqlite.Native / Sqlite.Drizzle).

import { createClient } from "@libsql/client"
import * as Context from "effect/Context"
import * as Effect from "effect/Effect"
import * as Fiber from "effect/Fiber"
import { identity } from "effect/Function"
import * as Layer from "effect/Layer"
import * as Scope from "effect/Scope"
import * as Semaphore from "effect/Semaphore"
import * as Stream from "effect/Stream"
import * as Reactivity from "effect/unstable/reactivity/Reactivity"
import * as Client from "effect/unstable/sql/SqlClient"
import type { Connection } from "effect/unstable/sql/SqlConnection"
import { classifySqliteError, SqlError } from "effect/unstable/sql/SqlError"
import * as Statement from "effect/unstable/sql/Statement"

export interface LibsqlConfig {
  readonly url: string
  readonly authToken?: string
  readonly transformResultNames?: (str: string) => string
  readonly transformQueryNames?: (str: string) => string
}

const make = (options: LibsqlConfig) =>
  Effect.gen(function* () {
    // intMode "number": our columns are text ids + epoch-millis / sequence integers, all well under
    // 2^53, so a JS number is exact and we avoid bigint round-tripping through the SqlClient.
    const native = yield* Effect.acquireRelease(
      Effect.sync(() => createClient({ url: options.url, authToken: options.authToken, intMode: "number" })),
      (client) => Effect.sync(() => client.close()),
    )

    const compiler = Statement.makeCompilerSqlite(options.transformQueryNames)
    const transformRows = options.transformResultNames
      ? Statement.defaultTransforms(options.transformResultNames).array
      : undefined

    const fail = (cause: unknown) =>
      new SqlError({
        reason: classifySqliteError(cause, { message: "Failed to execute statement", operation: "execute" }),
      })

    const run = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.tryPromise({
        try: () =>
          native
            .execute({ sql: query, args: params as never[] })
            .then((r) => r.rows as unknown as Array<Record<string, unknown>>),
        catch: fail,
      })

    const runValues = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.tryPromise({
        try: () =>
          native
            .execute({ sql: query, args: params as never[] })
            .then((r) => r.rows.map((row) => r.columns.map((c) => (row as Record<string, unknown>)[c])) as Array<unknown[]>),
        catch: fail,
      })

    const connection = identity<Connection>({
      execute(query, params, transformRows) {
        return transformRows ? Effect.map(run(query, params), transformRows) : run(query, params)
      },
      executeRaw(query, params) {
        return run(query, params)
      },
      executeValues(query, params) {
        return runValues(query, params)
      },
      executeUnprepared(query, params, transformRows) {
        return this.execute(query, params, transformRows)
      },
      executeStream() {
        return Stream.die("executeStream not implemented")
      },
    })

    const semaphore = yield* Semaphore.make(1)
    const acquirer = semaphore.withPermits(1)(Effect.succeed(connection))
    const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
      const fiber = Fiber.getCurrent()!
      const scope = Context.getUnsafe(fiber.context, Scope.Scope)
      return Effect.as(
        Effect.tap(restore(semaphore.take(1)), () => Scope.addFinalizer(scope, semaphore.release(1))),
        connection,
      )
    })

    return yield* Client.make({
      acquirer,
      compiler,
      transactionAcquirer,
      spanAttributes: [["db.system.name", "sqlite"]],
      transformRows,
    })
  })

export const layer = (config: LibsqlConfig) =>
  Layer.effect(Client.SqlClient, make(config)).pipe(Layer.provide(Reactivity.layer))
