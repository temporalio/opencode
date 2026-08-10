// A libSQL-backed SqlClient, so the event store can live in a shared/networked SQLite (Turso or a
// self-hosted sqld) that every worker points at -- the basis for any-worker resume across hosts.
// It speaks the same SQLite dialect as the bun/node drivers, so the schema and all migrations are
// unchanged; only the transport differs. `makeDatabase` needs only the generic `SqlClient`, so this
// layer provides just that (no Sqlite.Native / Sqlite.Drizzle).

import { createClient, type ResultSet, type Transaction } from "@libsql/client"
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

// The transaction-control statements the effect-drizzle layer emits onto the reserved connection.
// A remote libSQL client runs each execute() as an independent auto-commit request, so forwarding
// `begin`/`commit` as plain statements would NOT bind a transaction across the writes in between.
// We intercept them and drive a real interactive libSQL transaction instead (one pinned stream),
// which is atomic all-or-nothing. `rollback to savepoint` is a savepoint op, not a rollback, so it
// must fall through to the transaction.
const BEGIN = /^\s*begin\b/i
const COMMIT = /^\s*(commit|end)\b/i
const ROLLBACK = /^\s*rollback\s*(transaction)?\s*;?\s*$/i
const EMPTY = { rows: [], columns: [] } as unknown as ResultSet

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

    const toRows = (r: ResultSet) => r.rows as unknown as Array<Record<string, unknown>>
    const toValues = (r: ResultSet) =>
      r.rows.map((row) => r.columns.map((c) => (row as Record<string, unknown>)[c])) as Array<unknown[]>

    // Auto-commit connection: each statement is its own request. Used outside transactions.
    const run = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.tryPromise({ try: () => native.execute({ sql: query, args: params as never[] }).then(toRows), catch: fail })
    const runValues = (query: string, params: ReadonlyArray<unknown> = []) =>
      Effect.tryPromise({
        try: () => native.execute({ sql: query, args: params as never[] }).then(toValues),
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

    // A per-transaction connection bound to one interactive libSQL transaction. All statements in
    // the drizzle transaction scope run on the same pinned stream, so the writes commit atomically.
    // We always open in "write" mode: the client gives no read-only hint, and a write transaction is
    // correct for both reads and writes (refining read-only spans to "read" mode is a follow-up).
    const makeTxConnection = () => {
      let tx: Transaction | null = null
      const exec = async (query: string, params: ReadonlyArray<unknown>): Promise<ResultSet> => {
        if (BEGIN.test(query)) {
          tx = await native.transaction("write")
          return EMPTY
        }
        if (COMMIT.test(query)) {
          if (tx) {
            await tx.commit()
            tx = null
          }
          return EMPTY
        }
        if (ROLLBACK.test(query)) {
          if (tx) {
            await tx.rollback()
            tx = null
          }
          return EMPTY
        }
        // Data statements and savepoint ops (savepoint / release / rollback to savepoint) run inside
        // the transaction. This connection is only handed out inside a transaction scope, where the
        // drizzle layer always emits `begin` first, so a statement with no open transaction would
        // mean auto-committing it alone (the non-atomic behavior this backend exists to avoid).
        if (!tx) throw new Error("libSQL: statement on a transaction connection before begin")
        return tx.execute({ sql: query, args: params as never[] })
      }
      const close = async () => {
        // Safety net: if the scope unwinds with the transaction still open (no commit/rollback was
        // emitted), roll it back so the stream is released and nothing partial lingers.
        if (tx) {
          try {
            await tx.rollback()
          } catch {}
          try {
            tx.close()
          } catch {}
          tx = null
        }
      }
      const conn = identity<Connection>({
        execute(query, params, transformRows) {
          const e = Effect.tryPromise({ try: () => exec(query, params).then(toRows), catch: fail })
          return transformRows ? Effect.map(e, transformRows) : e
        },
        executeRaw(query, params) {
          return Effect.tryPromise({ try: () => exec(query, params).then(toRows), catch: fail })
        },
        executeValues(query, params) {
          return Effect.tryPromise({ try: () => exec(query, params).then(toValues), catch: fail })
        },
        executeUnprepared(query, params, transformRows) {
          return this.execute(query, params, transformRows)
        },
        executeStream() {
          return Stream.die("executeStream not implemented")
        },
      })
      return { conn, close }
    }

    const semaphore = yield* Semaphore.make(1)
    const acquirer = semaphore.withPermits(1)(Effect.succeed(connection))
    const transactionAcquirer = Effect.uninterruptibleMask((restore) => {
      const fiber = Fiber.getCurrent()!
      const scope = Context.getUnsafe(fiber.context, Scope.Scope)
      const { conn, close } = makeTxConnection()
      return Effect.as(
        Effect.tap(restore(semaphore.take(1)), () =>
          // Close the transaction (rollback if still open) before releasing the permit, so the next
          // transaction never starts against a half-finished one.
          Scope.addFinalizer(
            scope,
            Effect.gen(function* () {
              yield* Effect.promise(() => close())
              yield* semaphore.release(1)
            }),
          ),
        ),
        conn,
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
