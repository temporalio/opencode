// Durable permission asks: a pending approval is a row in the shared store, so an ask raised by one
// process (a standalone worker's activity) can be listed and replied to from another (the HTTP
// server), and the blocked assert observes the cross-process reply by polling the row. Two fully
// independent service stacks share one DB file to simulate the two processes.
import { describe, expect } from "bun:test"
import path from "path"
import { Cause, Context, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { AgentV2 } from "@opencode-ai/core/agent"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Database } from "@opencode-ai/core/database/database"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Location } from "@opencode-ai/core/location"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionV2 } from "@opencode-ai/core/session"
import { testEffect } from "./lib/effect"
import { tmpdir } from "./fixture/tmpdir"

const sessionID = SessionV2.ID.make("ses_permission_durable")
const agent = AgentV2.ID.make("build")

// Every action evaluates to "ask", so assert always parks on an approval.
const askAll = Layer.mock(AgentV2.Service, {
  resolve: () =>
    Effect.succeed({ permissions: [{ action: "*", resource: "*", effect: "ask" }] } as unknown as AgentV2.Info),
})

const stack = (file: string) =>
  AppNodeBuilder.build(PermissionV2.node, [
    [Database.node, Database.layerFromPath(file)],
    [AgentV2.node, askAll],
    [Location.node, Location.boundNode({ directory: AbsolutePath.make("/project") })],
  ])

const seed = (file: string) =>
  Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: "t",
        directory: "/project",
        title: "t",
        version: "t",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
  }).pipe(Effect.provide(Database.layerFromPath(file)), Effect.scoped)

const it = testEffect(Layer.empty)

const awaitAsk = (perm: PermissionV2.Interface) =>
  Effect.gen(function* () {
    for (;;) {
      const asks = yield* perm.list()
      const ask = asks[0]
      if (ask) return ask
      yield* Effect.sleep(50)
    }
  })

describe("PermissionV2 durable asks", () => {
  it.live("unblocks an assert via a reply from a second process sharing the store", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const permA = Context.get(A, PermissionV2.Service)
      const permB = Context.get(B, PermissionV2.Service)

      // A: a tool blocks on approval. B: a different process sees the durable ask and approves it.
      const blocked = yield* permA
        .assert({ sessionID, action: "bash", resources: ["echo hi"], agent })
        .pipe(Effect.forkChild)
      const ask = yield* awaitAsk(permB)
      expect(ask.action).toBe("bash")
      expect(ask.resources).toEqual(["echo hi"])
      yield* permB.reply({ requestID: ask.id, reply: "once" })
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isSuccess(exit)).toBe(true)
      // The row is settled everywhere: no pending asks remain on either side.
      expect(yield* permA.list()).toEqual([])
      expect(yield* permB.list()).toEqual([])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("delivers a cross-process correction as the typed CorrectedError", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const permA = Context.get(A, PermissionV2.Service)
      const permB = Context.get(B, PermissionV2.Service)

      const blocked = yield* permA
        .assert({ sessionID, action: "bash", resources: ["rm -rf /"], agent })
        .pipe(Effect.forkChild)
      const ask = yield* awaitAsk(permB)
      yield* permB.reply({ requestID: ask.id, reply: "reject", message: "Use a scoped path instead." })
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isFailure(exit)).toBe(true)
      const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
      expect(error).toBeInstanceOf(PermissionV2.CorrectedError)
      expect((error as PermissionV2.CorrectedError).feedback).toBe("Use a scoped path instead.")
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("a re-drive adopts the same pending ask instead of duplicating it", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const permA = Context.get(A, PermissionV2.Service)
      const permB = Context.get(B, PermissionV2.Service)
      const input = {
        sessionID,
        action: "bash",
        resources: ["git push"],
        agent,
        source: { type: "tool" as const, messageID: "msg_1", callID: "call_redrive" },
      }

      // Attempt 1 blocks, then dies (a crashed activity): the row stays pending.
      const first = yield* permA.assert(input).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(permB)
      yield* Fiber.interrupt(first)
      // Attempt 2 (the Temporal retry) files the same deterministic ask: one row, same id.
      const second = yield* permA.assert(input).pipe(Effect.forkChild)
      yield* Effect.sleep(100)
      const asks = yield* permB.list()
      expect(asks).toHaveLength(1)
      expect(asks[0]?.id).toBe(ask.id)
      yield* permB.reply({ requestID: ask.id, reply: "once" })
      const exit = yield* Fiber.await(second)
      expect(Exit.isSuccess(exit)).toBe(true)
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("honors an approval that landed while the asker was dead", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const permA = Context.get(A, PermissionV2.Service)
      const permB = Context.get(B, PermissionV2.Service)
      const input = {
        sessionID,
        action: "bash",
        resources: ["make deploy"],
        agent,
        source: { type: "tool" as const, messageID: "msg_2", callID: "call_dead_asker" },
      }

      const first = yield* permA.assert(input).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(permB)
      yield* Fiber.interrupt(first)
      // The human approves after the asker died; the retry short-circuits on the approved row.
      yield* permB.reply({ requestID: ask.id, reply: "once" })
      yield* permA.assert(input)
      expect(yield* permB.list()).toEqual([])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("shutdown honors an approval that landed before the poll saw it", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      const B = yield* Layer.build(stack(file))
      const permB = Context.get(B, PermissionV2.Service)
      // A lives in its own scope so the test can shut it down while the waiter is parked.
      const scope = yield* Scope.make()
      const A = yield* Layer.build(stack(file)).pipe(Effect.provideService(Scope.Scope, scope))
      const permA = Context.get(A, PermissionV2.Service)
      const blocked = yield* permA
        .assert({ sessionID, action: "bash", resources: ["echo hi"], agent })
        .pipe(Effect.forkChild)
      const ask = yield* awaitAsk(permB)
      yield* permB.reply({ requestID: ask.id, reply: "once" })
      // Shut A down inside the poll window: the finalizer must honor the approval on the row,
      // not record a decline the user never made.
      yield* Scope.close(scope, Exit.void)
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isSuccess(exit)).toBe(true)
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("a same-process retry shares the parked waiter instead of dying", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const permA = Context.get(A, PermissionV2.Service)
      const permB = Context.get(B, PermissionV2.Service)
      const input = {
        sessionID,
        action: "bash",
        resources: ["make it"],
        agent,
        source: { type: "tool" as const, messageID: "msg_3", callID: "call_shared" },
      }
      const first = yield* permA.assert(input).pipe(Effect.forkChild)
      yield* Effect.sleep(100)
      const second = yield* permA.assert(input).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(permB)
      expect(yield* permB.list()).toHaveLength(1)
      yield* permB.reply({ requestID: ask.id, reply: "once" })
      const exits = [yield* Fiber.await(first), yield* Fiber.await(second)]
      expect(exits.every(Exit.isSuccess)).toBe(true)
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("expires locally-pending asks on shutdown so they do not linger as pending rows", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      yield* seed(file)
      let askID: PermissionV2.ID | undefined
      // A raises an ask, then its scope closes (a graceful shutdown) before anyone replies.
      yield* Effect.scoped(
        Effect.gen(function* () {
          const A = yield* Layer.build(stack(file))
          const permA = Context.get(A, PermissionV2.Service)
          const result = yield* permA.ask({ sessionID, action: "bash", resources: ["echo bye"], agent })
          askID = result.id
        }),
      )
      const B = yield* Layer.build(stack(file))
      const permB = Context.get(B, PermissionV2.Service)
      expect(askID).toBeDefined()
      // The waiter died with A, so the ask is retired, not stuck pending forever.
      expect(yield* permB.get(askID!)).toBeUndefined()
      expect(yield* permB.list()).toEqual([])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )
})
