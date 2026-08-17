// Durable question asks: a pending question is a row in the shared store, so an ask raised by one
// process (a standalone worker's activity) can be listed and answered from another (the HTTP
// server), and the blocked ask observes the cross-process answer by polling the row. Two fully
// independent service stacks share one DB file to simulate the two processes.
import { describe, expect } from "bun:test"
import path from "path"
import { createClient } from "@libsql/client"
import { Cause, Context, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Database } from "@opencode-ai/core/database/database"
import { QuestionV2 } from "@opencode-ai/core/question"
import { SessionV2 } from "@opencode-ai/core/session"
import { testEffect } from "./lib/effect"
import { tmpdir } from "./fixture/tmpdir"

const sessionID = SessionV2.ID.make("ses_question_durable")
const question: QuestionV2.Info = {
  question: "Which option?",
  header: "Option",
  options: [{ label: "One", description: "First option" }],
}

const stack = (file: string) =>
  AppNodeBuilder.build(QuestionV2.node, [[Database.node, Database.layerFromPath(file)]])

const it = testEffect(Layer.empty)

const awaitAsk = (service: QuestionV2.Interface) =>
  Effect.gen(function* () {
    for (;;) {
      const asks = yield* service.list()
      const ask = asks[0]
      if (ask) return ask
      yield* Effect.sleep(50)
    }
  })

describe("QuestionV2 durable asks", () => {
  it.live("unblocks an ask via an answer from a second process sharing the store", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const questionA = Context.get(A, QuestionV2.Service)
      const questionB = Context.get(B, QuestionV2.Service)

      // A: a tool blocks on the questions. B: a different process sees the durable ask and answers.
      const blocked = yield* questionA.ask({ sessionID, questions: [question] }).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(questionB)
      expect(ask.sessionID).toBe(sessionID)
      expect(ask.questions).toEqual([question])
      yield* questionB.reply({ requestID: ask.id, answers: [["One"]] })
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toEqual([["One"]])
      // The row is settled everywhere: no pending asks remain on either side.
      expect(yield* questionA.list()).toEqual([])
      expect(yield* questionB.list()).toEqual([])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("delivers a cross-process rejection as the typed RejectedError", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const questionA = Context.get(A, QuestionV2.Service)
      const questionB = Context.get(B, QuestionV2.Service)

      const blocked = yield* questionA.ask({ sessionID, questions: [question] }).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(questionB)
      yield* questionB.reject(ask.id)
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isFailure(exit)).toBe(true)
      const error = Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined
      expect(error).toBeInstanceOf(QuestionV2.RejectedError)
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("a re-drive adopts the same pending ask instead of duplicating it", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const questionA = Context.get(A, QuestionV2.Service)
      const questionB = Context.get(B, QuestionV2.Service)
      const input: QuestionV2.AskInput = {
        sessionID,
        questions: [question],
        tool: { messageID: "msg_1", callID: "call_redrive" },
      }

      // Attempt 1 blocks, then dies (a crashed activity): the row stays pending.
      const first = yield* questionA.ask(input).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(questionB)
      yield* Fiber.interrupt(first)
      // Attempt 2 (the Temporal retry) files the same deterministic ask: one row, same id.
      const second = yield* questionA.ask(input).pipe(Effect.forkChild)
      yield* Effect.sleep(100)
      const asks = yield* questionB.list()
      expect(asks).toHaveLength(1)
      expect(asks[0]?.id).toBe(ask.id)
      yield* questionB.reply({ requestID: ask.id, answers: [["One"]] })
      const exit = yield* Fiber.await(second)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toEqual([["One"]])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("honors answers that landed while the asker was dead", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      const A = yield* Layer.build(stack(file))
      const B = yield* Layer.build(stack(file))
      const questionA = Context.get(A, QuestionV2.Service)
      const questionB = Context.get(B, QuestionV2.Service)
      const input: QuestionV2.AskInput = {
        sessionID,
        questions: [question],
        tool: { messageID: "msg_2", callID: "call_dead_asker" },
      }

      const first = yield* questionA.ask(input).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(questionB)
      yield* Fiber.interrupt(first)
      // The human answers after the asker died; the retry short-circuits on the answered row.
      yield* questionB.reply({ requestID: ask.id, answers: [["One"]] })
      expect(yield* questionA.ask(input)).toEqual([["One"]])
      expect(yield* questionB.list()).toEqual([])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("shutdown honors an answer that landed before the poll saw it", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      const B = yield* Layer.build(stack(file))
      const questionB = Context.get(B, QuestionV2.Service)
      // A lives in its own scope so the test can shut it down while the waiter is parked.
      const scope = yield* Scope.make()
      const A = yield* Layer.build(stack(file)).pipe(Effect.provideService(Scope.Scope, scope))
      const questionA = Context.get(A, QuestionV2.Service)
      const blocked = yield* questionA.ask({ sessionID, questions: [question] }).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(questionB)
      yield* questionB.reply({ requestID: ask.id, answers: [["One"]] })
      // Shut A down inside the poll window: the finalizer must honor the answer on the row, not
      // reject a question the user already answered.
      yield* Scope.close(scope, Exit.void)
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit)) expect(exit.value).toEqual([["One"]])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("sweeps an abandoned pending ask out of the list on read", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      const A = yield* Layer.build(stack(file))
      const questionA = Context.get(A, QuestionV2.Service)
      const blocked = yield* questionA.ask({ sessionID, questions: [question] }).pipe(Effect.forkChild)
      const ask = yield* awaitAsk(questionA)
      // Backdate the row past the TTL: the turn that raised it is gone.
      yield* Effect.promise(async () => {
        const raw = createClient({ url: `file:${file}` })
        await raw.execute({
          sql: "UPDATE question_request SET time_updated = ? WHERE id = ?",
          args: [Date.now() - 25 * 60 * 60 * 1000, ask.id],
        })
        raw.close()
      })
      expect(yield* questionA.list()).toEqual([])
      yield* Fiber.interrupt(blocked)
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("expires locally-pending asks on shutdown so they do not linger as pending rows", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(tmp.path, "shared.db")
      // A raises an ask, then its scope closes (a graceful shutdown) before anyone answers.
      const scope = yield* Scope.make()
      const A = yield* Layer.build(stack(file)).pipe(Effect.provideService(Scope.Scope, scope))
      const questionA = Context.get(A, QuestionV2.Service)
      const blocked = yield* questionA.ask({ sessionID, questions: [question] }).pipe(Effect.forkChild)
      yield* awaitAsk(questionA)
      yield* Scope.close(scope, Exit.void)
      const exit = yield* Fiber.await(blocked)
      expect(Exit.isFailure(exit)).toBe(true)
      const B = yield* Layer.build(stack(file))
      const questionB = Context.get(B, QuestionV2.Service)
      // The waiter died with A, so the ask is retired, not stuck pending forever.
      expect(yield* questionB.list()).toEqual([])
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )
})
