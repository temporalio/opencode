// The marker a tool call writes is the only record on a host of a call still inside its own
// execution, and every refusal the worktree materializer makes is built on it being there. The store
// side has its own tests; this one drives the real drain, because the three lines that bracket the
// tool body are what put a marker on the host at all.
//
// Take the bracket out of `toolCallDrain` and the first assertion fails: the tool runs with nothing
// on the host saying it is inside, and a later step is free to take the directory.

import { expect } from "bun:test"
import { mkdtemp } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "path"
import { Context, Effect, Layer } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { Global } from "@opencode-ai/core/global"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import * as Writers from "@opencode-ai/core/snapshot/writers"
import { testEffect } from "../../core/test/lib/effect"
import { makeL2Drains, type L2DrainDeps } from "../src/l2-drain"

const it = testEffect(Layer.empty)

const materializerStack = (data: string) =>
  AppNodeBuilder.build(WorktreeMaterializer.node, [
    [Database.node, Database.layerFromPath(":memory:")],
    [Global.node, Layer.succeed(Global.Service, Global.make({ data }))],
  ])

/** A drain over a runner that reports what the host said about it while it was running. */
const drainsOver = (
  worktrees: WorktreeMaterializer.Interface,
  directory: string,
  runner: Partial<SessionRunner.Interface>,
) =>
  makeL2Drains({
    store: {
      get: () => Effect.succeed({ id: "ses_writers", location: { directory } }),
    } as unknown as L2DrainDeps["store"],
    // The location resolves to a runner and nothing else: what is under test is the bracket around
    // the call, not what the tool does inside it.
    locations: {
      get: () => Layer.succeed(SessionRunner.Service, runner as SessionRunner.Interface),
    } as unknown as L2DrainDeps["locations"],
    ctx: Context.empty() as L2DrainDeps["ctx"],
    events: { claim: () => Effect.void } as unknown as L2DrainDeps["events"],
    worktrees,
  })

it.live("marks the directory while a tool call runs and takes the mark back after it", () =>
  Effect.gen(function* () {
    const root = yield* Effect.promise(() => mkdtemp(path.join(tmpdir(), "opencode-l2-writers-")))
    const data = path.join(root, "host-data")
    const directory = path.join(root, "project")
    const worktrees = yield* WorktreeMaterializer.Service.pipe(
      Effect.provide(yield* Layer.build(materializerStack(data))),
    )

    const seen: Writers.Writer[][] = []
    const { toolCallDrain } = drainsOver(worktrees, directory, { runToolCall: ((input: { call: { id: string } }) =>
      Effect.gen(function* () {
        // Inside the body, which is the window the refusal exists for. Asked with no step of its
        // own, so what comes back is every marker the host is holding.
        seen.push(yield* Writers.strandedWriters(data, directory))
        return { outcome: "settled" as const, call: input.call.id }
      })) as unknown as SessionRunner.Interface["runToolCall"] })

    yield* Effect.promise(() =>
      toolCallDrain(
        {
          sessionID: "ses_writers",
          call: { id: "call_one", name: "write", assistantMessageID: "msg_one" },
          owner: "run:1:1",
          step: 3,
        },
        new AbortController().signal,
      ),
    )

    expect(seen).toHaveLength(1)
    expect(seen[0].map((w) => ({ session: w.sessionID, step: w.step, call: w.callID }))).toEqual([
      { session: "ses_writers", step: 3, call: "call_one" },
    ])
    // And the mark is gone once the body returned, or every later step would be refused a directory
    // nothing is writing.
    expect(yield* Writers.strandedWriters(data, directory)).toEqual([])
  }),
)

it.live("takes the mark back when the tool fails rather than returns", () =>
  Effect.gen(function* () {
    const root = yield* Effect.promise(() => mkdtemp(path.join(tmpdir(), "opencode-l2-writers-")))
    const data = path.join(root, "host-data")
    const directory = path.join(root, "project")
    const worktrees = yield* WorktreeMaterializer.Service.pipe(
      Effect.provide(yield* Layer.build(materializerStack(data))),
    )

    const { toolCallDrain } = drainsOver(worktrees, directory, {
      runToolCall: (() =>
        Effect.die(new Error("the tool blew up"))) as unknown as SessionRunner.Interface["runToolCall"],
    })

    const failed = yield* Effect.promise(() =>
      toolCallDrain(
        {
          sessionID: "ses_writers",
          call: { id: "call_two", name: "write", assistantMessageID: "msg_two" },
          owner: "run:1:1",
          step: 4,
        },
        new AbortController().signal,
      ).then(
        () => undefined,
        (err: unknown) => err,
      ),
    )
    expect(String(failed)).toContain("the tool blew up")
    // A call that failed is not a call still inside its own execution. Leaving the mark would refuse
    // the directory to everything after it for a tool that is over.
    expect(yield* Writers.strandedWriters(data, directory)).toEqual([])
  }),
)

it.live("seals a step away from its host without touching the tree", () =>
  Effect.gen(function* () {
    const root = yield* Effect.promise(() => mkdtemp(path.join(tmpdir(), "opencode-l2-writers-")))
    const data = path.join(root, "host-data")
    const directory = path.join(root, "project")
    const real = yield* WorktreeMaterializer.Service.pipe(
      Effect.provide(yield* Layer.build(materializerStack(data))),
    )
    // The rebuild is the part that must not happen, and with no packs stored it would return
    // without doing anything, so what is counted is the call rather than its effect.
    let rebuilds = 0
    const worktrees: WorktreeMaterializer.Interface = {
      ...real,
      ensure: (dir, options) => {
        rebuilds++
        return real.ensure(dir, options)
      },
    }

    const sealed: unknown[] = []
    const { sealDrain } = drainsOver(worktrees, directory, {
      sealStep: ((input: unknown) => {
        sealed.push(input)
        return Effect.succeed({ ran: true, continue: true, step: 2, promotion: undefined })
      }) as unknown as SessionRunner.Interface["sealStep"],
    })
    const seal = (withoutTheTree?: boolean) =>
      Effect.promise(() =>
        sealDrain(
          { sessionID: "ses_writers", step: 1, owner: "run:1:1", withoutTheTree },
          new AbortController().signal,
        ),
      )

    // An ordinary seal is on the host that ran the step, and it ships what the step produced.
    yield* seal()
    expect(rebuilds).toBe(1)
    expect((sealed[0] as { withoutTheTree?: boolean }).withoutTheTree).toBeUndefined()

    // One closing a step away from that host is not. Rebuilding here would put this host on the
    // newest state while the one that ran the tools may still be writing, and what it captured
    // would be the state before them.
    yield* seal(true)
    expect(rebuilds).toBe(1)
    expect((sealed[1] as { withoutTheTree?: boolean }).withoutTheTree).toBe(true)
  }),
)
