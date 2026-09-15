// The seal is the one unit of a step that can run away from the host that ran the step, and when it
// does it has to leave the tree alone: rebuilding here would put this host on the newest state while
// the one that ran the tools may still be writing, and what it captured would be the state before
// them. This drives the real drain, because the option is threaded through it and nothing else
// checks that it reaches the materializer.

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
import { testEffect } from "../../core/test/lib/effect"
import { type DrainDeps, makeInSession } from "../src/drain"
import { makeSteppedDrains } from "../src/stepped-drain"

const it = testEffect(Layer.empty)

const materializerStack = (data: string) =>
  AppNodeBuilder.build(WorktreeMaterializer.node, [
    [Database.node, Database.layerFromPath(":memory:")],
    [Global.node, Layer.succeed(Global.Service, Global.make({ data }))],
  ])

/** A drain over a runner that reports what it was asked to do. */
const drainsOver = (
  worktrees: WorktreeMaterializer.Interface,
  directory: string,
  runner: Partial<SessionRunner.Interface>,
) =>
  makeSteppedDrains({
    inSession: makeInSession({
      store: {
        get: () => Effect.succeed({ id: "ses_writers", location: { directory } }),
      } as unknown as DrainDeps["store"],
      // The location resolves to a runner and nothing else: what is under test is what surrounds
      // the runner call, not what the runner does inside it.
      locations: {
        get: () => Layer.succeed(SessionRunner.Service, runner as SessionRunner.Interface),
      } as unknown as DrainDeps["locations"],
      ctx: Context.empty() as DrainDeps["ctx"],
      events: { claim: () => Effect.void } as unknown as DrainDeps["events"],
      worktrees,
    }),
  })

it.live("seals a step away from its host without touching the tree", () =>
  Effect.gen(function* () {
    const root = yield* Effect.promise(() => mkdtemp(path.join(tmpdir(), "opencode-l2-writers-")))
    const data = path.join(root, "host-data")
    const directory = path.join(root, "project")
    const real = yield* WorktreeMaterializer.Service.pipe(Effect.provide(yield* Layer.build(materializerStack(data))))
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
        return Effect.succeed({ continue: true, step: 2, promotion: undefined })
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
