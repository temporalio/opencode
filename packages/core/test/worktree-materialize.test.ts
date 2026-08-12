// Cross-host worktree materialization: the capture side ships snapshot trees as git packs into
// the shared store; a worker on a host without the project tree rebuilds it before draining. One
// DB file, two independent stacks: "host A" captures and pushes, the worktree is deleted to
// simulate a fresh host, "host B" materializes it back from the store alone.
import { describe, expect } from "bun:test"
import { $ } from "bun"
import { realpathSync } from "node:fs"
import { mkdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "path"
import { asc } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { SnapshotSync } from "@opencode-ai/core/snapshot-sync"
import { SnapshotPackTable } from "@opencode-ai/core/snapshot/sql"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { testEffect } from "./lib/effect"
import { tmpdir } from "./fixture/tmpdir"

const it = testEffect(Layer.empty)

// The capturing host: real Location resolution against the git worktree, its own data dir for the
// side snapshot repo, the shared DB file.
const captureStack = (file: string, worktree: string, data: string) =>
  AppNodeBuilder.build(LayerNode.group([Snapshot.node, SnapshotSync.node]), [
    [Database.node, Database.layerFromPath(file)],
    [Location.node, Location.boundNode({ directory: AbsolutePath.make(worktree) })],
    [Global.node, Layer.succeed(Global.Service, Global.make({ data }))],
  ])

// The resuming host: only the shared store, no location, no snapshot repo, no worktree.
const materializeStack = (file: string) =>
  AppNodeBuilder.build(WorktreeMaterializer.node, [[Database.node, Database.layerFromPath(file)]])

describe("WorktreeMaterializer", () => {
  it.live("rebuilds a deleted worktree from shared-store packs, incremental chain included", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const root = realpathSync(tmp.path)
      const worktree = path.join(root, "project")
      const file = path.join(root, "shared.db")
      yield* Effect.promise(async () => {
        await mkdir(worktree, { recursive: true })
        await $`git init -q ${worktree}`.quiet()
        await $`git -C ${worktree} config user.email t@t`.quiet()
        await $`git -C ${worktree} config user.name t`.quiet()
        await writeFile(path.join(worktree, "tracked.txt"), "v1\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
        // The state to port: an uncommitted edit and a file git never saw.
        await writeFile(path.join(worktree, "tracked.txt"), "v2\n")
        await writeFile(path.join(worktree, "untracked.txt"), "notes\n")
      })

      const A = yield* Layer.build(captureStack(file, worktree, path.join(root, "host-a-data")))
      const first = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      if (!first) throw new Error("expected the first capture to produce a tree")
      yield* SnapshotSync.Service.use((s) => s.push(first)).pipe(Effect.provide(A))

      // A second increment on top, so materialization has to index a base pack plus a delta pack.
      yield* Effect.sleep(10)
      yield* Effect.promise(async () => {
        await writeFile(path.join(worktree, "tracked.txt"), "v3\n")
        await writeFile(path.join(worktree, "extra.txt"), "more\n")
      })
      const second = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      if (!second) throw new Error("expected the second capture to produce a tree")
      yield* SnapshotSync.Service.use((s) => s.push(second)).pipe(Effect.provide(A))

      const rows = yield* Database.Service.use(({ db }) =>
        db.select().from(SnapshotPackTable).orderBy(asc(SnapshotPackTable.time_created)).all(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)
      expect(rows).toHaveLength(2)
      expect(rows[1]?.base).toBe(rows[0]!.id)

      // The fresh host: the tree is gone, only the shared store remains.
      yield* Effect.promise(() => rm(worktree, { recursive: true, force: true }))
      const B = yield* Layer.build(materializeStack(file))
      yield* WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(Effect.provide(B))

      const [tracked, untracked, extra] = yield* Effect.promise(() =>
        Promise.all([
          readFile(path.join(worktree, "tracked.txt"), "utf8"),
          readFile(path.join(worktree, "untracked.txt"), "utf8"),
          readFile(path.join(worktree, "extra.txt"), "utf8"),
        ]),
      )
      expect(tracked).toBe("v3\n")
      expect(untracked).toBe("notes\n")
      expect(extra).toBe("more\n")

      // A second ensure on an existing tree is a no-op, not a rebuild.
      yield* WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(Effect.provide(B))
      expect(yield* Effect.promise(() => readFile(path.join(worktree, "tracked.txt"), "utf8"))).toBe(
        "v3\n",
      )

      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  // The shared-store deployment uses the libsql backend, so the pack blob has to survive that
  // driver's parameter path too, not only bun's.
  it.live("round-trips a pack blob through the libsql backend", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const file = path.join(realpathSync(tmp.path), "libsql.db")
      const bytes = Buffer.from([0x50, 0x41, 0x43, 0x4b, 0x00, 0x01, 0xff, 0xfe, 0x00, 0x7f])
      const layer = Database.layerFromLibsql(`file:${file}`)
      yield* Database.Service.use(({ db }) =>
        db
          .insert(SnapshotPackTable)
          .values([{ id: "c".repeat(40), directory: "/w", worktree: "/w", tree: "t".repeat(40), pack: bytes }])
          .run(),
      ).pipe(Effect.orDie, Effect.provide(layer), Effect.scoped)
      const row = yield* Database.Service.use(({ db }) =>
        db.select().from(SnapshotPackTable).get(),
      ).pipe(Effect.orDie, Effect.provide(layer), Effect.scoped)
      expect(Buffer.from(row!.pack).equals(bytes)).toBeTrue()
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )
})
