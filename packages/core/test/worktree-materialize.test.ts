// Cross-host worktree materialization: the capture side ships snapshot trees as git packs into
// the shared store; a worker on a host without the project tree rebuilds it before draining. One
// DB file, two independent stacks: "host A" captures and pushes, the worktree is deleted to
// simulate a fresh host, "host B" materializes it back from the store alone.
import { describe, expect } from "bun:test"
import { $ } from "bun"
import { realpathSync } from "node:fs"
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "path"
import { asc } from "drizzle-orm"
import { Effect, Fiber, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { Global } from "@opencode-ai/core/global"
import { Location } from "@opencode-ai/core/location"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { SnapshotSync } from "@opencode-ai/core/snapshot-sync"
import { SnapshotPackTable } from "@opencode-ai/core/snapshot/sql"
import { writeWorktreeTip } from "@opencode-ai/core/snapshot/tip"
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

// The resuming host: the shared store and its own data directory, no location, no snapshot repo,
// no worktree. The data directory is what makes two of these independent hosts: it holds the note
// saying which state that host's tree is at.
const materializeStack = (file: string, data: string) =>
  AppNodeBuilder.build(WorktreeMaterializer.node, [
    [Database.node, Database.layerFromPath(file)],
    [Global.node, Layer.succeed(Global.Service, Global.make({ data }))],
  ])

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
      const B = yield* Layer.build(materializeStack(file, path.join(root, "host-b-data")))
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

  // A rebuild that fails removes what it created, and only that. The reading it asks is the one
  // taken inside the lock: another drain can fill the directory while this one waits for it, and
  // the reading from before the wait then names a directory that no longer exists. What that costs
  // is not the rebuild, which retries, but the files git ignores in what it removed: an install, a
  // build, a `.env`. `pauseBeforeLock` is the wait, and it is the only thing invented here.
  it.live("keeps a directory another drain filled while a failed rebuild waited for the lock", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const root = realpathSync(tmp.path)
      const worktree = path.join(root, "project")
      const file = path.join(root, "shared.db")
      const data = path.join(root, "host-b-data")
      yield* Effect.promise(async () => {
        await mkdir(worktree, { recursive: true })
        await $`git init -q ${worktree}`.quiet()
        await $`git -C ${worktree} config user.email t@t`.quiet()
        await $`git -C ${worktree} config user.name t`.quiet()
        await writeFile(path.join(worktree, "tracked.txt"), "v1\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
      })

      const A = yield* Layer.build(captureStack(file, worktree, path.join(root, "host-a-data")))
      const first = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      if (!first) throw new Error("expected a capture")
      yield* SnapshotSync.Service.use((s) => s.push(first)).pipe(Effect.provide(A))
      const stored = yield* Database.Service.use(({ db }) =>
        db.select().from(SnapshotPackTable).all(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)

      // The newest state in the store, and a pack that is not a pack: indexing it is how a rebuild
      // fails for reasons the store cannot rule out.
      yield* Effect.sleep(10)
      yield* Database.Service.use(({ db }) =>
        db
          .insert(SnapshotPackTable)
          .values([
            {
              id: "f".repeat(40),
              directory: worktree,
              worktree,
              tree: "e".repeat(40),
              base: stored[0]!.id,
              pack: Buffer.from([0x50, 0x41, 0x43, 0x4b]),
            },
          ])
          .run(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)

      // This host is empty and behind, which is the state that decides to rebuild.
      yield* Effect.promise(() => rm(worktree, { recursive: true, force: true }))
      const B = yield* Layer.build(materializeStack(file, data))
      const rebuilding = yield* WorktreeMaterializer.Service.use((w) =>
        w.ensure(worktree, { pauseBeforeLock: 400 }),
      ).pipe(Effect.provide(B), Effect.exit, Effect.forkChild)

      // What another drain leaves behind while this one waits: a checkout, the files git ignores,
      // and the note saying this host agreed to that state.
      yield* Effect.sleep(150)
      yield* Effect.promise(async () => {
        await mkdir(worktree, { recursive: true })
        await writeFile(path.join(worktree, "tracked.txt"), "v1\n")
        await writeFile(path.join(worktree, ".env"), "SECRET=1\n")
      })
      yield* writeWorktreeTip(data, worktree, stored[0]!.tree)

      const outcome = yield* Fiber.join(rebuilding)
      // The rebuild really did fail, which is the premise: a check where it succeeded would say
      // nothing about what a failure removes.
      expect(outcome._tag).toBe("Failure")

      // The rebuild failed on the bad pack. The packs would restore `tracked.txt` on a retry; the
      // ignored file is in no pack and nothing else has a copy.
      const left = yield* Effect.promise(() => readdir(worktree).catch(() => [] as string[]))
      // A `.git` the failed rebuild made on its way is fine; what must survive is the other drain's
      // work, and above all the file no pack carries.
      expect(left).toContain("tracked.txt")
      expect(left).toContain(".env")

      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("rebuilds into a directory that exists but is empty", () =>
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
        await writeFile(path.join(worktree, "note.txt"), "travelled\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
      })

      const A = yield* Layer.build(captureStack(file, worktree, path.join(root, "host-a-data")))
      const captured = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      if (!captured) throw new Error("expected a capture")
      yield* SnapshotSync.Service.use((s) => s.push(captured)).pipe(Effect.provide(A))

      // The shape a container gives a fresh host: the path is there because something mounted it,
      // and there is nothing in it. Deleting the directory instead is the case already covered,
      // and it is the easy one: an absent tree is obviously safe to build.
      yield* Effect.promise(async () => {
        await rm(worktree, { recursive: true, force: true })
        await mkdir(worktree, { recursive: true })
      })

      const B = yield* Layer.build(materializeStack(file, path.join(root, "host-b-data")))
      yield* WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(Effect.provide(B))

      expect(yield* Effect.promise(() => readFile(path.join(worktree, "note.txt"), "utf8"))).toBe(
        "travelled\n",
      )

      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  it.live("moves a tree that is behind forward, and leaves one already at the tip alone", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const root = realpathSync(tmp.path)
      const worktree = path.join(root, "project")
      const file = path.join(root, "shared.db")
      const tracked = path.join(worktree, "tracked.txt")
      const content = () => Effect.promise(() => readFile(tracked, "utf8"))
      const put = (text: string) => Effect.promise(() => writeFile(tracked, text))
      yield* Effect.promise(async () => {
        await mkdir(worktree, { recursive: true })
        await $`git init -q ${worktree}`.quiet()
        await $`git -C ${worktree} config user.email t@t`.quiet()
        await $`git -C ${worktree} config user.name t`.quiet()
        await writeFile(tracked, "v1\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
      })

      const A = yield* Layer.build(captureStack(file, worktree, path.join(root, "host-a-data")))
      const ship = Effect.gen(function* () {
        const tree = yield* Snapshot.Service.use((s) => s.capture())
        if (!tree) throw new Error("expected a capture to produce a tree")
        yield* SnapshotSync.Service.use((s) => s.push(tree))
      }).pipe(Effect.provide(A))
      yield* ship

      // A checkout this host already had is somebody's working copy: however far behind the store
      // it is, checking a stored tree out over it would rewrite files under whoever owns them.
      const C = yield* Layer.build(materializeStack(file, path.join(root, "host-c-data")))
      yield* put("mine\n")
      yield* WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(Effect.provide(C))
      expect(yield* content()).toBe("mine\n")

      // Host B builds the tree from the store, which is what makes it B's to move.
      yield* Effect.promise(() => rm(worktree, { recursive: true, force: true }))
      const B = yield* Layer.build(materializeStack(file, path.join(root, "host-b-data")))
      const ensureB = WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(
        Effect.provide(B),
      )
      yield* ensureB
      expect(yield* content()).toBe("v1\n")

      // The store moves on while B's tree does not: another host captured a newer state, which is
      // what a worker picking up a later step of the same session arrives to.
      yield* Effect.sleep(10)
      yield* put("v2\n")
      yield* ship
      yield* put("v1\n")

      yield* ensureB

      // Rebuilding only a missing tree is not enough: a present one is served as it was left, and
      // the tools of the step read files from whichever step this worker last ran.
      expect(yield* content()).toBe("v2\n")

      // A tree already at the newest state is left alone whatever is in it. This is what keeps the
      // later tools of a step from checking out over what its earlier tools wrote, since nothing
      // captures those until the step is sealed.
      yield* put("uncaptured\n")
      yield* ensureB
      expect(yield* content()).toBe("uncaptured\n")

      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  // The write direction. A host the store has moved past used to pack its older files, become the
  // newest by time, and every other host then checked that out over the work they were shipped to
  // carry. This is the same rule the read direction already had, in the direction nothing checked.
  it.live("refuses to ship from a host the store has moved past", () =>
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
        await writeFile(path.join(worktree, "f.txt"), "v1\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
      })

      const A = yield* Layer.build(captureStack(file, worktree, path.join(root, "a-data")))
      const first = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      yield* SnapshotSync.Service.use((s) => s.push(first!)).pipe(Effect.provide(A))

      // Another host ships while this one is not looking. Written straight into the store, because
      // two capture stacks for one worktree resolve to the same host: the node builder keys them by
      // location, so the second host has to be the row rather than a second stack.
      const elsewhere = "e".repeat(40)
      yield* Effect.sleep(10)
      yield* Database.Service.use(({ db }) =>
        db
          .insert(SnapshotPackTable)
          .values([
            {
              id: "d".repeat(40),
              directory: worktree,
              worktree,
              tree: elsewhere,
              pack: Buffer.from([0x50, 0x41, 0x43, 0x4b]),
            },
          ])
          .run(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)

      // This host is still standing on `first`, so what it holds is not built on what the store now
      // says the project is. Shipping it would revert the other host.
      yield* Effect.promise(() => writeFile(path.join(worktree, "f.txt"), "stale\n"))
      const stale = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      const exit = yield* SnapshotSync.Service.use((s) => s.push(stale!)).pipe(
        Effect.provide(A),
        Effect.exit,
      )
      expect(exit._tag).toBe("Failure")

      // Nothing was added, and the note was not moved either: a refused ship must leave this host
      // saying what it actually holds.
      const rows = yield* Database.Service.use(({ db }) =>
        db.select().from(SnapshotPackTable).orderBy(asc(SnapshotPackTable.time_created)).all(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)
      expect(rows).toHaveLength(2)
      expect(rows[1]?.tree).toBe(elsewhere)

      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )

  // A host that seeded the session from its own checkout has a note but no rebuild marker, because
  // only a rebuild writes one. Gating the move on that marker meant every activity such a host drew
  // died as soon as any other host shipped, and the comment said it would be sent elsewhere when the
  // boundary marks it non-retryable. The note is the rule: a host that agreed to a state may be
  // moved off it.
  it.live("moves a tree the host captured rather than rebuilt", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const root = realpathSync(tmp.path)
      const worktree = path.join(root, "project")
      const file = path.join(root, "shared.db")
      const data = path.join(root, "seed-host-data")
      yield* Effect.promise(async () => {
        await mkdir(worktree, { recursive: true })
        await $`git init -q ${worktree}`.quiet()
        await $`git -C ${worktree} config user.email t@t`.quiet()
        await $`git -C ${worktree} config user.name t`.quiet()
        await writeFile(path.join(worktree, "f.txt"), "seeded\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
      })

      // This host captures from its own checkout, so it gets a note and no rebuild marker.
      const A = yield* Layer.build(captureStack(file, worktree, data))
      const first = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      yield* SnapshotSync.Service.use((s) => s.push(first!)).pipe(Effect.provide(A))
      const packs = yield* Database.Service.use(({ db }) =>
        db.select().from(SnapshotPackTable).all(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)

      // Another host ships on top, so this one is behind.
      yield* Effect.sleep(10)
      yield* Database.Service.use(({ db }) =>
        db
          .insert(SnapshotPackTable)
          .values([
            {
              id: "d".repeat(40),
              directory: worktree,
              worktree,
              tree: "e".repeat(40),
              base: packs[0]!.id,
              pack: Buffer.from([0x50, 0x41, 0x43, 0x4b]),
            },
          ])
          .run(),
      ).pipe(Effect.orDie, Effect.provide(Database.layerFromPath(file)), Effect.scoped)

      const B = yield* Layer.build(materializeStack(file, data))
      const exit = yield* WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(
        Effect.provide(B),
        Effect.exit,
      )

      // The pack above is not a real one, so the rebuild itself cannot succeed here. What this pins
      // is which failure: a rebuild that was attempted and failed, not a refusal to try.
      const why = String(exit)
      expect(why).not.toContain("was not built")
      expect(why).toContain("could not materialize")

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
