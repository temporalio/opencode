// Cross-host worktree materialization: the capture side ships snapshot trees as git packs into
// the shared store; a worker on a host without the project tree rebuilds it before draining. One
// DB file, two independent stacks: "host A" captures and pushes, the worktree is deleted to
// simulate a fresh host, "host B" materializes it back from the store alone.
import { describe, expect } from "bun:test"
import { $ } from "bun"
import { execFile, spawn } from "node:child_process"
import { randomBytes } from "node:crypto"
import { promisify } from "node:util"
import { realpathSync } from "node:fs"
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "path"
import { asc } from "drizzle-orm"
import { Effect, Fiber, Layer } from "effect"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { EventSequenceTable } from "@opencode-ai/core/event/sql"
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
      expect(yield* Effect.promise(() => readFile(path.join(worktree, "tracked.txt"), "utf8"))).toBe("v3\n")

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
      const stored = yield* Database.Service.use(({ db }) => db.select().from(SnapshotPackTable).all()).pipe(
        Effect.orDie,
        Effect.provide(Database.layerFromPath(file)),
        Effect.scoped,
      )

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

      expect(yield* Effect.promise(() => readFile(path.join(worktree, "note.txt"), "utf8"))).toBe("travelled\n")

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
      const ensureB = WorktreeMaterializer.Service.use((w) => w.ensure(worktree)).pipe(Effect.provide(B))
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
      const exit = yield* SnapshotSync.Service.use((s) => s.push(stale!)).pipe(Effect.provide(A), Effect.exit)
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
      const packs = yield* Database.Service.use(({ db }) => db.select().from(SnapshotPackTable).all()).pipe(
        Effect.orDie,
        Effect.provide(Database.layerFromPath(file)),
        Effect.scoped,
      )

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
      const row = yield* Database.Service.use(({ db }) => db.select().from(SnapshotPackTable).get()).pipe(
        Effect.orDie,
        Effect.provide(layer),
        Effect.scoped,
      )
      expect(Buffer.from(row!.pack).equals(bytes)).toBeTrue()
      yield* Effect.promise(() => tmp[Symbol.asyncDispose]())
    }),
  )
})

// A process in a group of its own, which is what a worker somebody's supervisor started has, and
// what a tool that daemonizes gives itself. It carries a worker name of this test's choosing, so
// what each case turns on is the one thing that case is about. Without one it gets this process's
// environment, which is the case that says the name is exported rather than only written down.
const run = promisify(execFile)

// Per run, because a name is what the host looks for and an assertion that fails before its child
// is killed leaves that child running. A fixed name would then answer for every later run.
const runToken = randomBytes(4).toString("hex")
const named = (worker: string) => `${worker}-${runToken}`

const spawned = (worker?: string) => {
  const child = spawn(process.execPath, ["-e", "setInterval(() => {}, 1000)"], {
    detached: true,
    stdio: "ignore",
    env:
      worker === undefined ? process.env : { ...process.env, OPENCODE_WORKTREE_WRITER: named(worker) },
  })
  return { child, pid: child.pid! }
}

const ended = async (started: ReturnType<typeof spawned>) => {
  const exited = new Promise((resolve) => started.child.once("exit", resolve))
  started.child.kill("SIGKILL")
  await exited
  return { pid: started.pid, pgid: started.pid }
}

/** Say the one marker on this host was written by another process, or before a restart. */
const editMarker = async (data: string, worktree: string, patch: Record<string, unknown>) => {
  const root = path.join(data, "worktree-writers")
  const [dir] = await readdir(root)
  const [name] = await readdir(path.join(root, dir))
  const file = path.join(root, dir, name)
  const note = JSON.parse(await readFile(file, "utf8"))
  await writeFile(file, JSON.stringify({ ...note, ...patch }))
}

describe("WorktreeMaterializer quarantine", () => {
  // Refusing to move a step off a host protects that step and nothing after it: the turn ends, the
  // next prompt lands wherever there is room, and the tool from before can still be writing. A
  // marker says which calls are inside their own execution, and the directory belongs to that
  // call's step until it returns.
  it.live("refuses a directory to another step while an earlier call has not returned", () =>
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => tmpdir())
      const root = realpathSync(tmp.path)
      const worktree = path.join(root, "project")
      const file = path.join(root, "shared.db")
      const data = path.join(root, "host-data")
      yield* Effect.promise(async () => {
        await mkdir(worktree, { recursive: true })
        await $`git init -q ${worktree}`.quiet()
        await $`git -C ${worktree} config user.email t@t`.quiet()
        await $`git -C ${worktree} config user.name t`.quiet()
        await writeFile(path.join(worktree, "tracked.txt"), "v1\n")
        await $`git -C ${worktree} add .`.quiet()
        await $`git -C ${worktree} commit -qm seed`.quiet()
      })

      const A = yield* Layer.build(captureStack(file, worktree, data))
      const captured = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      if (!captured) throw new Error("expected a capture")
      yield* SnapshotSync.Service.use((s) => s.push(captured)).pipe(Effect.provide(A))

      const B = yield* Layer.build(materializeStack(file, data))
      const worktrees = yield* WorktreeMaterializer.Service.pipe(Effect.provide(B))

      // A call of an earlier step that never came back.
      const stranded = { sessionID: "ses_one", step: 1, callID: "call_stranded" }
      yield* worktrees.beginWrite(worktree, stranded)

      // Another step wants the directory. It is not this call's step, so it is refused, and the
      // refusal is a defect the activity boundary turns into a failure Temporal schedules again.
      const later = { sessionID: "ses_one", step: 2, callID: "call_later" }
      const refused = yield* Effect.exit(worktrees.ensure(worktree, { current: later }))
      expect(refused._tag).toBe("Failure")

      // A sibling of the same step is not stranded: two tools of one step share this directory by
      // design, and refusing them would be refusing the feature.
      const sibling = { sessionID: "ses_one", step: 1, callID: "call_sibling" }
      const allowed = yield* Effect.exit(worktrees.ensure(worktree, { current: sibling }))
      expect(allowed._tag).toBe("Success")

      // When the call comes back, whatever it did to the directory, it is not still doing it.
      yield* worktrees.endWrite(worktree, stranded.callID)
      const afterReturn = yield* Effect.exit(worktrees.ensure(worktree, { current: later }))
      expect(afterReturn._tag).toBe("Success")

      // A worker that died mid-tool leaves its marker behind, and that used to need a person. Most
      // of it is answerable without one: the writer's process is gone, nothing it started is left
      // in its group, and the machine has not restarted underneath the pids that say so.
      const usable = () =>
        Effect.exit(worktrees.ensure(worktree, { current: later })).pipe(Effect.map((exit) => exit._tag === "Success"))

      // Written by this process, which is running. Nothing to conclude, so the refusal stands.
      yield* worktrees.beginWrite(worktree, { sessionID: "ses_one", step: 3, callID: "call_dead" })
      expect(yield* usable()).toBe(false)

      // The worker died and left nothing behind: no process of its own, nothing carrying its name,
      // and an empty group. That is the whole of the proof that its tools are over.
      const gone = yield* Effect.promise(() => ended(spawned("gone")))
      const stale = { pid: gone.pid, pgid: gone.pgid, worker: named("gone") }
      yield* Effect.promise(() => editMarker(data, worktree, stale))
      expect(yield* usable()).toBe(true)

      // The worker died and something it started did not. That is what the refusal is for.
      const orphan = spawned("orphaned")
      yield* worktrees.beginWrite(worktree, { sessionID: "ses_one", step: 3, callID: "call_dead" })
      yield* Effect.promise(() =>
        editMarker(data, worktree, { ...stale, pgid: orphan.pid, worker: named("orphaned") }),
      )
      expect(yield* usable()).toBe(false)
      yield* Effect.promise(() => ended(orphan))
      expect(yield* usable()).toBe(true)

      // A tool that asks for a group of its own is out of the group check's reach. What it cannot
      // put down is the name its worker left in the environment it inherited.
      const escaped = spawned("escaped")
      yield* worktrees.beginWrite(worktree, { sessionID: "ses_one", step: 3, callID: "call_dead" })
      yield* Effect.promise(() => editMarker(data, worktree, { ...stale, worker: named("escaped") }))
      expect(yield* usable()).toBe(false)
      yield* Effect.promise(() => ended(escaped))
      expect(yield* usable()).toBe(true)

      // And the name has to reach the tool, not only the marker. This child is given no environment
      // of its own, so the only way it carries the name is that the worker exported it.
      const inheriting = spawned()
      yield* worktrees.beginWrite(worktree, { sessionID: "ses_one", step: 3, callID: "call_dead" })
      yield* Effect.promise(() =>
        editMarker(data, worktree, { ...stale, worker: process.env.OPENCODE_WORKTREE_WRITER }),
      )
      expect(yield* usable()).toBe(false)
      yield* Effect.promise(() => ended(inheriting))
      expect(yield* usable()).toBe(true)

      // A worker restarted from the same shell is in the group its predecessor was in, so the group
      // answers for this process rather than for the marker. What the dead worker started is what
      // decides, and it started nothing.
      const ourGroup = yield* Effect.promise(async () => {
        const { stdout } = await run("ps", ["-o", "pgid=", "-p", String(process.pid)])
        return Number.parseInt(stdout.trim(), 10)
      })
      yield* worktrees.beginWrite(worktree, { sessionID: "ses_one", step: 3, callID: "call_dead" })
      yield* Effect.promise(() => editMarker(data, worktree, { ...stale, pgid: ourGroup }))
      expect(yield* usable()).toBe(true)

      // A pid means nothing across a restart, so a marker from before one is not read as live.
      yield* worktrees.beginWrite(worktree, { sessionID: "ses_one", step: 3, callID: "call_dead" })
      yield* Effect.promise(() => editMarker(data, worktree, { bootAt: 0 }))
      expect(yield* usable()).toBe(true)
    }),
  )
})

// A dispatch the session stopped waiting for keeps running, and the files it ships afterwards would
// be the newest state the store holds: the tip check cannot refuse them, because the host is still
// standing exactly where it was told to stand. The event log already fences a superseded attempt
// out of the transcript, and the packs travel under the same token.
describe("SnapshotSync owner fence", () => {
  it.live("refuses a pack from an attempt the session has moved past", () =>
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
      })

      const A = yield* Layer.build(captureStack(file, worktree, path.join(root, "host-a-data")))
      const onDatabase = <A2, E2>(use: (db: Database.Interface["db"]) => Effect.Effect<A2, E2>) =>
        Database.Service.use(({ db }) => use(db)).pipe(
          Effect.orDie,
          Effect.provide(Database.layerFromPath(file)),
          Effect.scoped,
        )
      // The session is on a later attempt than the one that is about to publish.
      yield* onDatabase((db) =>
        db.insert(EventSequenceTable).values({ aggregate_id: "ses_fenced", seq: 1, owner_id: "run:1:2" }).run(),
      )

      const captured = yield* Snapshot.Service.use((s) => s.capture()).pipe(Effect.provide(A))
      if (!captured) throw new Error("expected a capture")
      const shipped = (owner: string) =>
        Effect.exit(
          SnapshotSync.Service.use((s) => s.push(captured, "ses_fenced")).pipe(
            Effect.provideService(EventV2.EventOwner, owner),
            Effect.provide(A),
          ),
        )

      const stale = yield* shipped("run:1:1")
      expect(stale._tag).toBe("Failure")
      expect(yield* onDatabase((db) => db.select().from(SnapshotPackTable).all())).toHaveLength(0)

      // The attempt the session is actually on ships as usual.
      const current = yield* shipped("run:1:2")
      expect(current._tag).toBe("Success")
      expect(yield* onDatabase((db) => db.select().from(SnapshotPackTable).all())).toHaveLength(1)
    }),
  )
})
