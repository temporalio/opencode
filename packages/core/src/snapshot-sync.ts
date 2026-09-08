export * as SnapshotSync from "./snapshot-sync"

// Packs let a worker rebuild tracked files without sharing the live directory.

import { readFile, rm } from "node:fs/promises"
import os from "node:os"
import path from "path"
import { Cause, Context, Effect, Layer } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { desc, eq } from "drizzle-orm"
import { Database } from "./database/database"
import { makeLocationNode } from "./effect/app-node"
import { FSUtil } from "./fs-util"
import { Git } from "./git"
import { Global } from "./global"
import { Location } from "./location"
import { AppProcess } from "./process"
import { AbsolutePath } from "./schema"
import type { Snapshot } from "./snapshot"
import { SnapshotPackTable } from "./snapshot/sql"
import { chainHead } from "./snapshot/chain"
import { readWorktreeTip, writeWorktreeTip } from "./snapshot/tip"
import { Hash } from "./util/hash"

export interface Interface {
  /** A stale tip fails the caller; packing and insertion errors are logged. */
  readonly push: (tree: Snapshot.ID) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/SnapshotSync") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const git = yield* Git.Service
    const global = yield* Global.Service
    const location = yield* Location.Service
    const proc = yield* AppProcess.Service
    const { db } = yield* Database.Service
    const source = yield* git.repo.discover(location.project.directory)
    const worktree = source
      ? AbsolutePath.make(yield* fs.realPath(source.worktree).pipe(Effect.orDie))
      : location.project.directory
    // The same side git-dir Snapshot.capture writes trees into.
    const gitDirectory = path.join(global.data, "snapshot", location.project.id, Hash.fast(worktree))

    const run = (args: string[], stdin?: string) =>
      proc.run(
        ChildProcess.make("git", ["--git-dir", gitDirectory, "--work-tree", worktree, ...args], {
          cwd: worktree,
          env: {
            GIT_AUTHOR_NAME: "opencode",
            GIT_AUTHOR_EMAIL: "opencode@sync",
            GIT_COMMITTER_NAME: "opencode",
            GIT_COMMITTER_EMAIL: "opencode@sync",
          },
          extendEnv: true,
        }),
        { stdin },
      )

    // The newest state the store holds for this worktree, read off the chain the packs form rather
    // than off `time_created`, which is whichever host wrote the row.
    const newest = () =>
      db
        .select()
        .from(SnapshotPackTable)
        .where(eq(SnapshotPackTable.worktree, worktree))
        .all()
        .pipe(Effect.orDie, Effect.map(chainHead))

    const push = Effect.fn("SnapshotSync.push")(function* (tree: Snapshot.ID) {
      // A host that has not caught up must not publish its older files as the next tree.
      // This reading is not an atomic head claim and does not fence concurrent publishers.
      if (source) {
        const stoodOn = yield* readWorktreeTip(global.data, worktree)
        const ahead = yield* newest()
        if (ahead && ahead.tree !== tree && stoodOn !== ahead.tree) {
          yield* Effect.die(
            new Error(
              `refusing to ship ${worktree}: this host stood on ${stoodOn ?? "nothing"}, ` +
                `and the store is at ${ahead.tree}`,
            ),
          )
        }
      }
      yield* Effect.gen(function* () {
        if (!source) return
        const latest = yield* newest()
        // The newest shipped state already is this tree: nothing to pack, and the note is true.
        if (latest?.tree === tree) {
          yield* writeWorktreeTip(global.data, worktree, tree)
          return
        }
        // Chain onto the previous sync commit only when this host has it; a base absent locally
        // would produce a delta pack the pack builder cannot compute.
        const base =
          latest &&
          (yield* run(["cat-file", "-e", `${latest.id}^{commit}`]).pipe(Effect.orDie)).exitCode === 0
            ? latest.id
            : undefined
        const committed = yield* run([
          "commit-tree",
          tree,
          ...(base ? ["-p", base] : []),
          "-m",
          "opencode snapshot sync",
        ]).pipe(Effect.orDie)
        if (committed.exitCode !== 0)
          return yield* Effect.die(new Error(`commit-tree: ${committed.stderr.toString("utf8")}`))
        const commit = committed.stdout.toString("utf8").trim()
        const prefix = path.join(os.tmpdir(), `opencode-snapshot-${commit.slice(0, 12)}`)
        const packed = yield* run(
          ["pack-objects", "--revs", "-q", prefix],
          `${commit}\n${base ? `^${base}\n` : ""}`,
        ).pipe(Effect.orDie)
        if (packed.exitCode !== 0)
          return yield* Effect.die(new Error(`pack-objects: ${packed.stderr.toString("utf8")}`))
        const packHash = packed.stdout.toString("utf8").trim()
        const packFile = `${prefix}-${packHash}.pack`
        const bytes = yield* Effect.promise(() => readFile(packFile))
        yield* Effect.promise(() => Promise.allSettled([rm(packFile), rm(`${prefix}-${packHash}.idx`)]))
        yield* db
          .insert(SnapshotPackTable)
          .values([
            { id: commit, directory: location.directory, worktree, tree, base: base ?? null, pack: bytes },
          ])
          .onConflictDoNothing()
          .run()
          .pipe(Effect.orDie)
        // A note must not name a state whose insertion failed.
        yield* writeWorktreeTip(global.data, worktree, tree)
      }).pipe(
        Effect.catchCauseIf(
          (cause) => !Cause.hasInterrupts(cause),
          (cause) => Effect.logWarning("failed to ship snapshot pack", { tree, cause }),
        ),
        Effect.asVoid,
      )
    })

    return Service.of({ push })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [Database.node, FSUtil.node, Git.node, Global.node, Location.node, AppProcess.node],
})
