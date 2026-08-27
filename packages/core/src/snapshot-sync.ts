export * as SnapshotSync from "./snapshot-sync"

// Ships captured snapshot trees to the shared store as git packs, so a worker on another host can
// rebuild the project worktree before it drains a session (see session/execution/worktree.ts).
// Each push wraps the tree in a sync commit chained onto the previous push and packs only the
// delta. Best-effort by design: a failed push degrades portability, never the turn.

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
import { writeWorktreeTip } from "./snapshot/tip"
import { Hash } from "./util/hash"

export interface Interface {
  /** Ship a captured tree to the shared store as an incremental pack. Never fails the caller. */
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

    const push = Effect.fn("SnapshotSync.push")(function* (tree: Snapshot.ID) {
      // Noted before the packing, which is best-effort: what this host holds is true whether or not
      // the pack reaches the store, and a note left behind would let a later drain check out an
      // older tree over work only this host has.
      if (source) yield* writeWorktreeTip(global.data, worktree, tree)
      yield* Effect.gen(function* () {
        if (!source) return
        const latest = yield* db
          .select()
          .from(SnapshotPackTable)
          .where(eq(SnapshotPackTable.worktree, worktree))
          .orderBy(desc(SnapshotPackTable.time_created))
          .limit(1)
          .get()
          .pipe(Effect.orDie)
        // The newest shipped state already is this tree: nothing to pack.
        if (latest?.tree === tree) return
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
