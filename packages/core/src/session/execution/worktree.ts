export * as WorktreeMaterializer from "./worktree"

// Rebuilds a missing project worktree from the shared store before a drain runs. This closes the
// host-local gap in cross-host resume: file tools need the tree, and a fresh worker does not have
// it. The capture side (snapshot-sync.ts) ships each snapshot as an incremental git pack; this
// side indexes every pack for the worktree into a fresh repo and checks out the newest tree.
// Ignored files and dependencies are not captured, so a bootstrap step (install, build) stays the
// project's own concern.

import { rm, writeFile } from "node:fs/promises"
import path from "path"
import { Cause, Context, Effect, Layer } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { asc, desc, eq } from "drizzle-orm"
import { Database } from "../../database/database"
import { makeGlobalNode } from "../../effect/app-node"
import { KeyedMutex } from "../../effect/keyed-mutex"
import { FSUtil } from "../../fs-util"
import { Git } from "../../git"
import { AppProcess } from "../../process"
import { AbsolutePath } from "../../schema"
import { SnapshotPackTable } from "../../snapshot/sql"

export interface Interface {
  /**
   * Make sure the session's directory exists, rebuilding its worktree from stored snapshot packs
   * when it does not. A directory with no stored packs, or one whose worktree root already
   * exists, is left alone. Never fails the caller.
   */
  readonly ensure: (directory: string) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()(
  "@opencode/v2/WorktreeMaterializer",
) {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const git = yield* Git.Service
    const proc = yield* AppProcess.Service
    const { db } = yield* Database.Service
    const locks = KeyedMutex.makeUnsafe<string>()

    const materialize = Effect.fnUntraced(function* (tip: typeof SnapshotPackTable.$inferSelect) {
      const worktree = AbsolutePath.make(tip.worktree)
      const repository = yield* git.repo
        .create({ worktree, gitDirectory: AbsolutePath.make(path.join(worktree, ".git")) })
        .pipe(Effect.orDie)
      // Index every pack shipped for this worktree; objects accumulate, the newest tree wins.
      const rows = yield* db
        .select()
        .from(SnapshotPackTable)
        .where(eq(SnapshotPackTable.worktree, tip.worktree))
        .orderBy(asc(SnapshotPackTable.time_created))
        .all()
        .pipe(Effect.orDie)
      const packDirectory = path.join(repository.gitDirectory, "objects", "pack")
      yield* fs.ensureDir(packDirectory).pipe(Effect.orDie)
      for (const row of rows) {
        const packFile = path.join(packDirectory, `pack-${row.id}.pack`)
        yield* Effect.promise(() => writeFile(packFile, row.pack))
        const indexed = yield* proc
          .run(
            ChildProcess.make("git", ["--git-dir", repository.gitDirectory, "index-pack", packFile], {
              cwd: worktree,
              extendEnv: true,
            }),
          )
          .pipe(Effect.orDie)
        if (indexed.exitCode !== 0)
          return yield* Effect.die(new Error(`index-pack: ${indexed.stderr.toString("utf8")}`))
      }
      yield* git.tree.checkout({ repository, tree: Git.TreeID.make(tip.tree) }).pipe(Effect.orDie)
      // Point HEAD at the sync commit so the rebuilt repo reads as a clean checkout, not an
      // unborn branch over a full untracked tree. Cosmetic; the files above are what matter.
      yield* proc
        .run(
          ChildProcess.make(
            "git",
            ["--git-dir", repository.gitDirectory, "update-ref", "refs/heads/opencode-restore", tip.id],
            { cwd: worktree, extendEnv: true },
          ),
        )
        .pipe(Effect.ignore)
      yield* proc
        .run(
          ChildProcess.make(
            "git",
            ["--git-dir", repository.gitDirectory, "symbolic-ref", "HEAD", "refs/heads/opencode-restore"],
            { cwd: worktree, extendEnv: true },
          ),
        )
        .pipe(Effect.ignore)
      yield* Effect.logInfo("materialized worktree from snapshot packs", {
        worktree: tip.worktree,
        packs: rows.length,
        tree: tip.tree,
      })
    })

    const ensure = Effect.fn("WorktreeMaterializer.ensure")(function* (directory: string) {
      if (yield* fs.existsSafe(directory)) return
      // The newest capture whose session ran in this directory decides which worktree to rebuild.
      const tip = yield* db
        .select()
        .from(SnapshotPackTable)
        .where(eq(SnapshotPackTable.directory, directory))
        .orderBy(desc(SnapshotPackTable.time_created))
        .limit(1)
        .get()
        .pipe(Effect.orDie)
      if (!tip) return
      yield* locks.withLock(tip.worktree)(
        Effect.gen(function* () {
          // Re-check inside the lock (a concurrent drain may have rebuilt it), and never touch a
          // worktree root that already exists: something else owns that tree.
          if (yield* fs.existsSafe(tip.worktree)) return
          yield* materialize(tip).pipe(
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterrupts(cause),
              (cause) =>
                Effect.gen(function* () {
                  // A half-built tree would pass the exists check forever; remove what we created.
                  yield* Effect.promise(() => rm(tip.worktree, { recursive: true, force: true }))
                  yield* Effect.logWarning("failed to materialize worktree", {
                    worktree: tip.worktree,
                    cause,
                  })
                }),
            ),
          )
        }),
      )
    })

    return Service.of({ ensure })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer,
  deps: [Database.node, FSUtil.node, Git.node, AppProcess.node],
})
