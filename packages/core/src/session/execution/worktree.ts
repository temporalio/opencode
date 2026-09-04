export * as WorktreeMaterializer from "./worktree"

// Brings the project worktree to the newest state in the shared store before a drain runs. This
// closes the host-local gap in cross-host resume: file tools need the tree, and a worker that never
// ran an earlier step either has no tree at all or has one from the step it last ran. The capture
// side (snapshot-sync.ts) ships each snapshot as an incremental git pack; this side indexes every
// pack for the worktree and checks out the newest tree. Ignored files and dependencies are not
// captured, so a bootstrap step (install, build) stays the project's own concern.
//
// A tree is only ever moved forward, and only when this host's own note (snapshot/tip.ts) says it
// is behind the store. What that leaves open: two workers running tools of the SAME step on two
// hosts still cannot see each other's writes, because those are not captured until the step is
// sealed. One worker per worktree is what makes a step's tools share a tree.

import { readdir, rm, writeFile } from "node:fs/promises"
import path from "path"
import { Cause, Context, Effect, Layer, Schema } from "effect"
import { ChildProcess } from "effect/unstable/process"
import { and, asc, desc, eq } from "drizzle-orm"
import { Database } from "../../database/database"
import { makeGlobalNode } from "../../effect/app-node"
import { KeyedMutex } from "../../effect/keyed-mutex"
import { FSUtil } from "../../fs-util"
import { Git } from "../../git"
import { Global } from "../../global"
import { AppProcess } from "../../process"
import { AbsolutePath } from "../../schema"
import { SnapshotPackTable } from "../../snapshot/sql"
import { chainHead, isBehind, orderChain } from "../../snapshot/chain"
import { readWorktreeTip, writeWorktreeTip } from "../../snapshot/tip"

export interface Interface {
  /**
   * Make sure the session's directory holds the newest state the shared store has for it,
   * rebuilding its worktree from stored snapshot packs when it is missing or behind. A directory
   * with no stored packs, and a tree this host has neither built nor captured from, are left
   * alone.
   *
   * A rebuild that fails dies with `WorktreeMaterializeError` rather than returning: running the
   * step against whatever is in the directory tells the model those files are the project, and for
   * a fresh host that is nothing at all. Tagged so the activity boundary retries it elsewhere.
   *
   * `pauseBeforeLock` waits between reading the directory and taking the lock. Zero everywhere but
   * the check that reproduces what a concurrent drain does in that gap: nothing outside this module
   * can hold a caller there, and what the check asserts is the real outcome, whether a failed
   * rebuild removes a directory this call did not create.
   */
  readonly ensure: (
    directory: string,
    options?: { readonly pauseBeforeLock?: number },
  ) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()(
  "@opencode/v2/WorktreeMaterializer",
) {}

// HEAD of a rebuilt tree, so the rebuilt repo reads as a clean checkout rather than an unborn
// branch over a full untracked tree.
const RESTORED = "refs/heads/opencode-restore"

/** A rebuild that did not finish. Tagged so the boundary can tell it from a refusal and retry it. */
export class WorktreeMaterializeError extends Schema.TaggedErrorClass<WorktreeMaterializeError>()(
  "WorktreeMaterializer.MaterializeError",
  { message: Schema.String },
) {}

// Nothing in it at all, so there is no work to protect and nothing to lose by checking a tree out
// over it. Unreadable counts as not empty: a directory we cannot look into is not one to overwrite.
const isEmptyDir = (dir: string) =>
  Effect.promise(() =>
    readdir(dir)
      .then((entries) => entries.length === 0)
      .catch(() => false),
  )

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FSUtil.Service
    const git = yield* Git.Service
    const global = yield* Global.Service
    const proc = yield* AppProcess.Service
    const { db } = yield* Database.Service
    const locks = KeyedMutex.makeUnsafe<string>()

    const materialize = Effect.fnUntraced(function* (tip: typeof SnapshotPackTable.$inferSelect) {
      const worktree = AbsolutePath.make(tip.worktree)
      const repository = yield* git.repo
        .create({ worktree, gitDirectory: AbsolutePath.make(path.join(worktree, ".git")) })
        .pipe(Effect.orDie)
      // Index every pack shipped for this worktree; objects accumulate, the newest tree wins.
      const stored = yield* db
        .select()
        .from(SnapshotPackTable)
        .where(eq(SnapshotPackTable.worktree, tip.worktree))
        .all()
        .pipe(Effect.orDie)
      // A pack cannot be indexed before the one it was built on, and the write clock does not order
      // them: two hosts disagree about the time, and one behind puts its pack first.
      const rows = orderChain(stored)
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
            ["--git-dir", repository.gitDirectory, "update-ref", RESTORED, tip.id],
            { cwd: worktree, extendEnv: true },
          ),
        )
        .pipe(Effect.ignore)
      yield* proc
        .run(
          ChildProcess.make(
            "git",
            ["--git-dir", repository.gitDirectory, "symbolic-ref", "HEAD", RESTORED],
            { cwd: worktree, extendEnv: true },
          ),
        )
        .pipe(Effect.ignore)
      yield* writeWorktreeTip(global.data, tip.worktree, tip.tree)
      yield* Effect.logInfo("materialized worktree from snapshot packs", {
        worktree: tip.worktree,
        packs: rows.length,
        tree: tip.tree,
      })
    })

    // Whether the tree on this host is older than what the store holds. No note means the tree was
    // neither built from packs nor captured from here, so it belongs to whoever put it there. A
    // note the store has never seen means a capture that never shipped, so this host holds work
    // nothing else has and must not be moved back to an older tree.
    const behind = Effect.fnUntraced(function* (tip: typeof SnapshotPackTable.$inferSelect) {
      const held = yield* readWorktreeTip(global.data, tip.worktree)
      if (!held || held === tip.tree) return false
      const rows = yield* db
        .select()
        .from(SnapshotPackTable)
        .where(eq(SnapshotPackTable.worktree, tip.worktree))
        .all()
        .pipe(Effect.orDie)
      return isBehind(rows, held)
    })

    const ensure = Effect.fn("WorktreeMaterializer.ensure")(function* (
      directory: string,
      options?: { readonly pauseBeforeLock?: number },
    ) {
      // The newest capture whose session ran in this directory decides which worktree to rebuild,
      // and which state a tree that is already here has to be brought to.
      const tip = chainHead(
        yield* db
          .select()
          .from(SnapshotPackTable)
          .where(eq(SnapshotPackTable.directory, directory))
          .all()
          .pipe(Effect.orDie),
      )
      if (!tip) return
      // An empty directory is not somebody's working copy, so the rule that protects one does not
      // apply to it. Treating it as present is what stops a fresh host from ever building the tree:
      // it has no tip note, so `behind` says no, and the tools then run against nothing. A mounted
      // path that exists but holds nothing is the ordinary shape of a host that has never seen this
      // project, which is exactly the case the packs are for.
      const present = (yield* fs.existsSafe(tip.worktree)) && !(yield* isEmptyDir(tip.worktree))
      // `behind` is already the whole rule. It is false unless this host has a note of its own, and
      // a note means this host agreed to that state: either it built the tree from packs or it
      // captured the tree from here. Moving it forward from a state it agreed to loses nothing.
      //
      // What used to gate this as well was whether the tree carried the marker `materialize`
      // writes. Only a rebuilt tree ever has that, so a host that seeded the session from its own
      // checkout never did, and once any other host shipped, every activity that host drew died
      // here. A developer's checkout is protected by having no note at all, not by the marker.
      if (present && !(yield* behind(tip))) return
      if (options?.pauseBeforeLock) yield* Effect.sleep(options.pauseBeforeLock)
      yield* locks.withLock(tip.worktree)(
        Effect.gen(function* () {
          // Re-check inside the lock: a concurrent drain may have done this already. Same notion of
          // present as above, or an empty directory bails out here instead and the tree that the
          // outer check just decided to build never gets built.
          const here =
            (yield* fs.existsSafe(tip.worktree)) && !(yield* isEmptyDir(tip.worktree))
          if (here && !(yield* behind(tip))) return
          yield* materialize(tip).pipe(
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterrupts(cause),
              (cause) =>
                Effect.gen(function* () {
                  // A half-built tree would pass the exists check forever, so what we created is
                  // removed. A tree that was already here is not ours to remove: a failed refresh
                  // leaves it as stale as it was. Asked of the reading taken inside the lock, which
                  // is the only one that describes the directory this attempt started from: the
                  // outer one is why the re-check exists, and a drain that materialized while this
                  // one waited makes it name a directory that no longer exists.
                  if (!here)
                    yield* Effect.promise(() => rm(tip.worktree, { recursive: true, force: true }))
                  yield* Effect.logError("failed to materialize worktree", {
                    worktree: tip.worktree,
                    cause,
                  })
                  // Swallowing this ran the step against whatever was in the directory, which for
                  // a fresh host is nothing at all. Tagged, so the activity boundary can retry it:
                  // git and the filesystem fail for reasons that pass, and the alternative is a
                  // turn that fails for good because one worker had a bad minute.
                  return yield* Effect.die(
                    new WorktreeMaterializeError({
                      message: `could not materialize ${tip.worktree} at ${tip.tree}`,
                    }),
                  )
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
  deps: [Database.node, FSUtil.node, Git.node, Global.node, AppProcess.node],
})
