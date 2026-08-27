// The state a host's worktree is known to hold, kept in that host's own data directory.
//
// The shared store says what the newest captured tree is; it cannot say whether THIS host has it.
// The git objects cannot say either: a capture writes its tree into the side snapshot repository
// while a materialization indexes packs into the project repository, so "the tree is present" has
// two different answers depending on which host produced it. A host-local note is the one place
// both paths can agree on.
//
// It is written whenever this host captures (snapshot-sync) or rebuilds (execution/worktree), and
// read to answer one question: is this tree behind the store, or ahead of it? Being wrong towards
// "ahead" only leaves a stale tree; being wrong towards "behind" checks out over work nothing else
// holds, so an unreadable or missing note always means "leave the tree alone".

import { mkdir, readFile, writeFile } from "node:fs/promises"
import path from "path"
import { Effect } from "effect"
import { Hash } from "../util/hash"

const noteFile = (data: string, worktree: string) =>
  path.join(data, "worktree-tip", `${Hash.fast(worktree)}.tree`)

export const readWorktreeTip = (data: string, worktree: string) =>
  Effect.promise(() =>
    readFile(noteFile(data, worktree), "utf8").then(
      (text) => text.trim() || undefined,
      () => undefined,
    ),
  )

export const writeWorktreeTip = (data: string, worktree: string, tree: string) =>
  Effect.promise(async () => {
    const file = noteFile(data, worktree)
    await mkdir(path.dirname(file), { recursive: true }).catch(() => {})
    await writeFile(file, tree).catch(() => {})
  })
