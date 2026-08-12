# Worktree portability across workers

## Problem

The shared event store makes the **conversation** resumable on any worker: history, tool results,
attachments, and credentials are all rebuilt from the DB. The one thing that is not in the DB is the
**project working tree**. File-touching tools (`bash`, `read`, `edit`, `write`, `apply_patch`,
`glob`, `grep`) operate on `Location.directory`, a local filesystem path. A worker that picks up a
session without that worktree resumes the conversation correctly and then acts on a missing (or
wrong) directory. Mid-session uncommitted changes make this worse: they exist only on the disk of
the worker that made them, so even a fresh clone of the repository is not the session's real state.

This is a deployment/design decision, not a bug fix. Three ways to satisfy it, in order of
recommendation.

## A. Session affinity: one task queue per worktree (recommended default)

Temporal-native and no new infrastructure. Derive the task queue from the worktree identity
(`opencode-session-exec@<hash(worktree)>`); a worker registers on the queues for the worktrees whose
filesystem it actually hosts, and the client starts each session's workflow on the queue derived
from the session's location. Activities for a session then only ever land on a worker that has the
session's files.

- Scale-out happens across sessions/worktrees; within one worktree the queue is served by workers
  sharing one filesystem view of it (typically exactly one worker, or one volume).
- Worker loss stalls only that worktree's sessions until a replacement mounts the same volume (the
  Kubernetes PVC-reattach pattern); Temporal re-drives the in-flight step when it comes up.
- Implementation is small: `TASK_QUEUE` in `packages/core/src/session/execution/temporal.ts` is a
  fixed constant today; it becomes a function of the session's location on the client side, and the
  worker side (`packages/server/src/worker.ts`) takes the list of hosted worktrees and registers one
  worker per queue.

## B. Shared filesystem

Mount the worktrees on every worker (NFS/EFS/SMB) and keep the single queue. No code change, and
any worker genuinely can resume any session. The costs are operational: git and build tools over
network filesystems are slow and occasionally surprising, and two sessions sharing one worktree can
collide across hosts just as they can within one (a session's own tools stay serialized either way,
one activity at a time).

## C. Reconstruct the worktree from snapshots (implemented)

The engine already captures git-tree snapshots around each step (`Step.Started`/`Step.Ended` carry
snapshot ids). Those trees now also ride the shared store: after each capture the runner ships the
tree as a git pack (`snapshot-sync.ts` into the `snapshot_pack` table), incremental against the
previous shipped state. Before a drain runs, the worker checks the session's directory and, when
it is missing, rebuilds the worktree from the stored packs
(`session/execution/worktree.ts`): a fresh repo, every pack indexed, the newest tree checked out,
uncommitted edits and untracked files included. Verified by
`packages/core/test/worktree-materialize.test.ts` (capture on one stack, delete the tree,
materialize from the store alone on a second stack).

Honest limits: the tree is rebuilt at the same absolute path it was captured at (a uniform fleet
layout, containers in practice); snapshots capture the git tree, not the world around it (ignored
files, dependencies, running processes), so a reconstructed worktree may still need a dependency
install before `bash` behaves identically; and shipping is best-effort on the capture side, so a
worker that dies between the last capture and its edits loses those edits, exactly as it would
have lost them locally.

## Recommendation

**C is on by default**: any worker can pick up any session and materialize the tree it needs.
Layer **A** on top when worktrees are large or hot (affinity routes the common case to the warm
worktree and skips materialization latency); use **B** where shared volumes already exist. A and C
compose: affinity serves the warm path, snapshot reconstruction lets a cold worker join after
materializing.
