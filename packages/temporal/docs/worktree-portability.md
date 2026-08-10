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

## C. Reconstruct the worktree from snapshots (long-term)

The engine already captures git-tree snapshots around each step (`Step.Started`/`Step.Ended` carry
snapshot ids); today they live in a local per-project git store (`${data}/snapshot`). Point that
store at shared storage and a worker without the worktree can materialize the session's exact file
state on resume: clone the repository, then check out the last recorded snapshot tree. This is the
only option that gives true any-worker resume including uncommitted changes. Its honest limits:
materialization latency on first touch, and snapshots capture the git tree, not the world around it
(ignored files, dependencies, running processes), so a reconstructed worktree may still need a
dependency install before `bash` behaves identically.

## Recommendation

Ship **A** as the deployment default (small change, no new infra, correct by construction), allow
**B** where shared volumes already exist, and treat **C** as the future enhancement that removes
the affinity constraint entirely. A and C compose: affinity routes the common case to the warm
worktree; snapshot reconstruction lets a cold worker join the queue after materializing.
