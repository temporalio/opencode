// Routing a session's work to a worker that already has its project tree.
//
// Without affinity every worker polls one queue, and a worker that draws a session whose worktree
// it
// has never seen rebuilds it from snapshot packs (`session/execution/worktree.ts`). That is the
// portable baseline and it works, but it costs a materialization on the first step a worker takes
// for that session. Affinity avoids the cost by routing instead: the queue name is derived from the
// session's directory, and only workers serving that directory poll it.
//
// The trade is availability for latency, and it is the whole reason this is opt-in. With affinity
// on,
// a session whose worktree has no worker polling for it does not fall back to another worker; it
// waits. Reconstruction is what makes any worker able to serve any session, and turning affinity on
// is choosing not to use it.
//
// Deliberately NOT imported by workflow code: this reaches for node:crypto, which the Temporal
// sandbox does not have. The queue is chosen by the client that starts the workflow and by the
// worker
// that polls, both of which are ordinary Node.
import { createHash } from "node:crypto"
import { resolve } from "node:path"

/** Longer than needed to make a collision implausible, short enough to keep the queue name readable
 * in the Temporal UI and in `temporal task-queue describe`. */
const DIGEST_LENGTH = 12

/**
 * The queue for one worktree.
 *
 * Derived from the path alone, never from the filesystem. The two sides that have to agree do not
 * see the same disk: the client hashes the session's directory, the worker hashes the tree it was
 * told to serve, and a worker may not have materialized that tree yet. Resolving through the
 * filesystem would make the key depend on whether the path happens to exist locally, so one side
 * would canonicalize and the other would not, and they would sit on different queues while nothing
 * reported an error.
 *
 * The cost is that the operator has to spell the tree the same way on both sides. `/tmp/x` and
 * `/private/tmp/x` are one tree on macOS and two keys here. Both processes log the queue they use,
 * so a mismatch shows up as two names rather than as a session that never runs.
 */
export const queueForWorktree = (base: string, directory: string): string => {
  const canonical = resolve(directory).replace(/[/\\]+$/, "")
  const digest = createHash("sha256").update(canonical).digest("hex").slice(0, DIGEST_LENGTH)
  return `${base}-wt-${digest}`
}
