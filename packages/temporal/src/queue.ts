// Routing a session's work to a worker that already has its project tree.
//
// Without affinity every worker polls one queue, and a worker that draws a session whose worktree it
// has never seen rebuilds it from snapshot packs (`session/execution/worktree.ts`). That is the
// portable baseline and it works, but it costs a materialization on the first step a worker takes
// for that session. Affinity avoids the cost by routing instead: the queue name is derived from the
// session's directory, and only workers serving that directory poll it.
//
// The trade is availability for latency, and it is the whole reason this is opt-in. With affinity on,
// a session whose worktree has no worker polling for it does not fall back to another worker; it
// waits. Reconstruction is what makes any worker able to serve any session, and turning affinity on
// is choosing not to use it.
//
// Deliberately NOT imported by workflow code: this reaches for node:crypto, which the Temporal
// sandbox does not have. The queue is chosen by the client that starts the workflow and by the worker
// that polls, both of which are ordinary Node.
import { createHash } from "node:crypto"
import { realpathSync } from "node:fs"

/** Longer than needed to make a collision implausible, short enough to keep the queue name readable
 * in the Temporal UI and in `temporal task-queue describe`. */
const DIGEST_LENGTH = 12

/**
 * The queue for one worktree. Resolved through realpath so two spellings of the same directory agree
 * on a name: on macOS `/tmp/x` and `/private/tmp/x` are the same tree, and a client and a worker that
 * disagreed about that would sit on two queues and never meet. Falls back to the given path when it
 * does not exist yet, which is the case for a worker declaring a tree it has not materialized.
 */
export const queueForWorktree = (base: string, directory: string): string => {
  let canonical = directory
  try {
    canonical = realpathSync(directory)
  } catch {
    // Not present yet; the literal path is still a stable key.
  }
  return `${base}-wt-${createHash("sha256").update(canonical).digest("hex").slice(0, DIGEST_LENGTH)}`
}
