// Worktree affinity is only useful if the client starting a workflow and the worker polling for it
// derive the SAME queue name from the same tree. If they disagree they sit on two queues and the
// session waits forever, which is silent: nothing errors, the work simply never runs.
import { describe, it, expect } from "bun:test"
import { mkdtempSync, mkdirSync, realpathSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { queueForWorktree } from "../src/queue"

describe("worktree queue", () => {
  it("gives one tree one name and different trees different names", () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), "queue-")))
    try {
      const a = join(root, "a")
      const b = join(root, "b")
      mkdirSync(a)
      mkdirSync(b)

      expect(queueForWorktree("q", a)).toBe(queueForWorktree("q", a))
      expect(queueForWorktree("q", a)).not.toBe(queueForWorktree("q", b))
      // The base is kept as a readable prefix so a queue is identifiable in the Temporal UI.
      expect(queueForWorktree("q", a).startsWith("q-wt-")).toBe(true)
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  })

  it("agrees whether or not the tree exists locally", () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), "queue-")))
    try {
      const present = join(root, "present")
      mkdirSync(present)
      const absent = join(root, "absent")

      // This is the case that matters. The client hashes the session's directory and the worker
      // hashes the tree it was told to serve, and those two processes do not see the same disk. If
      // the key depended on the path resolving locally, one side would canonicalize and the other
      // would not, and they would sit on different queues with nothing reporting an error.
      expect(queueForWorktree("q", present)).toBe(queueForWorktree("q", join(root, "present")))
      expect(queueForWorktree("q", absent)).toBe(queueForWorktree("q", join(root, "absent")))
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  })

  it("normalizes a path without touching the filesystem", () => {
    expect(queueForWorktree("q", "/srv/trees/acme/")).toBe(queueForWorktree("q", "/srv/trees/acme"))
    expect(queueForWorktree("q", "/srv/trees/./acme")).toBe(
      queueForWorktree("q", "/srv/trees/acme"),
    )
    // Two spellings of one tree are two keys. That is the trade for a key both sides can compute
    // without a filesystem, and it is why the derived queue is logged on both sides.
    expect(queueForWorktree("q", "/tmp/x")).not.toBe(queueForWorktree("q", "/private/tmp/x"))
  })

  it("still names a tree that does not exist yet", () => {
    // A worker can declare the tree it serves before anything has materialized it, so an
    // unresolvable path has to stay a stable key rather than throw.
    const missing = join(tmpdir(), "queue-missing-tree-that-is-not-there")
    expect(queueForWorktree("q", missing)).toBe(queueForWorktree("q", missing))
    expect(queueForWorktree("q", missing).startsWith("q-wt-")).toBe(true)
  })
})
