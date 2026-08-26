// Worktree affinity is only useful if the client starting a workflow and the worker polling for it
// derive the SAME queue name from the same tree. If they disagree they sit on two queues and the
// session waits forever, which is silent: nothing errors, the work simply never runs.
import { describe, it, expect } from "bun:test"
import { mkdtempSync, mkdirSync, realpathSync, rmSync, symlinkSync } from "node:fs"
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

  it("agrees on a tree reached through a symlink", () => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), "queue-")))
    try {
      const real = join(root, "real")
      const link = join(root, "link")
      mkdirSync(real)
      symlinkSync(real, link)

      // This is the case that actually bites: on macOS /tmp is a symlink to /private/tmp, so a
      // client saying /tmp/x and a worker saying /private/tmp/x mean one tree. Keying on the raw
      // string would put them on two queues and the session would hang with nothing to show for it.
      expect(queueForWorktree("q", link)).toBe(queueForWorktree("q", real))
    } finally {
      rmSync(root, { recursive: true, force: true })
    }
  })

  it("still names a tree that does not exist yet", () => {
    // A worker can declare the tree it serves before anything has materialized it, so an
    // unresolvable path has to stay a stable key rather than throw.
    const missing = join(tmpdir(), "queue-missing-tree-that-is-not-there")
    expect(queueForWorktree("q", missing)).toBe(queueForWorktree("q", missing))
    expect(queueForWorktree("q", missing).startsWith("q-wt-")).toBe(true)
  })
})
