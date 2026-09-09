// The packs form a chain, and the chain is what orders them. `time_created` is whichever host
// wrote the row, and hosts do not agree on the time, so a worker whose clock is behind used to make
// its older tree the newest one that every other host then checked out.
import { describe, expect, test } from "bun:test"
import { chainHead, isBehind, orderChain } from "@opencode-ai/core/snapshot/chain"

const row = (id: string, base: string | null, time: number, tree = `tree-${id}`) => ({
  id,
  base,
  time_created: time,
  tree,
})

describe("snapshot chain", () => {
  test("orders a chain by its links, not by the write clock", () => {
    // Written by a host five minutes behind, so `b` claims an earlier time than its own parent.
    const rows = [row("b", "a", 1_000), row("a", null, 300_000), row("c", "b", 2_000)]
    expect(orderChain(rows).map((r) => r.id)).toEqual(["a", "b", "c"])
  })

  test("the head is the deepest link, whatever the clock says", () => {
    const rows = [row("a", null, 300_000), row("b", "a", 1_000), row("c", "b", 2_000)]
    expect(chainHead(rows)?.id).toBe("c")
  })

  test("an empty store has no head", () => {
    expect(chainHead([])).toBeUndefined()
  })

  test("a fork is decided by depth, and the clock only breaks a tie", () => {
    // `x` and `y` both build on `a`. `y` is deeper, so it wins even though `x` was written later.
    const rows = [row("a", null, 1), row("x", "a", 99_000), row("y", "a", 2), row("z", "y", 3)]
    expect(chainHead(rows)?.id).toBe("z")
  })

  test("a row whose base is not in the store is treated as a root", () => {
    const rows = [row("b", "missing", 5), row("c", "b", 6)]
    expect(orderChain(rows).map((r) => r.id)).toEqual(["b", "c"])
    expect(chainHead(rows)?.id).toBe("c")
  })

  test("behind means earlier in the chain, not earlier on a clock", () => {
    const rows = [row("a", null, 300_000), row("b", "a", 1_000)]
    expect(isBehind(rows, "tree-a")).toBe(true)
    expect(isBehind(rows, "tree-b")).toBe(false)
  })

  test("a tree the store has never seen is this host's own work, not a state behind", () => {
    const rows = [row("a", null, 1), row("b", "a", 2)]
    expect(isBehind(rows, "tree-never-shipped")).toBe(false)
  })

  test("a row that names itself as its base does not run the stack out", () => {
    const rows = [row("a", "a", 1), row("b", "a", 2)]
    expect(() => chainHead(rows)).not.toThrow()
    expect(chainHead(rows)?.id).toBe("b")
  })
})
