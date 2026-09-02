// The order snapshot packs go in, decided by the packs themselves rather than by a clock.
//
// Each push chains onto the one before it, so `base` already records the order. `time_created` is
// whichever host wrote the row, and hosts do not agree on the time: a worker five minutes behind
// makes its older tree look like the newest one, and every other host then checks that out over
// the work they were shipped to carry. The chain has no such failure, because a host cannot invent
// a parent it has not seen.
//
// Forks should not happen: only a host standing on the newest state may add to it. They are still
// handled rather than assumed away, because a store written before that rule existed can hold one.
// Depth decides, and the write clock is only the tiebreak between two rows at the same depth.

export interface ChainRow {
  readonly id: string
  readonly base: string | null
  readonly time_created: number
}

const depths = <T extends ChainRow>(rows: readonly T[]): Map<string, number> => {
  const byID = new Map(rows.map((row) => [row.id, row]))
  const depth = new Map<string, number>()
  const of = (row: T): number => {
    const known = depth.get(row.id)
    if (known !== undefined) return known
    // Seeded before recursing, so a row that somehow names an ancestor of itself terminates here
    // instead of running the stack out.
    depth.set(row.id, 0)
    const parent = row.base ? byID.get(row.base) : undefined
    const found = parent ? of(parent as T) + 1 : 0
    depth.set(row.id, found)
    return found
  }
  for (const row of rows) of(row)
  return depth
}

/** Packs in an order where a pack's base always comes before it, which is what indexing them needs. */
export const orderChain = <T extends ChainRow>(rows: readonly T[]): T[] => {
  const depth = depths(rows)
  return [...rows].sort(
    (a, b) => (depth.get(a.id) ?? 0) - (depth.get(b.id) ?? 0) || a.time_created - b.time_created,
  )
}

/** The newest state the store holds, which is the deepest link in the chain. */
export const chainHead = <T extends ChainRow>(rows: readonly T[]): T | undefined => {
  const depth = depths(rows)
  let head: T | undefined
  for (const row of rows) {
    if (!head) {
      head = row
      continue
    }
    const here = depth.get(row.id) ?? 0
    const best = depth.get(head.id) ?? 0
    if (here > best || (here === best && row.time_created > head.time_created)) head = row
  }
  return head
}

/**
 * Whether `tree` is an earlier state than the head, as opposed to one the store has never seen.
 * A tree the store does not hold is this host's own uncaptured work, and moving off it would drop
 * work nothing else has.
 */
export const isBehind = <T extends ChainRow & { readonly tree: string }>(
  rows: readonly T[],
  tree: string,
): boolean => {
  const head = chainHead(rows)
  if (!head || head.tree === tree) return false
  const depth = depths(rows)
  const mine = rows.filter((row) => row.tree === tree)
  if (mine.length === 0) return false
  const deepest = Math.max(...mine.map((row) => depth.get(row.id) ?? 0))
  return deepest < (depth.get(head.id) ?? 0)
}
