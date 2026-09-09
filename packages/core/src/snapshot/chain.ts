// Parent depth avoids ordering an intact chain by clocks from different hosts.
// Forks still use the timestamp tiebreaker. Ordering does not reject a competing publication.

export interface ChainRow {
  readonly id: string
  readonly base: string | null
  readonly time_created: number
}

const depths = <T extends ChainRow>(rows: readonly T[]): Map<string, number> => {
  const byID = new Map(rows.map((row) => [row.id, row]))
  const depth = new Map<string, number>()
  // Iterative, because the chain is one link per capture and nothing prunes it: a long session
  // would put a stack frame per tool call that changed a file.
  for (const start of rows) {
    if (depth.has(start.id)) continue
    const pending: T[] = []
    const seen = new Set<string>()
    let at: T | undefined = start
    while (at && !depth.has(at.id) && !seen.has(at.id)) {
      seen.add(at.id)
      pending.push(at)
      at = at.base ? (byID.get(at.base) as T | undefined) : undefined
    }
    // A root, a row whose base is not in the store, or a cycle: all start the count at zero.
    let below = at && depth.has(at.id) ? depth.get(at.id)! : -1
    for (const row of pending.reverse()) depth.set(row.id, ++below)
  }
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
