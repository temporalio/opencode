// Which tool calls are inside their own execution on this host, kept in this host's data directory.
//
// A timeout settles the workflow's promise; it does not stop the process behind it. So after a
// step is abandoned, the host can still be running that step's tool, and nothing the workflow can
// see says whether it is. The step that was abandoned is protected by refusing to move it. What is
// not protected by that is everything after: the turn ends, the next prompt lands wherever there is
// room, and the directory that tool is writing is free again.
//
// A marker is written before a call can have any effect and removed when its body returns. While
// one stands, the directory belongs to that call's step and no other step may rebuild or capture
// it. The refusal outlives the process that made it, on purpose: a marker whose writer died is
// exactly the case where nothing can prove the tool stopped, so the directory stays refused until
// somebody says otherwise. That strands a directory, not a session, because the work is scheduled
// again and another host can take it.

import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises"
import path from "path"
import { Effect } from "effect"
import { Hash } from "../util/hash"

/** What a tool call is, for telling this step's writers from an earlier one's. */
export interface Writer {
  readonly sessionID: string
  readonly step: number
  readonly callID: string
}

interface WriterNote extends Writer {
  readonly pid: number
  readonly started: string
}

const writersDir = (data: string, directory: string) =>
  path.join(data, "worktree-writers", Hash.fast(directory))

const writerFile = (data: string, directory: string, callID: string) =>
  path.join(writersDir(data, directory), `${Hash.fast(callID)}.json`)

/** Say a call is about to write this directory. `endWrite` says its body came back. */
export const beginWrite = (data: string, directory: string, writer: Writer) =>
  Effect.promise(async () => {
    const file = writerFile(data, directory, writer.callID)
    await mkdir(path.dirname(file), { recursive: true }).catch(() => {})
    const note: WriterNote = { ...writer, pid: process.pid, started: new Date().toISOString() }
    await writeFile(file, JSON.stringify(note)).catch(() => {})
  })

/** Whatever it did to the directory, it is not still doing it. */
export const endWrite = (data: string, directory: string, callID: string) =>
  Effect.promise(() => rm(writerFile(data, directory, callID), { force: true }).catch(() => {}))

/**
 * The calls of some other step that never came back. A step's own tools run at once on one host by
 * design, so their markers are not a reason to refuse; a marker from another step is the case the
 * workflow cannot see.
 */
export const strandedWriters = (data: string, directory: string, current?: Writer) =>
  Effect.promise(async () => {
    const dir = writersDir(data, directory)
    const names = await readdir(dir).catch(() => [] as string[])
    const found: WriterNote[] = []
    for (const name of names) {
      const text = await readFile(path.join(dir, name), "utf8").catch(() => undefined)
      if (text === undefined) continue
      try {
        const note = JSON.parse(text) as WriterNote
        if (!current || note.sessionID !== current.sessionID || note.step !== current.step) found.push(note)
      } catch {
        // A note nothing can parse is still a note somebody wrote before running a tool.
        found.push({ sessionID: "unknown", step: -1, callID: name, pid: -1, started: "unknown" })
      }
    }
    return found
  })

/** Forget them, for an operator who has stopped whatever was left running. */
export const clearWriters = (data: string, directory: string) =>
  Effect.promise(async () => {
    const dir = writersDir(data, directory)
    const names = await readdir(dir).catch(() => [] as string[])
    await rm(dir, { recursive: true, force: true }).catch(() => {})
    return names.length
  })
