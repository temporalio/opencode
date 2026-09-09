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
// it. The refusal outlives the process that made it, and takes itself back where this host can show
// that it is over: the writer's process is gone, nothing it started is left in its process group,
// and the machine has not restarted underneath the pids that say so. What is left after that is a
// tool that put itself in another group, which nothing here can follow. Until then the directory
// stays refused, which strands a directory and not a session: the work is scheduled again and
// another host can take it.

import { execFile } from "node:child_process"
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "path"
import { promisify } from "node:util"
import { Effect } from "effect"
import { Hash } from "../util/hash"

const run = promisify(execFile)

/** What a tool call is, for telling this step's writers from an earlier one's. */
export interface Writer {
  readonly sessionID: string
  readonly step: number
  readonly callID: string
}

interface WriterNote extends Writer {
  readonly pid: number
  // The group the writer's process was in. What a tool starts stays in it, so this answers for the
  // children a dead writer left behind.
  readonly pgid?: number
  // When this machine last started, so a pid from before a restart is not read as a live one.
  readonly bootAt?: number
  readonly host?: string
  readonly started: string
}

// os.uptime has second granularity and drifts between reads, so this is a stamp to compare with a
// tolerance rather than an identifier. A restart moves it by the whole of the last uptime.
const bootAt = () => Math.round(Date.now() - os.uptime() * 1000)
const SAME_BOOT = 60_000

const alive = (pid: number) => {
  try {
    process.kill(pid, 0)
    return true
  } catch (err) {
    // Somebody else's process is still a process.
    return (err as NodeJS.ErrnoException).code === "EPERM"
  }
}

// Every process group with something in it, or undefined when this host cannot be asked. `ps` is
// not installed on a slim container image, which is where most of these run, so Linux is read from
// `/proc` and everything else asks `ps`.
const groupsHere = async (): Promise<Set<number> | undefined> => {
  const groups = new Set<number>()
  try {
    if (process.platform === "linux") {
      for (const name of await readdir("/proc")) {
        if (!/^\d+$/.test(name)) continue
        const stat = await readFile(`/proc/${name}/stat`, "utf8").catch(() => undefined)
        // A command can hold spaces and brackets, so the fields after it are counted from the last
        // close bracket: state, ppid, pgrp.
        const pgrp = stat ? Number.parseInt(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[2], 10) : NaN
        if (Number.isFinite(pgrp)) groups.add(pgrp)
      }
      return groups
    }
    const { stdout } = await run("ps", ["-A", "-o", "pgid="], { maxBuffer: 8 * 1024 * 1024 })
    for (const line of stdout.split("\n")) {
      const pgid = Number.parseInt(line.trim(), 10)
      if (Number.isFinite(pgid)) groups.add(pgid)
    }
    return groups
  } catch {
    return undefined
  }
}

const readGroup = async (pid: number): Promise<number | undefined> => {
  try {
    if (process.platform === "linux") {
      const stat = await readFile(`/proc/${pid}/stat`, "utf8")
      const pgrp = Number.parseInt(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[2], 10)
      return Number.isFinite(pgrp) ? pgrp : undefined
    }
    const { stdout } = await run("ps", ["-o", "pgid=", "-p", String(pid)])
    const pgid = Number.parseInt(stdout.trim(), 10)
    return Number.isFinite(pgid) ? pgid : undefined
  } catch {
    return undefined
  }
}

// Once per process: a process cannot change the group it is in.
let ourGroup: Promise<number | undefined> | undefined
const group = () => (ourGroup ??= readGroup(process.pid))

/**
 * Whether this marker can still be a tool inside its own execution, which is the only thing the
 * refusal is worth its cost for. Everything here is a reason to stop refusing, never a reason to
 * start: what cannot be answered is answered as still running.
 */
const maybeInside = async (note: WriterNote, groups: Set<number> | undefined): Promise<boolean> => {
  // A pid from another machine says nothing here, and two hosts sharing one data directory is the
  // only way to get one. Neither of them can see the other's processes.
  if (note.host !== undefined && note.host !== os.hostname()) return true
  // Written by a worker that did not date its marker, so there is nothing to tell a live pid from
  // a reused one.
  if (note.bootAt === undefined) return true
  // The machine restarted. Nothing it was running came back with it.
  if (Math.abs(note.bootAt - bootAt()) > SAME_BOOT) return false
  if (alive(note.pid)) return true
  // The writer is gone, and what a tool starts can outlive it. Those stay in the group the writer
  // was in, so an empty group is the rest of the proof. It only answers for the writer while this
  // process is somewhere else: a worker restarted from the same shell is in the group its
  // predecessor was in, and finding ourselves there is not evidence about anything.
  if (note.pgid === undefined || groups === undefined) return true
  return note.pgid === (await group()) ? false : groups.has(note.pgid)
}

const writersDir = (data: string, directory: string) => path.join(data, "worktree-writers", Hash.fast(directory))

const writerFile = (data: string, directory: string, callID: string) =>
  path.join(writersDir(data, directory), `${Hash.fast(callID)}.json`)

/** Say a call is about to write this directory. `endWrite` says its body came back. */
export const beginWrite = (data: string, directory: string, writer: Writer) =>
  Effect.promise(async () => {
    const file = writerFile(data, directory, writer.callID)
    await mkdir(path.dirname(file), { recursive: true }).catch(() => {})
    const note: WriterNote = {
      ...writer,
      pid: process.pid,
      ...((await group()) === undefined ? {} : { pgid: await group() }),
      bootAt: bootAt(),
      host: os.hostname(),
      started: new Date().toISOString(),
    }
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
    if (names.length === 0) return []
    const groups = await groupsHere()
    const found: WriterNote[] = []
    for (const name of names) {
      const file = path.join(dir, name)
      const text = await readFile(file, "utf8").catch(() => undefined)
      if (text === undefined) continue
      let note: WriterNote
      try {
        note = JSON.parse(text) as WriterNote
      } catch {
        // A note nothing can parse is still a note somebody wrote before running a tool.
        found.push({ sessionID: "unknown", step: -1, callID: name, pid: -1, started: "unknown" })
        continue
      }
      // A marker that cannot be a live tool any more is dropped rather than reported: the refusal
      // exists because nothing could prove the tool stopped, so where something can, it stops
      // standing. Its own step's siblings are not a refusal either way.
      if (!(await maybeInside(note, groups))) {
        await rm(file, { force: true }).catch(() => {})
        continue
      }
      if (!current || note.sessionID !== current.sessionID || note.step !== current.step) found.push(note)
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
