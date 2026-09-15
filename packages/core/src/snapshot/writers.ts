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
// that it is over. Four readings answer that, because a tool can put down anything the worker gave
// it: its process is gone, nothing carrying the name the worker exported is running, nothing is
// left in its control group or its process group, and nothing is standing in the directory. The
// machine not having restarted is what makes the pids in those readings mean anything. What is left
// after all of it is a tool that left the group, was handed an environment of somebody else's
// choosing, shares this process's own control group, and writes the directory without being in it.
// Until then the directory stays refused, which strands a directory and not a session: the work is
// scheduled again and another host can take it.

import { execFile } from "node:child_process"
import { randomBytes } from "node:crypto"
import { mkdir, readdir, readFile, readlink, realpath, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "path"
import { promisify } from "node:util"
import { Effect } from "effect"
import { Hash } from "../util/hash"

const run = promisify(execFile)

// Put in the environment rather than kept in memory, because the point of it is to be inherited:
// a tool's children carry it wherever they end up, including through `setsid`, and a scan finds
// them after the worker that spawned them is gone. Fresh per process, so a worker never answers for
// its predecessor's children. Set at the first marker rather than at import, so loading this module
// changes nothing; a marker is written before its tool can spawn anything, so the children inherit
// it all the same.
const WORKER_ENV = "OPENCODE_WORKTREE_WRITER"
let name: string | undefined
const worker = () => (name ??= process.env[WORKER_ENV] = randomBytes(8).toString("hex"))

/** What a tool call is, for telling this step's writers from an earlier one's. */
export interface Writer {
  readonly sessionID: string
  readonly step: number
  readonly callID: string
}

interface WriterNote extends Writer {
  readonly pid: number
  // The group the writer's process was in. What a tool starts stays in it unless it asks for a
  // group of its own, which is what every daemonizing wrapper does.
  readonly pgid?: number
  // The worker that ran the call, as a name it put in its own environment. Everything a tool starts
  // inherits it, so this is what finds a child the group check lost.
  readonly worker?: string
  // The control group the writer was in. A tool keeps it through `setsid` and through `sudo`, and
  // it can be read whoever owns the process, so it is what answers for a tool running as somebody
  // else. Linux only.
  readonly cgroup?: string
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

/** What is running on this host, in the four shapes a tool this worker started can be recognised
 * by. Undefined when the host cannot be asked, which is answered as "everything is still here".
 * `ps` is not installed on a slim container image, which is where most of these run, so Linux is
 * read from `/proc` and everything else asks `ps`. */
export interface LiveHere {
  readonly groups: Set<number>
  readonly workers: Set<string>
  // Which control groups still hold a process. Linux only, and the only reading here that answers
  // for a process running as somebody else: a control group is inherited across fork and exec,
  // `setsid` does not change it, `sudo` does not change it, and `/proc/<pid>/cgroup` is readable
  // whoever owns the process, where that process's environment and working directory are not.
  readonly cgroups: Set<string>
  // Processes standing in the directory itself: their working directory is inside it, or they hold
  // a file under it open. This is the reading that does not depend on the tool having kept anything
  // the worker gave it. This host's own processes only, on both platforms.
  readonly holding: ReadonlyArray<{ readonly pid: number; readonly how: string }>
  // Where this process itself stands. A marker naming either of these is one the reading cannot be
  // asked about: a worker restarted from the same shell is in the group its predecessor was in, and
  // in a container every process shares one control group.
  readonly ourGroup?: number
  readonly ourCgroup?: string
}

const liveHere = async (directory: string): Promise<LiveHere | undefined> => {
  const groups = new Set<number>()
  const workers = new Set<string>()
  const cgroups = new Set<string>()
  const holding: { pid: number; how: string }[] = []
  // Through the links, because a temporary directory on a Mac is reached by one and every reading
  // below comes back resolved. Comparing the two forms finds nothing, quietly.
  const here = await realpath(directory).catch(() => directory)
  const inside = (p?: string) => p !== undefined && (p === here || p.startsWith(`${here}/`))
  const nameOf = (text: string) => {
    // The environment is NUL-separated in `/proc` and space-separated in `ps`, so this reads the
    // name off either without pretending to parse the whole of it.
    const at = text.indexOf(`${WORKER_ENV}=`)
    return at < 0 ? undefined : text.slice(at + WORKER_ENV.length + 1).split(/[\0\s]/)[0]
  }
  // A tool of this worker's own is accounted for by its own marker, and the git subprocesses the
  // snapshot store runs stand in the directory by design. Neither is a writer nobody can account
  // for.
  const ours = (held: string | undefined) => held === worker()

  try {
    if (process.platform === "linux") {
      for (const name of await readdir("/proc")) {
        if (!/^\d+$/.test(name) || name === String(process.pid)) continue
        const stat = await readFile(`/proc/${name}/stat`, "utf8").catch(() => undefined)
        // A command can hold spaces and brackets, so the fields after it are counted from the last
        // close bracket: state, ppid, pgrp.
        const pgrp = stat ? Number.parseInt(stat.slice(stat.lastIndexOf(")") + 2).split(" ")[2], 10) : NaN
        if (Number.isFinite(pgrp)) groups.add(pgrp)
        // Readable whoever owns the process, unlike the two below it.
        const cgroup = await readFile(`/proc/${name}/cgroup`, "utf8").catch(() => undefined)
        const path = cgroup?.split("\n")[0]?.slice(cgroup.indexOf(":", cgroup.indexOf(":") + 1) + 1)
        if (path) cgroups.add(path.trim())
        // Readable for this user's processes, which is what a tool of ours is. Anything else is not
        // something this worker started, unless it went through `sudo`, and that one is what the
        // control group above answers for.
        const held = nameOf(await readFile(`/proc/${name}/environ`, "utf8").catch(() => ""))
        if (held !== undefined) workers.add(held)
        if (ours(held)) continue
        const pid = Number.parseInt(name, 10)
        const cwd = await readlink(`/proc/${name}/cwd`).catch(() => undefined)
        if (inside(cwd)) {
          holding.push({ pid, how: "its working directory is in it" })
          continue
        }
        for (const fd of await readdir(`/proc/${name}/fd`).catch(() => [] as string[])) {
          const open = await readlink(`/proc/${name}/fd/${fd}`).catch(() => undefined)
          if (!inside(open)) continue
          holding.push({ pid, how: `it holds ${open} open` })
          break
        }
      }
      return { groups, workers, cgroups, holding, ourGroup: await group(), ourCgroup: await cgroup() }
    }

    // `-E` prints each process's environment after its command, for the processes this user owns.
    // Without the name in its own environment, and standing somewhere else: `ps` and `lsof` are
    // children of this process, so they inherit what we hold and where we are, and would otherwise
    // report themselves as work this worker left behind.
    const { [WORKER_ENV]: _ours, ...env } = process.env
    const asked = { maxBuffer: 32 * 1024 * 1024, cwd: "/", env }
    const { stdout } = await run("ps", ["-A", "-E", "-o", "pid=,pgid=,command="], asked)
    const named = new Map<number, string | undefined>()
    for (const line of stdout.split("\n")) {
      const [pid, pgid] = line
        .trim()
        .split(/\s+/, 2)
        .map((n) => Number.parseInt(n, 10))
      if (!Number.isFinite(pid) || pid === process.pid) continue
      if (Number.isFinite(pgid)) groups.add(pgid)
      const held = nameOf(line)
      if (held !== undefined) workers.add(held)
      named.set(pid, held)
    }
    // One reading for every process, rather than walking the directory: `lsof +D` stats the whole
    // tree, and the answer wanted here is about the processes, not about the files.
    const cwds = await run("lsof", ["-a", "-d", "cwd", "-Fpn"], asked).catch(() => undefined)
    let at: number | undefined
    for (const line of cwds?.stdout.split("\n") ?? []) {
      if (line.startsWith("p")) at = Number.parseInt(line.slice(1), 10)
      if (!line.startsWith("n") || at === undefined || at === process.pid) continue
      if (inside(line.slice(1)) && !ours(named.get(at))) {
        holding.push({ pid: at, how: "its working directory is in it" })
      }
    }
    return { groups, workers, cgroups, holding, ourGroup: await group(), ourCgroup: await cgroup() }
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

// Nor, without asking, the control group it is in. Absent off Linux, where there are none.
const readCgroup = async (): Promise<string | undefined> => {
  if (process.platform !== "linux") return undefined
  const text = await readFile("/proc/self/cgroup", "utf8").catch(() => undefined)
  const line = text?.split("\n")[0]
  // `<hierarchy>:<controllers>:<path>`, and the path is the part that names the unit or container.
  return line ? line.slice(line.indexOf(":", line.indexOf(":") + 1) + 1).trim() || undefined : undefined
}

let ourCgroup: Promise<string | undefined> | undefined
const cgroup = () => (ourCgroup ??= readCgroup())

/**
 * Why this marker can still be a tool inside its own execution, or nothing when it cannot. That is
 * the only thing the refusal is worth its cost for, and the reason is carried out of here because
 * an operator is who acts on it.
 *
 * Everything here is a reason to stop refusing, never a reason to start: what cannot be answered is
 * answered as still running. Exported for a test, because one of the readings it decides on is
 * Linux's alone and a Mac cannot produce it.
 */
export const insideBecause = (note: WriterNote, live: LiveHere | undefined): string | undefined => {
  // A pid from another machine says nothing here, and two hosts sharing one data directory is the
  // only way to get one. Neither of them can see the other's processes.
  if (note.host !== undefined && note.host !== os.hostname())
    return `it was written on ${note.host}, and this is ${os.hostname()}`
  // Written by a worker that did not date its marker, so there is nothing to tell a live pid from
  // a reused one.
  if (note.bootAt === undefined) return "it does not say which boot its pid belongs to"
  // The machine restarted. Nothing it was running came back with it.
  if (Math.abs(note.bootAt - bootAt()) > SAME_BOOT) return undefined
  if (alive(note.pid)) return `pid ${note.pid} is still running`
  // The writer is gone, and what a tool starts can outlive it. Nothing below is asked of the pids
  // themselves, which come round again; it is asked of what those processes carry and where they
  // stand.
  if (live === undefined) return "this host could not be asked what is running on it"
  // Anything the worker started, wherever it ended up. A tool that daemonizes leaves the group and
  // keeps the environment, which is why this is the check that decides most cases.
  if (note.worker !== undefined && live.workers.has(note.worker)) return "something it started is still running"
  // The control group, for a tool running as somebody else: `sudo` empties the environment it
  // passes on and this survives it. Only when the writer was in a group of its own, which a service
  // manager or a container per worker gives it: in the container this process is in, every process
  // shares one, and finding ourselves there is not evidence about anything.
  if (note.cgroup !== undefined && note.cgroup !== live.ourCgroup && live.cgroups.has(note.cgroup))
    return `something is still in ${note.cgroup}`
  // The process group, for a tool that was given an environment of somebody else's choosing. Only
  // when this process is somewhere else, for the reason above.
  if (note.pgid !== undefined && note.pgid !== live.ourGroup && live.groups.has(note.pgid))
    return `something is still in process group ${note.pgid}`
  // And last, the directory itself. A tool that left the group and kept nothing the worker gave it
  // is still standing where it writes, which is the one thing it cannot put down and go on writing.
  const standing = live.holding[0]
  return standing ? `pid ${standing.pid} is in the directory: ${standing.how}` : undefined
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
      ...((await cgroup()) === undefined ? {} : { cgroup: await cgroup() }),
      worker: worker(),
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
    const live = await liveHere(directory)
    const found: (WriterNote & { because: string })[] = []
    for (const name of names) {
      const file = path.join(dir, name)
      const text = await readFile(file, "utf8").catch(() => undefined)
      if (text === undefined) continue
      let note: WriterNote
      try {
        note = JSON.parse(text) as WriterNote
      } catch {
        // A note nothing can parse is still a note somebody wrote before running a tool.
        found.push({
          sessionID: "unknown",
          step: -1,
          callID: name,
          pid: -1,
          started: "unknown",
          because: "nothing can read what it says",
        })
        continue
      }
      // A marker that cannot be a live tool any more is dropped rather than reported: the refusal
      // exists because nothing could prove the tool stopped, so where something can, it stops
      // standing. Its own step's siblings are not a refusal either way.
      const because = insideBecause(note, live)
      if (because === undefined) {
        await rm(file, { force: true }).catch(() => {})
        continue
      }
      if (!current || note.sessionID !== current.sessionID || note.step !== current.step)
        found.push({ ...note, because })
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
