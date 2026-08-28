// Commands for a session nobody is sitting in front of: start one and walk away, ask the
// deployment what it is still running, and follow one from a machine that never had it.
//
// These are thin HTTP clients on purpose. In a durable deployment the serve processes are
// interchangeable (any of them reads the shared store and signals the same workflows), so a client
// needs an endpoint and a session id, never a particular host. That is the whole reason a session
// can outlive the process that started it, and it is why nothing here imports Temporal.

import type { Argv } from "yargs"
import { cmd } from "./cmd"
import { UI } from "../ui"
import { ServerAuth } from "@/server/auth"

const DEFAULT_URL = "http://127.0.0.1:4096"

type Remote = { readonly url: string; readonly headers: Record<string, string> }

function remote(args: { attach?: string; password?: string; username?: string }): Remote {
  const url = (args.attach ?? process.env["OPENCODE_SERVER"] ?? DEFAULT_URL).replace(/\/+$/, "")
  // No password configured is a valid deployment, so absent auth is absent headers, not an error.
  return { url, headers: ServerAuth.headers({ password: args.password, username: args.username }) ?? {} }
}

async function call<T>(r: Remote, path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${r.url}/api${path}`, {
    ...init,
    headers: { ...r.headers, ...(init?.body ? { "content-type": "application/json" } : {}), ...init?.headers },
  })
  if (!response.ok) {
    const detail = await response.text().catch(() => "")
    throw new Error(`${init?.method ?? "GET"} /api${path} failed: ${response.status} ${detail.slice(0, 200)}`)
  }
  if (response.status === 204) return undefined as T
  const body = (await response.json()) as { data: T }
  return body.data
}

// The remote-facing options every command here shares. Kept in one builder so a second endpoint
// flag can never drift between them.
function remoteOptions(yargs: Argv) {
  return yargs
    .option("attach", {
      type: "string",
      describe: `server to talk to (default ${DEFAULT_URL}, or $OPENCODE_SERVER)`,
    })
    .option("password", { alias: "p", type: "string", describe: "basic auth password" })
    .option("username", { alias: "u", type: "string", describe: "basic auth username" })
    .option("json", { type: "boolean", describe: "print machine-readable output", default: false })
}

interface SessionInfo {
  id: string
  title?: string
  time?: { created?: number; updated?: number }
  location?: { directory?: string }
}

const stamp = (ms?: number) => (ms ? new Date(ms).toISOString().replace("T", " ").slice(0, 19) : "")

// UI.println writes to stderr, which is right for a person and wrong for a pipe. Anything a script
// is meant to read goes to stdout instead.
const emit = (line: string) => process.stdout.write(line + "\n")

export const SessionStartCommand = cmd({
  command: "start <prompt>",
  describe: "start a session, hand it a prompt, and return without waiting for it",
  builder: (yargs: Argv) =>
    remoteOptions(yargs)
      .positional("prompt", { type: "string", describe: "what the agent should do", demandOption: true })
      .option("dir", { type: "string", describe: "session working directory (default: this one)" })
      .option("model", { type: "string", describe: "provider/model, e.g. openai/gpt-5-mini" }),
  handler: async (args) => {
    const r = remote(args)
    try {
      const directory = args.dir ?? process.cwd()
      const session = await call<SessionInfo>(r, "/session", {
        method: "POST",
        body: JSON.stringify({ directory }),
      })
      if (args.model) {
        const slash = args.model.indexOf("/")
        if (slash < 1) throw new Error(`--model wants provider/model, got ${args.model}`)
        const model = { providerID: args.model.slice(0, slash), id: args.model.slice(slash + 1) }
        await call(r, `/session/${session.id}/model`, { method: "POST", body: JSON.stringify({ model }) })
      }
      // The prompt is admitted, not awaited. Whoever is polling the task queue runs the turn, and
      // this process has nothing left to do with it.
      await call(r, `/session/${session.id}/prompt`, {
        method: "POST",
        body: JSON.stringify({ prompt: { text: args.prompt } }),
      })
      if (args.json) {
        emit(JSON.stringify({ id: session.id, directory, url: r.url }))
        return
      }
      emit(session.id)
      UI.println(`  follow it with: opencode session watch ${session.id}`)
    } catch (error) {
      UI.error(error instanceof Error ? error.message : String(error))
      process.exitCode = 1
    }
  },
})

export const SessionRunningCommand = cmd({
  command: "running",
  describe: "list the sessions this deployment is executing right now",
  builder: (yargs: Argv) => remoteOptions(yargs),
  handler: async (args) => {
    const r = remote(args)
    try {
      // Which sessions are running is the executor's answer, not a guess from the transcript: a
      // durable deployment reads it from the running workflows, so it survives a restart of
      // whichever process happens to be answering this call.
      const active = await call<Record<string, { type: string }>>(r, "/session/active")
      const ids = Object.keys(active)
      if (args.json) {
        emit(JSON.stringify(ids.map((id) => ({ id, status: active[id]?.type }))))
        return
      }
      if (ids.length === 0) {
        UI.println("nothing running")
        return
      }
      const all = await call<SessionInfo[]>(r, "/session").catch(() => [] as SessionInfo[])
      const byId = new Map(all.map((s) => [s.id, s]))
      for (const id of ids) {
        const session = byId.get(id)
        const cells = [id, active[id]?.type ?? "?", stamp(session?.time?.updated), session?.title ?? ""]
        emit(cells.join("  "))
      }
    } catch (error) {
      UI.error(error instanceof Error ? error.message : String(error))
      process.exitCode = 1
    }
  },
})

// What a follower prints. The stream carries far more than a person watching wants to read, so this
// keeps the events that say the work moved and drops the token-level ones.
const INTERESTING: Record<string, (data: any) => string | undefined> = {
  "session.next.prompted": () => "prompted",
  "session.next.step.started": () => "step",
  "session.next.tool.called": (d) => `tool ${d.tool}: ${JSON.stringify(d.input ?? {}).slice(0, 120)}`,
  "session.next.tool.success": (d) => `tool ok  ${firstText(d.content).slice(0, 200)}`,
  "session.next.tool.failed": (d) => `tool failed  ${firstText(d.content).slice(0, 200)}`,
  "session.next.text.ended": (d) => (d.text ? `said: ${String(d.text).slice(0, 400)}` : undefined),
  "session.next.step.failed": (d) => `step failed: ${d.error?.message ?? ""}`,
}

function firstText(content: unknown): string {
  if (!Array.isArray(content)) return ""
  const part = content.find((c) => c && typeof c === "object" && (c as any).type === "text") as any
  return part?.text ? String(part.text).trim() : ""
}

export const SessionWatchCommand = cmd({
  command: "watch <sessionID>",
  describe: "follow a running session from anywhere, and exit when it goes idle",
  builder: (yargs: Argv) =>
    remoteOptions(yargs)
      .positional("sessionID", { type: "string", describe: "session to follow", demandOption: true })
      .option("wait", { type: "boolean", default: true, describe: "keep following until the session is idle" }),
  handler: async (args) => {
    const r = remote(args)
    const sessionID = args.sessionID
    try {
      const response = await fetch(`${r.url}/api/session/${sessionID}/event`, { headers: r.headers })
      if (!response.ok || !response.body) throw new Error(`cannot follow ${sessionID}: ${response.status}`)

      // Where a turn ends, from the model's own finish reason: `tool-calls` is the one that means
      // another step follows. The running-session set cannot answer this, because a session stays
      // in it while its supervisor waits out the idle timeout with nothing left to do.
      const turnOver = (event: { type?: string; data?: any }) =>
        event.type === "session.next.step.failed" ||
        (event.type === "session.next.step.ended" && event.data?.finish !== "tool-calls")

      const reader = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer = ""
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split("\n")
        buffer = lines.pop() ?? ""
        for (const raw of lines) {
          const line = raw.startsWith("data:") ? raw.slice(5).trim() : raw.trim()
          if (!line.startsWith("{")) continue
          let event: { type?: string; data?: any }
          try {
            event = JSON.parse(line)
          } catch {
            continue
          }
          if (args.json) {
            emit(line)
          } else {
            const render = event.type ? INTERESTING[event.type] : undefined
            const text = render?.(event.data ?? {})
            if (text) UI.println(`${stamp(event.data?.timestamp)}  ${text}`)
          }
          if (args.wait && turnOver(event)) {
            await reader.cancel().catch(() => {})
            return
          }
        }
      }
    } catch (error) {
      UI.error(error instanceof Error ? error.message : String(error))
      process.exitCode = 1
    }
  },
})
