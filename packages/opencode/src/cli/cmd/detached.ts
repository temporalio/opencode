// Commands for a session nobody is sitting in front of: start one and walk away, ask the
// deployment what it is still running, and follow one from a machine that never had it.
//
// These are thin HTTP clients on purpose. In a durable deployment the serve processes are
// interchangeable (any of them reads the shared store and signals the same workflows), so a client
// needs an endpoint and a session id, never a particular host. That is the whole reason a session
// can outlive the process that started it, and it is why nothing here talks to Temporal. The one
// exception is `doctor`, which reads the driver's own configuration module: it answers what this
// deployment resolved, and a second copy of those rules living here is how the two would disagree.

import type { Argv } from "yargs"
import { cmd } from "./cmd"
import { UI } from "../ui"
import { ServerAuth } from "@/server/auth"
import { TemporalConfig } from "@opencode-ai/temporal/config"
// Type-only: the SDK types a duration as a template literal, and this takes one from a person.
import type { Duration } from "@temporalio/common"

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

// What this process resolved, and what is wrong with it. Deploying was a handful of variables that
// have to agree, with no way to ask whether they did: every mistake in them fails as something
// else, hours later, on whoever prompted the session rather than on whoever deployed it.
export const SessionDoctorCommand = cmd({
  command: "doctor",
  describe: "what this deployment resolved, and what is wrong with it",
  builder: (yargs: Argv) => yargs,
  handler: async () => {
    const config = TemporalConfig.fromEnv()
    UI.println("opencode, temporal execution")
    for (const [name, value] of Object.entries(TemporalConfig.describe(config))) {
      UI.println(`  ${name}: ${value}`)
    }
    for (const note of TemporalConfig.notes(config)) UI.println(`note: ${note}`)
    const problems = TemporalConfig.preflight(config)
    for (const problem of problems) UI.println(`problem: ${problem}`)
    if (problems.length > 0) {
      process.exitCode = 1
      return
    }
    UI.println("this deployment looks consistent")
  },
})

// A turn nobody starts. `start` still needs something running to hand the prompt to; a schedule
// does not, which is the difference between a session you can walk away from and one that runs
// without you. The session is created once, here, over HTTP like everything else in this file; the
// firing itself reaches only Temporal, and a deployment with no serve process at all still runs it.
export const SessionScheduleCommand = cmd({
  command: "schedule <prompt>",
  describe: "run a prompt on a schedule, with no client at firing time",
  builder: (yargs: Argv) =>
    remoteOptions(yargs)
      .positional("prompt", { type: "string", describe: "what the agent should do", demandOption: true })
      .option("every", { type: "string", describe: "interval, e.g. 1h" })
      .option("cron", { type: "string", describe: "cron expression, e.g. '0 9 * * *'" })
      .option("id", { type: "string", describe: "schedule id (default: generated)" })
      .option("session", { type: "string", describe: "an existing session to prompt (default: a new one)" })
      .option("dir", { type: "string", describe: "session working directory (default: this one)" }),
  handler: async (args) => {
    const r = remote(args)
    try {
      if (!args.every && !args.cron) throw new Error("schedule wants --every=<duration> or --cron=<expression>")
      const sessionID =
        args.session ??
        (
          await call<SessionInfo>(r, "/session", {
            method: "POST",
            body: JSON.stringify({ directory: args.dir ?? process.cwd() }),
          })
        ).id
      const config = TemporalConfig.fromEnv()
      const { Client, Connection, ScheduleOverlapPolicy } = await import("@temporalio/client")
      const connection = await Connection.connect(TemporalConfig.connectionOptions(config))
      try {
        const client = new Client({ connection, namespace: config.namespace })
        const scheduleId = args.id ?? `opencode-${sessionID}`
        await client.schedule.create({
          scheduleId,
          spec: {
            ...(args.cron ? { cronExpressions: [args.cron] } : {}),
            ...(args.every ? { intervals: [{ every: args.every as Duration }] } : {}),
          },
          // A firing that lands while the last one is still working is skipped rather than queued.
          // An agent task is not a metrics scrape: two of them on one project is a bad day.
          policies: { overlap: ScheduleOverlapPolicy.SKIP },
          action: {
            type: "startWorkflow",
            workflowType: "scheduledPrompt",
            taskQueue: config.taskQueue,
            args: [
              {
                sessionID,
                text: args.prompt,
                session: { idleTimeout: config.idleTimeout, stepped: config.stepped === true },
              },
            ],
          },
        })
        if (args.json) {
          emit(JSON.stringify({ schedule: scheduleId, session: sessionID }))
          return
        }
        emit(scheduleId)
        UI.println(`  every firing prompts ${sessionID}; follow it with: opencode session watch ${sessionID}`)
      } finally {
        await connection.close()
      }
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

    // The turn saying so itself, which is the only thing that knows: a step ending is not a turn
    // ending, because a steer or a queued prompt continues the same turn. It covers the ordinary
    // ending only, so the reasons below are still what a stopped or failed turn ends this on.
    const turnEnded = (event: { type?: string }) => event.type === "session.next.turn.ended"

    // A step ending is where a turn usually ends, from the model's own finish reason: `tool-calls`
    // is the one that means another step follows. It is not on its own proof the turn is over,
    // because a steer or a queued prompt continues it, so the executor's own answer decides.
    const looksDone = (event: { type?: string; data?: any }) =>
      event.type === "session.next.step.failed" ||
      (event.type === "session.next.step.ended" && event.data?.finish !== "tool-calls")

    // What says the turn did NOT end after all: a steer or a queued prompt continues the same turn
    // through `stepContinuation`, and the next thing on the wire is another step starting.
    const carriesOn = (event: { type?: string }) =>
      event.type === "session.next.step.started" || event.type === "session.next.prompted"

    // The running set cannot end a `watch` on its own. It holds a session for the whole idle
    // timeout after the work is done, so gating the exit on it means never exiting. Nothing
    // publishes a turn-level ending either, so what usually ends this is the wire going quiet: a
    // terminal step, then no continuation within a grace window. A steer arrives in milliseconds,
    // so the window only has to outlast the hop between two activities.
    const GRACE_MS = Number(process.env.OPENCODE_WATCH_GRACE_MS ?? 5_000)
    // The wire cannot answer the other case. A turn that ends while this is reconnecting publishes
    // its last step into a gap, and the stream has no replay, so nothing arrives afterwards and the
    // quiet means nothing. Absence from the running set is slow but certain, and it is the one
    // reading that only ever says "finished", so it can end a watch without being able to hang one.
    const POLL_MS = Number(process.env.OPENCODE_WATCH_POLL_MS ?? 30_000)
    const inactive = async () => {
      const active = await call<Record<string, unknown>>(r, "/session/active").catch(() => undefined)
      // Unreachable is not finished, which is the whole point of this command.
      return active !== undefined && !(sessionID in active)
    }

    // Kept across reconnects. The terminal step lands on one connection and the quiet that follows
    // it on the next, and starting this again per connection is what followed a finished turn for
    // as long as the terminal stayed open.
    let settleAt: number | undefined

    // The stream ends when the serve this is attached to restarts, which is the event this command
    // exists for. Exiting 0 there reports a turn that is still running as done.
    const attempts = 30
    try {
      for (let attempt = 0; ; attempt++) {
        const response = await fetch(`${r.url}/api/session/${sessionID}/event`, {
          headers: r.headers,
        }).catch(() => undefined)
        if (!response?.ok || !response.body) {
          if (attempt >= attempts)
            throw new Error(`cannot follow ${sessionID}: ${response?.status ?? "unreachable"}`)
          await new Promise((resolve) => setTimeout(resolve, 1000))
          continue
        }

        const reader = response.body.getReader()
        const decoder = new TextDecoder()
        let buffer = ""
        let ended = false
        // Held across iterations rather than started fresh each time: a read that loses the race is
        // still queued on the stream, and dropping it drops whatever it goes on to deliver.
        let pending: ReturnType<typeof reader.read> | undefined
        for (;;) {
          const next = (pending ??= reader.read())
          let timer: ReturnType<typeof setTimeout> | undefined
          const chunk = args.wait
            ? await Promise.race([
                next,
                new Promise<"quiet">((resolve) => {
                  const wait = settleAt ? Math.max(0, settleAt - Date.now()) : POLL_MS
                  timer = setTimeout(() => resolve("quiet"), wait)
                }),
              ])
            : await next
          if (timer) clearTimeout(timer)
          if (chunk === "quiet") {
            // A terminal step and then nothing: the turn is over. Otherwise this is the periodic
            // ask, and only the running set can end the wait.
            if ((settleAt && Date.now() >= settleAt) || (await inactive())) {
              ended = true
              break
            }
            continue
          }
          pending = undefined
          const { done, value } = chunk
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
            if (args.wait) {
              // The turn saying it is over ends this now: there is nothing to wait out, and the
              // grace window exists only because nothing used to say it.
              if (turnEnded(event)) {
                ended = true
                break
              }
              if (carriesOn(event)) settleAt = undefined
              else if (looksDone(event)) settleAt = Date.now() + GRACE_MS
            }
          }
        }
        await reader.cancel().catch(() => {})
        // Without `--wait` the stream itself is the whole command, so its end is this one's too.
        if (ended || !args.wait) return
        // The stream dropped with the turn still going. Reconnect and keep following.
        if (attempt >= attempts) throw new Error(`lost the stream for ${sessionID}`)
        await new Promise((resolve) => setTimeout(resolve, 1000))
      }
    } catch (error) {
      UI.error(error instanceof Error ? error.message : String(error))
      process.exitCode = 1
    }
  },
})
