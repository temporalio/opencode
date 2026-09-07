// How much of a real provider's stream arrives AFTER it has asked for its first tool?
//
// That gap is the only thing a stepped executor gives up: a whole-step activity forks the tool
// immediately and runs it under the rest of the stream, while a split has to wait for the attempt
// to
// return. The bench measured the loss as min(tail, tool duration) with a dialled mock. This
// measures
// the tail itself, against the real API, so the mock's dial can be set to something honest.
//
// The measuring point matters. OpenCode forks on the `tool-call` event, which the protocol layer
// emits only once that call's ARGUMENTS are complete and parsed, not when its id first appears.
// Measuring from the id overstates the tail by however long the arguments took to stream.
//
// Measured 2026-08-26, 3 runs per probe (2 for gpt-5), key from ~/.config/ai363/llm.key:
//
//   model                   median tail   max tail   text after first call
//   gpt-4o-mini (chat)             0 ms      50 ms   none, in any probe
//   gpt-5-mini  (responses)       33 ms      36 ms   none, in any probe
//   gpt-5       (responses)       34 ms      77 ms   none, in any probe
//
// So the tail is 0 to 77 ms, at most a few percent of the stream, and no model emitted a single
// character of text after asking for its first tool. A single tool call IS the end of the stream.
// The tail only appears with several calls at once, and is then the time to stream calls 2..N.
//
//   bun run packages/temporal/scripts/stream-tail-probe.ts <model> <chat|responses> <runs>
//
// Reads the key from a file and never prints it.
import { readFileSync } from "node:fs"

const KEY = readFileSync(`${process.env.HOME}/.config/ai363/llm.key`, "utf8").trim()

const TOOLS = [
  {
    name: "bash",
    description: "Run a shell command and return its output.",
    parameters: {
      type: "object",
      properties: { command: { type: "string", description: "The command to run" } },
      required: ["command"],
      additionalProperties: false,
    },
  },
  {
    name: "read_file",
    description: "Read a file and return its contents.",
    parameters: {
      type: "object",
      properties: { path: { type: "string", description: "Path to the file" } },
      required: ["path"],
      additionalProperties: false,
    },
  },
]

interface Probe {
  readonly label: string
  readonly prompt: string
}

// Shapes a coding agent actually produces. The interesting ones are the last two: several tools at
// once (streaming calls 2..N is the tail for call 1) and a model asked to narrate as it goes.
const PROBES: ReadonlyArray<Probe> = [
  { label: "single-call", prompt: "Run `ls -la` in the project root. Use the bash tool." },
  { label: "single-call-terse", prompt: "What files are here? Use the bash tool once." },
  {
    label: "three-calls",
    prompt:
      "Read these three files: src/a.ts, src/b.ts, src/c.ts. Use the read_file tool, one call per file.",
  },
  {
    label: "five-calls",
    prompt:
      "Read these five files: src/a.ts, src/b.ts, src/c.ts, src/d.ts, src/e.ts. Use the read_file tool, one call per file.",
  },
  {
    label: "narrate-then-call",
    prompt:
      "Say one short sentence about what you are going to check, then run `git status` with the bash tool.",
  },
  {
    label: "call-then-narrate",
    prompt:
      "Run `git status` with the bash tool, and in the same reply also write a two-sentence note explaining why you ran it.",
  },
]

interface Result {
  readonly firstToolAt?: number
  readonly lastAt: number
  readonly calls: number
  readonly textAfterFirstCall: number
  readonly totalMs: number
}

const streamChat = async (model: string, prompt: string): Promise<Result> => {
  const started = performance.now()
  const res = await fetch("https://api.openai.com/v1/chat/completions", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({
      model,
      stream: true,
      messages: [{ role: "user", content: prompt }],
      tools: TOOLS.map((t) => ({ type: "function", function: t })),
    }),
  })
  if (!res.ok) throw new Error(`${res.status} ${(await res.text()).slice(0, 200)}`)
  // chat.completions has no per-call "arguments done" event: a call is complete once a LATER index
  // starts streaming, or when finish_reason arrives.
  let openIndex: number | undefined
  return consume(res, started, (obj) => {
    const choice = obj?.choices?.[0]
    const delta = choice?.delta
    const done: string[] = []
    for (const tc of delta?.tool_calls ?? []) {
      if (openIndex !== undefined && tc.index !== openIndex) done.push(`idx${openIndex}`)
      openIndex = tc.index
    }
    if (choice?.finish_reason && openIndex !== undefined) {
      done.push(`idx${openIndex}`)
      openIndex = undefined
    }
    return { toolIds: done, text: typeof delta?.content === "string" ? delta.content.length : 0 }
  })
}

const streamResponses = async (model: string, prompt: string): Promise<Result> => {
  const started = performance.now()
  const res = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: { authorization: `Bearer ${KEY}`, "content-type": "application/json" },
    body: JSON.stringify({
      model,
      stream: true,
      store: false,
      input: [{ role: "user", content: [{ type: "input_text", text: prompt }] }],
      tools: TOOLS.map((t) => ({ type: "function", ...t })),
    }),
  })
  if (!res.ok) throw new Error(`${res.status} ${(await res.text()).slice(0, 200)}`)
  return consume(res, started, (obj) => {
    const ids: string[] = []
    // The fork point: arguments complete, so the call can actually be dispatched.
    if (obj?.type === "response.function_call_arguments.done")
      ids.push(String(obj.item_id ?? obj.output_index))
    const text = obj?.type === "response.output_text.delta" ? String(obj.delta ?? "").length : 0
    return { toolIds: ids, text }
  })
}

const consume = async (
  res: Response,
  started: number,
  parse: (obj: any) => { toolIds: string[]; text: number },
): Promise<Result> => {
  const reader = res.body!.getReader()
  const decoder = new TextDecoder()
  let buffer = ""
  let firstToolAt: number | undefined
  let lastAt = started
  let textAfterFirstCall = 0
  const seen = new Set<string>()

  for (;;) {
    const { done, value } = await reader.read()
    if (done) break
    buffer += decoder.decode(value, { stream: true })
    const lines = buffer.split("\n")
    buffer = lines.pop() ?? ""
    for (const line of lines) {
      if (!line.startsWith("data: ")) continue
      const payload = line.slice(6).trim()
      if (payload === "[DONE]") continue
      let obj: any
      try {
        obj = JSON.parse(payload)
      } catch {
        continue
      }
      const now = performance.now()
      lastAt = now
      const { toolIds, text } = parse(obj)
      for (const id of toolIds) {
        if (seen.has(id)) continue
        seen.add(id)
        if (firstToolAt === undefined) firstToolAt = now
      }
      if (firstToolAt !== undefined && text > 0) textAfterFirstCall += text
    }
  }
  return {
    firstToolAt,
    lastAt,
    calls: seen.size,
    textAfterFirstCall,
    totalMs: Math.round(lastAt - started),
  }
}

const model = process.argv[2] ?? "gpt-4o-mini"
const api = process.argv[3] ?? "chat"
const runs = Number(process.argv[4] ?? 3)

console.log(`\nmodel=${model}  api=${api}  runs=${runs}`)
console.log(
  `${"probe".padEnd(20)}${"calls".padStart(6)}${"stream".padStart(9)}${"tail".padStart(8)}${"tail%".padStart(7)}${"text after".padStart(12)}`,
)

const allTails: number[] = []
for (const probe of PROBES) {
  const tails: number[] = []
  let calls = 0
  let total = 0
  let after = 0
  for (let i = 0; i < runs; i++) {
    try {
      const r =
        api === "responses"
          ? await streamResponses(model, probe.prompt)
          : await streamChat(model, probe.prompt)
      if (r.firstToolAt === undefined) continue
      tails.push(Math.round(r.lastAt - r.firstToolAt))
      calls = Math.max(calls, r.calls)
      total += r.totalMs
      after += r.textAfterFirstCall
    } catch (e) {
      console.log(`${probe.label.padEnd(20)}  ERROR ${(e as Error).message}`)
    }
  }
  if (!tails.length) {
    console.log(`${probe.label.padEnd(20)}${"-".padStart(6)}  (no tool call)`)
    continue
  }
  const median = tails.slice().sort((a, b) => a - b)[Math.floor(tails.length / 2)]!
  const avgTotal = Math.round(total / tails.length)
  const pct = avgTotal > 0 ? Math.round((median / avgTotal) * 100) : 0
  allTails.push(median)
  console.log(
    `${probe.label.padEnd(20)}${String(calls).padStart(6)}${(avgTotal + "ms").padStart(9)}${(median + "ms").padStart(8)}${(pct + "%").padStart(7)}${String(Math.round(after / tails.length) + " ch").padStart(12)}`,
  )
}
if (allTails.length) {
  const sorted = allTails.slice().sort((a, b) => a - b)
  console.log(
    `\nmedian tail across probes: ${sorted[Math.floor(sorted.length / 2)]}ms   max: ${sorted[sorted.length - 1]}ms`,
  )
}
