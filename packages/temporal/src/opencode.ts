// A thin HTTP client for the shipping `opencode serve` API. Used by activities (Node context),
// never from workflow code. The durability layer treats opencode as a black-box server it drives.

const BASE = () => process.env.OPENCODE_BASE_URL ?? "http://127.0.0.1:4599"
const MODEL = () => ({
  providerID: process.env.OPENCODE_PROVIDER ?? "openai",
  modelID: process.env.OPENCODE_MODEL ?? "gpt-5-mini",
})

export interface OcMessage {
  info: { id: string; role: string; time?: { created?: number; completed?: number } }
  parts: Array<{ type: string; text?: string }>
}

async function api(path: string, init?: RequestInit): Promise<Response> {
  const res = await fetch(BASE() + path, init)
  if (!res.ok) {
    const body = await res.text().catch(() => "")
    throw new Error(`opencode ${init?.method ?? "GET"} ${path} -> ${res.status} ${body.slice(0, 300)}`)
  }
  return res
}

export async function createSession(title?: string): Promise<string> {
  const res = await api("/session", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(title ? { title } : {}),
  })
  return (await res.json()).id as string
}

export async function listMessages(sessionID: string): Promise<OcMessage[]> {
  const res = await api(`/session/${sessionID}/message`)
  return (await res.json()) as OcMessage[]
}

// Fire-and-return: the server forks the turn and answers 204 immediately, so the turn keeps
// running even if the caller (our worker) dies. Recovery re-attaches by polling listMessages.
export async function promptAsync(sessionID: string, text: string): Promise<void> {
  await api(`/session/${sessionID}/prompt_async`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ model: MODEL(), parts: [{ type: "text", text }] }),
  })
}

export async function abort(sessionID: string): Promise<void> {
  await api(`/session/${sessionID}/abort`, { method: "POST" }).catch(() => {})
}

// The session is busy for the whole turn (all steps) and idle when it is fully done. Verified: the
// status does not blip idle between steps, so this is a reliable end-of-turn signal.
export async function isIdle(sessionID: string): Promise<boolean> {
  const res = await api("/session/status")
  const map = (await res.json()) as Record<string, { type?: string }>
  return (map[sessionID]?.type ?? "idle") === "idle"
}

export function assistantText(m: OcMessage): string {
  return m.parts
    .filter((p) => p.type === "text" && p.text)
    .map((p) => p.text)
    .join("")
    .trim()
}
