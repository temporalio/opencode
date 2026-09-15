// Turn-level loop bounds: a run whose model repeats the exact same tool call step after step must
// terminate on its own -- after REPEAT_LIMIT identical steps the next attempt runs as a last step
// (tools disabled, text-only wrap-up) instead of looping forever. Driven end to end through
// SessionRunner.run with a mock LLM that always answers with the same tool call until tools are
// disabled. Also unit-covers the trailing-signature detection.
import type { LLMClientShape } from "@opencode-ai/llm/route"
import { LLMEvent } from "@opencode-ai/llm"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionRunner } from "@opencode-ai/core/session/runner"
import { REPEAT_LIMIT, trailingIdenticalToolSteps } from "@opencode-ai/core/session/runner/loop-guard"
import { ApplicationTools } from "@opencode-ai/core/tool/application-tools"
import { Tool } from "@opencode-ai/core/tool/tool"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { describe, expect, test } from "bun:test"
import { DateTime, Effect, Schema, Stream } from "effect"
import { emptyStep, runnerHarness, seedSession } from "./lib/runner-harness"

// Always answers with the same tool call until tools are disabled, then a bare text-less final step.
// Records each request's toolChoice so the test can assert the guard fired.
const stuckModel = () => {
  // LLM.request normalizes toolChoice into a ToolChoice class; record its `type`.
  const requests: Array<{ readonly tools: number; readonly toolChoice: string | undefined }> = []
  let attempt = 0
  const stream: LLMClientShape["stream"] = (request) => {
    const toolChoice = (request.toolChoice as { type?: string } | undefined)?.type
    requests.push({ tools: request.tools.length, toolChoice })
    attempt++
    if (toolChoice === "none" || request.tools.length === 0) return emptyStep(request)
    return Stream.fromIterable([
      LLMEvent.stepStart({ index: 0 }),
      LLMEvent.toolCall({ id: `call_${attempt}`, name: "probe_stuck", input: { target: "same" } }),
      LLMEvent.stepFinish({ index: 0, reason: "tool-calls" }),
    ])
  }
  return { requests, stream }
}

const sessionID = SessionV2.ID.make("ses_loop_guard")

describe("SessionRunner loop guard", () => {
  const { requests, stream } = stuckModel()
  runnerHarness(stream).effect("ends a run whose model repeats the same tool call every step", () =>
    Effect.gen(function* () {
      yield* seedSession(sessionID)
      yield* (yield* ApplicationTools.Service).register({
        probe_stuck: Tool.make({
          description: "always same result",
          input: Schema.Struct({ target: Schema.String }),
          output: Schema.String,
          toModelOutput: ({ output }) => [{ type: "text", text: output }],
          execute: () => Effect.succeed("unchanged"),
        }),
      })
      const runner = yield* SessionRunner.Service
      // Terminates on its own: REPEAT_LIMIT identical tool steps, then one text-only wrap-up step.
      yield* runner.run({ sessionID, force: true })
      const context = yield* (yield* SessionStore.Service).context(sessionID)
      const assistants = context.filter((message) => message.type === "assistant")
      expect(assistants).toHaveLength(REPEAT_LIMIT + 1)
      const last = assistants.at(-1)
      expect(last?.type === "assistant" ? last.content.filter((part) => part.type === "tool") : undefined).toEqual([])
      // The guard, not the model, ended the run: the final request had tools disabled.
      expect(requests.at(-1)?.tools).toBe(0)
      expect(requests.at(-1)?.toolChoice).toBe("none")
      expect(requests).toHaveLength(REPEAT_LIMIT + 1)
    }),
  )

  test("trailing-signature detection counts only consecutive identical non-empty tool steps", () => {
    const created = DateTime.makeUnsafe(0)
    const assistant = (value: string, tools: Array<{ name: string; input: Record<string, unknown> }>) =>
      SessionMessage.Assistant.make({
        id: SessionMessage.ID.make(`msg_${value}`),
        type: "assistant",
        agent: "build",
        model: { id: ModelV2.ID.make("model"), providerID: ProviderV2.ID.make("provider") },
        content: tools.map((tool) =>
          SessionMessage.AssistantTool.make({
            type: "tool",
            id: `call_${value}_${tool.name}`,
            name: tool.name,
            time: { created },
            state: SessionMessage.ToolStateRunning.make({
              status: "running",
              input: tool.input,
              structured: {},
              content: [],
            }),
          }),
        ),
        time: { created, completed: created },
      })
    const same = { name: "read", input: { path: "a.txt" } }
    const other = { name: "edit", input: { path: "a.txt", change: 1 } }

    expect(trailingIdenticalToolSteps([assistant("1", [same]), assistant("2", [same]), assistant("3", [same])])).toBe(3)
    // A step that also does different work breaks the run (iterating, not stuck).
    expect(
      trailingIdenticalToolSteps([assistant("1", [same]), assistant("2", [same, other]), assistant("3", [same])]),
    ).toBe(1)
    // A text-only step breaks the run.
    expect(trailingIdenticalToolSteps([assistant("1", [same]), assistant("2", []), assistant("3", [same])])).toBe(1)
  })
})
