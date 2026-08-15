// The interrupt path must not report a genuine signal-delivery failure as success. Only an already
// closed workflow ("already completed"/"not found") is a benign no-op; everything else must fail.
import { describe, it, expect } from "bun:test"
import { SessionExecutionTemporal } from "../src/executor"

const classify = SessionExecutionTemporal.classifyInterruptError

describe("temporal interrupt error classification", () => {
  it("ignores an already-closed workflow", () => {
    expect(classify(new Error("workflow execution already completed"))).toBe("ignore")
    expect(classify(new Error("workflow not found"))).toBe("ignore")
  })

  it("fails a genuine delivery error", () => {
    expect(classify(new Error("14 UNAVAILABLE: tcp connect error"))).toBe("fail")
    expect(classify(new Error("DEADLINE_EXCEEDED"))).toBe("fail")
    expect(classify("some non-error value")).toBe("fail")
  })
})
