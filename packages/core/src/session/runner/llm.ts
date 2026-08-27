import {
  LLM,
  LLMClient,
  LLMError,
  LLMEvent,
  Message,
  SystemPart,
  isContextOverflowFailure,
  type ProviderErrorEvent,
} from "@opencode-ai/llm"
import { Cause, DateTime, Effect, FiberSet, Layer, Option, Semaphore, Stream } from "effect"
import { AgentV2 } from "../../agent"
import { Config } from "../../config"
import { Database } from "../../database/database"
import { EventV2 } from "../../event"
import { Location } from "../../location"
import { ModelV2 } from "../../model"
import { PermissionV2 } from "../../permission"
import { ProviderV2 } from "../../provider"
import { QuestionV2 } from "../../question"
import { SystemContext } from "../../system-context/index"
import { SystemContextRegistry } from "../../system-context/registry"
import { SkillGuidance } from "../../skill/guidance"
import { ReferenceGuidance } from "../../reference/guidance"
import { ToolRegistry } from "../../tool/registry"
import { ToolOutputStore } from "../../tool-output-store"
import { SessionContextEpoch } from "../context-epoch"
import { SessionCompaction } from "../compaction"
import { SessionRunDeclinedError } from "../error"
import { SessionEvent } from "../event"
import { SessionHistory } from "../history"
import { SessionInput } from "../input"
import { SessionMessage } from "../message"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import {
  type DeferredToolCall,
  type ModelCallResult,
  type RunError,
  type SealStepInput,
  type StepInput,
  type ToolCallInput,
  type ToolCallResult,
  type TurnAttemptResult,
  Service,
} from "./index"
import { SessionRunnerModel } from "./model"
import { createLLMEventPublisher, emitToolResult, record } from "./publish-llm-event"
import { toLLMMessages } from "./to-llm-message"
import { MAX_STEPS_PROMPT } from "./max-steps"
import { DEFAULT_MAX_STEPS, REPEAT_LIMIT, REPEATED_CALLS_PROMPT, trailingIdenticalToolSteps } from "./loop-guard"
import { Snapshot } from "../../snapshot"
import { SnapshotSync } from "../../snapshot-sync"
import { makeLocationNode } from "../../effect/app-node"
import { llmClient } from "../../effect/app-node-platform"

/**
 * Runs one durable coding-agent Session until it settles.
 *
 * Keep this as orchestration over smaller collaborators rather than rebuilding the legacy
 * `SessionPrompt` monolith. Implement the unchecked items in small reviewed slices:
 *
 * - Session ownership and controls
 *   - [x] Coordinate one local active drain per Session; explicit resumes join and prompt wakeups
 * coalesce.
 *   - [ ] Replace local ownership with durable multi-node ownership when clustered.
 *   - [ ] Mark busy, retrying, idle, interrupted, or terminal-failure status durably.
 *   - [ ] Honor interruption and reject stale work after runtime attachment replacement.
 *   - [x] Honor optional agent step limits.
 *   - [x] Bound provider retries and repeated identical tool calls.
 *
 * - Runtime context assembly
 *   - Track V1 runtime-context parity canonically in `specs/v2/session.md`.
 *
 * - One provider turn
 *   - [x] Translate every projected V2 Session message variant into canonical
 *     `@opencode-ai/llm` messages.
 *   - [ ] Resolve policy-filtered built-in, MCP, plugin, and structured-output tool definitions.
 *   - [x] Stream exactly one `llm.stream(request)` provider turn.
 *   - [x] Persist assistant text and usage events incrementally as they arrive.
 *   - [ ] Persist snapshots, patches, and retry notices incrementally as they arrive.
 *   - [x] Persist reasoning, provider errors, and tool-call events incrementally as they arrive.
 *
 * - Tool settlement and continuation
 *   - [x] Durably record each tool call before side effects begin.
 *   - [x] Authorize and execute recorded local calls through a core-owned registry hook.
 *   - [x] Persist typed success, failure, and provider-executed tool outcomes.
 *   - [x] Start each recorded local call eagerly and await all settlements before continuation.
 *   - [ ] Add scoped runtime context, progress updates, attachment normalization,
 *     plugins, and cancellation settlement.
 *   - [x] Reload projected history and start the next explicit provider turn after local tool
 * results.
 *   - [x] Continue for durable user steering accepted during an active provider turn.
 *   - [ ] Continue for compaction or another continuation condition when required.
 *
 * - Post-run maintenance
 *   - [ ] Settle final status and expose durable output events to replayable consumers.
 *   - [ ] Coalesce streamed deltas and add covering projected-history indexes.
 *   - [ ] Update title, summaries, compaction state, and cleanup in bounded background work.
 *
 * Use `llm.stream(request)` for each provider turn. Keep tool execution and continuation here.
 * Durable continuation recovery remains a separate future slice with an explicit retry policy.
 *
 * The current slice loads V2 history, translates it, resolves a model through a core service, and
 * persists one
 * provider turn. Registry definitions are advertised, local tool calls are settled durably, and an
 * explicit loop starts the next provider turn after local settlement. Configured agent step limits
 * bound the loop.
 */

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2.Service
    const llm = yield* LLMClient.Service
    const agents = yield* AgentV2.Service
    const tools = yield* ToolRegistry.Service
    const models = yield* SessionRunnerModel.Service
    const store = yield* SessionStore.Service
    const location = yield* Location.Service
    const systemContext = yield* SystemContextRegistry.Service
    const skillGuidance = yield* SkillGuidance.Service
    const referenceGuidance = yield* ReferenceGuidance.Service
    const config = yield* Config.Service
    const snapshots = yield* Snapshot.Service
    const snapshotSync = yield* SnapshotSync.Service
    const db = (yield* Database.Service).db
    const compaction = SessionCompaction.make({ events, llm, config: yield* config.entries() })
    const getSession = Effect.fn("SessionRunner.getSession")(function* (sessionID: SessionSchema.ID) {
      const session = yield* store.get(sessionID)
      if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
      return session
    })

    const getContext = Effect.fn("SessionRunner.getContext")(function* (sessionID: SessionSchema.ID) {
      return yield* store.context(sessionID)
    })
    // `preloaded` lets one projected-history read serve the step-entry checks; callers that just
    // mutated the projection must not pass it, or they act on a stale view.
    const failInterruptedTools = Effect.fn("SessionRunner.failInterruptedTools")(function* (
      sessionID: SessionSchema.ID,
      preloaded?: ReadonlyArray<SessionMessage.Message>,
    ) {
      for (const message of preloaded ?? (yield* getContext(sessionID))) {
        if (message.type !== "assistant") continue
        for (const tool of message.content) {
          if (tool.type !== "tool" || (tool.state.status !== "pending" && tool.state.status !== "running")) continue
          yield* events.publish(SessionEvent.Tool.Failed, {
            sessionID,
            timestamp: yield* DateTime.now,
            assistantMessageID: message.id,
            callID: tool.id,
            error: { type: "unknown", message: "Tool execution interrupted" },
            provider: {
              executed: tool.provider?.executed === true,
              ...(tool.provider?.metadata === undefined ? {} : { metadata: tool.provider.metadata }),
            },
          })
        }
      }
    })

    const awaitToolFibers = (fibers: FiberSet.FiberSet<void, ToolOutputStore.Error>) =>
      Effect.raceFirst(FiberSet.join(fibers), FiberSet.awaitEmpty(fibers))

    // A crashed drain leaves durable evidence that the input tables no longer show: promotion
    // consumed the pending row inside the turn's own transaction, so a re-driven activity that
    // checks only hasPending would drop the crashed turn as a no-op. Recoverable work = the latest
    // conversational message is a user prompt with no assistant reply, or the latest assistant is
    // still in flight. An interrupted turn stays excluded because its cleanup completes the
    // assistant via Step.Failed.
    const hasRecoverableWork = Effect.fnUntraced(function* (
      sessionID: SessionSchema.ID,
      preloaded?: ReadonlyArray<SessionMessage.Message>,
    ) {
      const context = preloaded ?? (yield* getContext(sessionID))
      for (let index = context.length - 1; index >= 0; index--) {
        const message = context[index]
        if (message?.type === "assistant") return !message.time.completed
        if (message?.type === "user") return true
      }
      return false
    })

    // Match V1: declining a user prompt halts the loop instead of becoming model-facing tool
    // output.
    const isUserDeclined = (cause: Cause.Cause<unknown>) =>
      cause.reasons.some(
        (reason) =>
          Cause.isDieReason(reason) &&
          (reason.defect instanceof PermissionV2.DeclinedError || reason.defect instanceof QuestionV2.RejectedError),
      )

    type TurnTransition =
      // Automatic compaction completed; rebuild the request from compacted history.
      | { readonly _tag: "ContinueAfterCompaction"; readonly step: number }
      // Overflow compaction completed; rebuild once through the path without overflow recovery.
      | { readonly _tag: "ContinueAfterOverflowCompaction"; readonly step: number }

    class TurnTransitionError extends Error {
      constructor(readonly transition: TurnTransition) {
        super()
      }
    }

    const continueAfterCompaction = (step: number) => new TurnTransitionError({ _tag: "ContinueAfterCompaction", step })
    const continueAfterOverflowCompaction = (step: number) =>
      new TurnTransitionError({ _tag: "ContinueAfterOverflowCompaction", step })

    const loadSystemContext = (agent: AgentV2.Selection) =>
      Effect.all([systemContext.load(), skillGuidance.load(agent), referenceGuidance.load()], {
        concurrency: "unbounded",
      }).pipe(Effect.map(SystemContext.combine))

    const runTurnAttempt = Effect.fn("SessionRunner.runTurn")(function* (
      sessionID: SessionSchema.ID,
      promotion: SessionInput.Delivery | undefined,
      step: number,
      recoverOverflow?: typeof compaction.compactAfterOverflow,
      // When set, tool calls are recorded and returned instead of run, and the step is left open
      // for
      // whoever runs them. This is what lets a durable executor make each call its own unit of
      // work.
      deferTools = false,
    ) {
      const session = yield* getSession(sessionID)
      if (session.location.directory !== location.directory || session.location.workspaceID !== location.workspaceID)
        return yield* Effect.interrupt
      const agent = yield* agents.select(session.agent)
      const initialized = yield* SessionContextEpoch.initialize(db, loadSystemContext(agent), session.id)
      const toolFibers = yield* FiberSet.make<void, ToolOutputStore.Error>()
      const deferred: DeferredToolCall[] = []
      let needsContinuation = false
      let currentStep = step
      if (promotion) {
        const cutoff = yield* EventV2.latestSequence(db, session.id)
        let promoted = 0
        if (promotion === "steer") promoted = yield* SessionInput.promoteSteers(db, events, session.id, cutoff)
        if (promotion === "queue") {
          promoted += Number(yield* SessionInput.promoteNextQueued(db, events, session.id))
          promoted += yield* SessionInput.promoteSteers(db, events, session.id, cutoff)
        }
        if (promoted > 0) currentStep = 1
      }
      const system =
        initialized ?? (yield* SessionContextEpoch.prepare(db, events, loadSystemContext(agent), session.id))
      const model = yield* models.resolve(session)
      const entries = yield* SessionHistory.entriesForRunner(db, session.id, system.baselineSeq)
      const context = entries.map((entry) => entry.message)
      // Two loop bounds, both ending in one final text-only step: a ceiling on provider attempts
      // (the agent's configured limit, else a default so no run is unbounded), and the stuck loop
      // where the model repeats the exact same tool calls step after step. Detection reads only the
      // durable history, so it holds across re-drives too.
      const stuck = trailingIdenticalToolSteps(context) >= REPEAT_LIMIT
      const isLastStep = stuck || currentStep >= (agent.info?.steps ?? DEFAULT_MAX_STEPS)
      const toolMaterialization = isLastStep ? undefined : yield* tools.materialize(agent.info?.permissions)
      const promptCacheKey = /^ses_[0-9a-f]{64}$/.test(session.id) ? session.id.slice(4) : session.id
      const request = LLM.request({
        model,
        providerOptions: { openai: { promptCacheKey } },
        system: [agent.info?.system, system.baseline]
          .filter((part): part is string => part !== undefined && part.length > 0)
          .map(SystemPart.make),
        messages: [
          ...toLLMMessages(context, model),
          ...(isLastStep ? [Message.assistant(stuck ? REPEATED_CALLS_PROMPT : MAX_STEPS_PROMPT)] : []),
        ],
        tools: toolMaterialization?.definitions ?? [],
        toolChoice: isLastStep ? "none" : undefined,
      })
      if (yield* compaction.compactIfNeeded({ sessionID: session.id, entries, model, request }))
        return yield* Effect.die(continueAfterCompaction(currentStep))
      const startSnapshot = yield* snapshots.capture()
      // Ship the pre-step tree so another host can rebuild the worktree; best-effort inside push.
      if (startSnapshot) yield* snapshotSync.push(startSnapshot)
      const publisher = createLLMEventPublisher(events, {
        sessionID: session.id,
        agent: agent.id,
        model: {
          id: ModelV2.ID.make(model.id),
          providerID: ProviderV2.ID.make(model.provider),
          ...(session.model?.variant === undefined ? {} : { variant: session.model.variant }),
        },
        snapshot: startSnapshot,
        deferCalls: deferTools,
      })
      const withPublication = Semaphore.makeUnsafe(1).withPermit
      const publish = (event: LLMEvent, outputPaths: ReadonlyArray<string> = []) =>
        withPublication(publisher.publish(event, outputPaths))
      let overflowFailure: ProviderErrorEvent | undefined
      const providerStream = llm.stream(request).pipe(
        Stream.runForEach((event) =>
          Effect.gen(function* () {
            if (overflowFailure || publisher.hasProviderError()) return
            if (LLMEvent.is.providerError(event)) {
              if (isContextOverflowFailure(event) && !publisher.hasAssistantStarted()) {
                overflowFailure = event
                return
              }
            }
            yield* publish(event)
            if (event.type !== "tool-call" || event.providerExecuted) return
            if (!toolMaterialization) {
              yield* withPublication(publisher.failUnsettledTools("Tools are disabled after the maximum agent steps"))
              return
            }
            needsContinuation = true
            const assistantMessageID = yield* publisher.assistantMessageID(event.id)
            // Tool.Called is already durable (the publish above), so handing the call back is
            // enough
            // for the caller to run it later. Nothing forks here, which is why the step ends when
            // the
            // stream does and the overlap between the model and its tools is lost.
            if (deferTools) {
              deferred.push({
                id: event.id,
                name: event.name,
                input: event.input,
                assistantMessageID,
              })
              return
            }
            yield* Effect.uninterruptibleMask((restore) =>
              restore(
                toolMaterialization.settle({
                  sessionID: session.id,
                  agent: agent.id,
                  assistantMessageID,
                  call: event,
                }),
              ).pipe(
                Effect.flatMap((settlement) =>
                  publish(
                    LLMEvent.toolResult({
                      id: event.id,
                      name: event.name,
                      result: settlement.result,
                      output: settlement.output,
                    }),
                    settlement.outputPaths ?? [],
                  ),
                ),
              ),
            ).pipe(FiberSet.run(toolFibers))
          }),
        ),
        Effect.ensuring(withPublication(publisher.flush())),
      )

      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const stream = yield* restore(providerStream).pipe(Effect.exit)
          const failure =
            stream._tag === "Failure" ? Option.getOrUndefined(Cause.findErrorOption(stream.cause)) : undefined
          if (
            recoverOverflow &&
            !publisher.hasAssistantStarted() &&
            isContextOverflowFailure(overflowFailure ?? failure) &&
            (yield* restore(recoverOverflow({ sessionID: session.id, entries, model, request })))
          )
            return yield* Effect.die(continueAfterOverflowCompaction(currentStep))
          if (overflowFailure) yield* publish(overflowFailure)
          const llmFailure = failure instanceof LLMError ? failure : undefined
          if (llmFailure && !publisher.hasProviderError()) {
            yield* withPublication(publisher.failUnsettledTools("Provider did not return a tool result", true))
            yield* withPublication(publisher.failAssistant(llmFailure.reason.message))
          }
          if (stream._tag === "Failure" && Cause.hasInterrupts(stream.cause)) yield* FiberSet.clear(toolFibers)
          const settled = yield* restore(awaitToolFibers(toolFibers)).pipe(Effect.exit)
          if (settled._tag === "Failure" && isUserDeclined(settled.cause)) {
            yield* FiberSet.clear(toolFibers)
            yield* withPublication(publisher.failUnsettledTools("Tool execution interrupted"))
            return yield* Effect.interrupt
          }
          if (
            (stream._tag === "Failure" && Cause.hasInterrupts(stream.cause)) ||
            (settled._tag === "Failure" && Cause.hasInterrupts(settled.cause))
          ) {
            yield* FiberSet.clear(toolFibers)
            yield* withPublication(publisher.failUnsettledTools("Tool execution interrupted"))
            if (publisher.hasActiveAssistant())
              yield* withPublication(publisher.failAssistant("Provider turn interrupted"))
          }
          if (settled._tag === "Failure" && !Cause.hasInterrupts(settled.cause)) {
            const failure = Cause.squash(settled.cause)
            const message = failure instanceof Error ? failure.message : String(failure)
            yield* withPublication(publisher.failUnsettledTools(`Tool execution failed: ${message}`))
          }
          const stepSettlement = publisher.stepSettlement()
          // Deferred tools have not run yet, so the step is not over and the end snapshot would be
          // taken before their side effects. Whoever runs them seals it, with the settlement below.
          if (stepSettlement && !publisher.hasProviderError() && !deferTools) {
            const endSnapshot = yield* snapshots.capture()
            // Ship the post-step tree: this is the state a resumed step on another host needs.
            if (endSnapshot) yield* snapshotSync.push(endSnapshot)
            const files =
              startSnapshot && endSnapshot
                ? yield* snapshots
                    .files({ from: startSnapshot, to: endSnapshot })
                    .pipe(Effect.catch(() => Effect.succeed(undefined)))
                : undefined
            yield* withPublication(
              events.publish(SessionEvent.Step.Ended, {
                sessionID: session.id,
                timestamp: yield* DateTime.now,
                assistantMessageID: yield* publisher.startAssistant(),
                finish: stepSettlement.finish,
                cost: 0,
                tokens: stepSettlement.tokens,
                snapshot: endSnapshot,
                files,
              }),
            )
          }
          if (publisher.hasProviderError())
            yield* withPublication(publisher.failUnsettledTools("Tool execution interrupted"))
          if (stream._tag === "Success" && !publisher.hasProviderError())
            yield* withPublication(publisher.failUnsettledTools("Provider did not return a tool result", true))
          if (stream._tag === "Failure") return yield* Effect.failCause(stream.cause)
          if (settled._tag === "Failure" && Cause.hasInterrupts(settled.cause))
            return yield* Effect.failCause(settled.cause)
          // A provider turn can finish having published nothing at all (no text, no tool call), and
          // then no assistant message exists for the seal to close. The whole-step path never hits
          // this because it mints one right here, inside Step.Ended. Mint it the same way and carry
          // the id, rather than leave the seal to guess from the projection.
          const assistantMessageID =
            deferTools && stepSettlement && !publisher.hasProviderError()
              ? yield* withPublication(publisher.startAssistant())
              : undefined
          return {
            needsContinuation: !publisher.hasProviderError() && needsContinuation,
            step: currentStep,
            // A provider error already failed every recorded call, so handing them back would only
            // buy a dispatch that reads them as settled.
            calls: (publisher.hasProviderError()
              ? []
              : deferred) as ReadonlyArray<DeferredToolCall>,
            settlement: stepSettlement,
            assistantMessageID,
          }
        }),
      )
    }, Effect.scoped)
    type RunTurn = (
      sessionID: SessionSchema.ID,
      promotion: SessionInput.Delivery | undefined,
      step: number,
      deferTools?: boolean,
    ) => Effect.Effect<TurnAttemptResult, RunError>

    const runAfterOverflowCompaction: RunTurn = Effect.fnUntraced(function* (
      sessionID,
      promotion,
      step,
      deferTools,
    ) {
      return yield* runTurnAttempt(sessionID, promotion, step, undefined, deferTools).pipe(
        Effect.catchDefect(
          Effect.fnUntraced(function* (defect) {
            if (!(defect instanceof TurnTransitionError)) return yield* Effect.die(defect)
            if (defect.transition._tag === "ContinueAfterOverflowCompaction")
              return yield* Effect.die("Post-compaction provider attempt cannot recover another overflow")
            yield* Effect.yieldNow
            return yield* runAfterOverflowCompaction(
              sessionID,
              undefined,
              defect.transition.step,
              deferTools,
            )
          }),
        ),
      )
    })

    const runTurn: RunTurn = Effect.fnUntraced(function* (sessionID, promotion, step, deferTools) {
      return yield* runTurnAttempt(
        sessionID,
        promotion,
        step,
        compaction.compactAfterOverflow,
        deferTools,
      ).pipe(
        Effect.catchDefect(
          Effect.fnUntraced(function* (defect) {
            if (!(defect instanceof TurnTransitionError)) return yield* Effect.die(defect)
            yield* Effect.yieldNow
            if (defect.transition._tag === "ContinueAfterOverflowCompaction")
              return yield* runAfterOverflowCompaction(
              sessionID,
              undefined,
              defect.transition.step,
              deferTools,
            )
            return yield* runTurn(sessionID, undefined, defect.transition.step, deferTools)
          }),
        ),
      )
    })

    const run = Effect.fn("SessionRunner.run")(function* (input: {
      readonly sessionID: SessionSchema.ID
      readonly force: boolean
    }) {
      const hasSteer = yield* SessionInput.hasPending(db, input.sessionID, "steer")
      const hasQueue = hasSteer ? false : yield* SessionInput.hasPending(db, input.sessionID, "queue")
      const entryContext = yield* getContext(input.sessionID)
      const recover =
        !input.force && !hasSteer && !hasQueue &&
        (yield* hasRecoverableWork(input.sessionID, entryContext))
      if (!input.force && !hasSteer && !hasQueue && !recover) return
      yield* failInterruptedTools(input.sessionID, entryContext)
      let promotion: SessionInput.Delivery | undefined = hasSteer ? "steer" : hasQueue ? "queue" : undefined
      let shouldRun = input.force || hasSteer || hasQueue || recover
      while (shouldRun) {
        let needsContinuation = true
        let step = 1
        while (needsContinuation) {
          const result = yield* runTurn(input.sessionID, promotion, step)
          needsContinuation = result.needsContinuation
          step = result.step + 1
          promotion = "steer"
          if (!needsContinuation) needsContinuation = yield* SessionInput.hasPending(db, input.sessionID, "steer")
        }
        shouldRun = yield* SessionInput.hasPending(db, input.sessionID, "queue")
        promotion = shouldRun ? "queue" : undefined
      }
    })

    // Resume a crashed step from the durable log instead of re-streaming it. A Temporal step retry
    // re-invokes runStep on the same log; if the in-flight step already DISPATCHED tools
    // (Tool.Called
    // is recorded before the side effect runs, so a running/completed tool may have run),
    // re-streaming
    // would re-run that side effect and duplicate the assistant message. Instead we close the step
    // from the log: keep completed tool results, fail the ones still unsettled (their result never
    // committed -- we can't know if they ran, so the model redoes them), and publish a synthesized
    // Step.Ended. The model is NOT re-called. Returns undefined when there is nothing to finalize
    // (a
    // fresh step, or a partial with no dispatched tools, which is safe to re-stream). Token/cost
    // metering is 0 for the resumed step only; faithful metering would need a durable step-sealed
    // marker carrying the provider usage.
    const resumeCrashedStep = Effect.fn("SessionRunner.resumeCrashedStep")(function* (
      input: {
        readonly sessionID: SessionSchema.ID
        readonly step: number
      },
      preloaded?: ReadonlyArray<SessionMessage.Message>,
    ) {
      const context = preloaded ?? (yield* getContext(input.sessionID))
      // At most one assistant is in flight (the projector supersedes older ones); it only exists at
      // step entry on a re-drive, never on a fresh step.
      const inFlight = context.findLast(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" && !message.time.completed,
      )
      if (!inFlight) return undefined
      const toolParts = inFlight.content.filter(
        (part): part is SessionMessage.AssistantTool => part.type === "tool",
      )
      const dispatched = toolParts.some(
        (part) => part.state.status === "running" || part.state.status === "completed",
      )
      if (!dispatched) return undefined
      // A tool declared idempotent (a pure read) has no external side effect, so it is safe to
      // re-run: re-settle it for a real result instead of failing it. Everything else still open is
      // failed below -- we cannot know whether a side-effecting tool already ran. Completed tools
      // keep their recorded results either way.
      const session = yield* getSession(input.sessionID)
      const agent = yield* agents.select(session.agent)
      const materialization = yield* tools.materialize(agent.info?.permissions)
      for (const part of toolParts) {
        if (part.state.status !== "running") continue
        if (!materialization.idempotent(part.name)) continue
        const settlement = yield* materialization.settle({
          sessionID: session.id,
          agent: agent.id,
          assistantMessageID: inFlight.id,
          call: LLMEvent.toolCall({ id: part.id, name: part.name, input: part.state.input }),
        })
        yield* emitToolResult(events, {
          sessionID: session.id,
          assistantMessageID: inFlight.id,
          callID: part.id,
          result: settlement.result,
          output: settlement.output,
          outputPaths: settlement.outputPaths,
          provider: {
            executed: part.provider?.executed ?? false,
            ...(part.provider?.metadata === undefined ? {} : { metadata: part.provider.metadata }),
          },
        })
      }
      // Fail whatever is still open (non-idempotent or never dispatched); completed and re-settled
      // tools are terminal now and are skipped.
      yield* failInterruptedTools(input.sessionID)
      const startSnapshot = inFlight.snapshot?.start
      const endSnapshot = yield* snapshots.capture()
      if (endSnapshot) yield* snapshotSync.push(endSnapshot)
      const files =
        startSnapshot && endSnapshot
          ? yield* snapshots
              .files({ from: Snapshot.ID.make(startSnapshot), to: endSnapshot })
              .pipe(Effect.catch(() => Effect.succeed(undefined)))
          : undefined
      const localTools = toolParts.some((part) => part.provider?.executed !== true)
      yield* events.publish(SessionEvent.Step.Ended, {
        sessionID: input.sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID: inFlight.id,
        // "tool-calls" only when a local tool actually needs a follow-up turn; a step whose tools
        // were all provider-executed finalizes as a plain stop.
        finish: localTools ? "tool-calls" : "stop",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
        snapshot: endSnapshot,
        files,
      })
      // A step with local tool calls continues so the model sees the (reused or failed) results.
      return yield* stepContinuation(input.sessionID, localTools, input.step)
    })

    // One iteration of `run`'s loop, exposed so a Temporal workflow can drive the turn one step at
    // a time (each step = one runTurn = one provider attempt + its tools). Semantics match `run`.
    // The checks every step runs before the provider is called. Shared by the whole-step path and
    // the model-only path so the two cannot drift apart. Returns the terminal result when the step
    // is already over, otherwise the promotion the turn should apply.
    const stepPrologue = Effect.fn("SessionRunner.stepPrologue")(function* (input: StepInput) {
      // One projected-history load serves all the entry checks; nothing mutates the projection
      // between them. The turn itself reloads after the first mutation.
      const entryContext = yield* getContext(input.sessionID)
      // Re-drive of a crashed step: finalize it from the log rather than re-calling the model and
      // re-running its already-dispatched tools.
      const resumed = yield* resumeCrashedStep(input, entryContext)
      if (resumed) return { kind: "settled", result: resumed } as const
      let promotion = input.promotion
      if (input.first) {
        const hasSteer = yield* SessionInput.hasPending(db, input.sessionID, "steer")
        const hasQueue = hasSteer ? false : yield* SessionInput.hasPending(db, input.sessionID, "queue")
        // Same recovery gate as `run`: a first-step retry whose prompt was already promoted (and
        // whose crash predates any tool dispatch, so resumeCrashedStep had nothing to finalize)
        // must re-stream, not no-op.
        if (
          !input.force &&
          !hasSteer &&
          !hasQueue &&
          !(yield* hasRecoverableWork(input.sessionID, entryContext))
        )
          return {
            kind: "settled",
            result: { ran: false, continue: false, step: input.step, promotion: undefined },
          } as const
        promotion = hasSteer ? "steer" : hasQueue ? "queue" : undefined
      }
      // Close tools left pending/running by an interrupted attempt before every turn, not just the
      // first. A mid-turn re-drive (first=false, from a Temporal step retry) would otherwise
      // re-stream a request with a dangling tool_use and no tool_result, which the provider rejects
      // -- a retry poison loop. This is a no-op on a healthy step (the prior step settled its
      // tools).
      yield* failInterruptedTools(input.sessionID, entryContext)
      return { kind: "run", promotion } as const
    })

    // What the loop does once a step's provider attempt and its tools are done: a pending steer
    // wins
    // over a queued prompt, and either one keeps the turn going. Shared for the same reason as the
    // prologue, and read after the tools have run so work admitted during the step is seen.
    const stepContinuation = Effect.fn("SessionRunner.stepContinuation")(function* (
      sessionID: SessionSchema.ID,
      hadToolCalls: boolean,
      step: number,
    ) {
      let needsContinuation = hadToolCalls
      if (!needsContinuation)
        needsContinuation = yield* SessionInput.hasPending(db, sessionID, "steer")
      if (needsContinuation)
        return {
          ran: true,
          continue: true,
          step: step + 1,
          promotion: "steer" as SessionInput.Delivery,
        }
      const moreQueue = yield* SessionInput.hasPending(db, sessionID, "queue")
      if (moreQueue) return { ran: true, continue: true, step: 1, promotion: "queue" as SessionInput.Delivery }
      return { ran: true, continue: false, step: step + 1, promotion: undefined }
    })

    const sealStep = Effect.fn("SessionRunner.sealStep")(function* (input: SealStepInput) {
      const context = yield* getContext(input.sessionID)
      const inFlight = context.findLast(
        (message): message is SessionMessage.Assistant =>
          message.type === "assistant" && !message.time.completed,
      )
      // On a retry that lands after Step.Ended was published there is nothing open, but the loop
      // decision still has to come out the same, so it is read off the step we just closed.
      const carried = input.assistantMessageID
      const target =
        (carried
          ? context.findLast(
              (m): m is SessionMessage.Assistant => m.type === "assistant" && m.id === carried,
            )
          : undefined) ??
        inFlight ??
        context.findLast(
          (message): message is SessionMessage.Assistant => message.type === "assistant",
        )
      if (!target) return yield* stepContinuation(input.sessionID, false, input.step)
      const toolParts = target.content.filter(
        (part): part is SessionMessage.AssistantTool => part.type === "tool",
      )
      // A step continues so the model can see its tool results. Provider-executed calls need no
      // follow-up turn, so a step holding only those finalizes as a plain stop.
      const localTools = toolParts.some((part) => part.provider?.executed !== true)
      if (target.time.completed)
        return yield* stepContinuation(
          input.sessionID,
          input.needsContinuation ?? localTools,
          input.step,
        )
      // A dispatch that failed outright leaves its call open. Close it here, or the next attempt
      // sends a request carrying a tool_use with no tool_result and the provider rejects it.
      yield* failInterruptedTools(input.sessionID, context)
      const startSnapshot = target.snapshot?.start
      const endSnapshot = yield* snapshots.capture()
      // Ship the post-step tree: this is the state a later step on another host needs.
      if (endSnapshot) yield* snapshotSync.push(endSnapshot)
      const files =
        startSnapshot && endSnapshot
          ? yield* snapshots
              .files({ from: Snapshot.ID.make(startSnapshot), to: endSnapshot })
              .pipe(Effect.catch(() => Effect.succeed(undefined)))
          : undefined
      yield* events.publish(SessionEvent.Step.Ended, {
        sessionID: input.sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID: target.id,
        // The attempt's own settlement when the caller carried it; otherwise the same fallback the
        // crash-resume path uses, which costs only the metering on that step.
        finish: input.settlement?.finish ?? (localTools ? "tool-calls" : "stop"),
        cost: 0,
        tokens: input.settlement?.tokens ?? {
          input: 0,
          output: 0,
          reasoning: 0,
          cache: { read: 0, write: 0 },
        },
        snapshot: endSnapshot,
        files,
      })
      return yield* stepContinuation(
        input.sessionID,
        input.needsContinuation ?? localTools,
        input.step,
      )
    })


    const runToolCall = Effect.fn("SessionRunner.runToolCall")(function* (input: ToolCallInput) {
      const session = yield* getSession(input.sessionID)
      const assistantMessageID = SessionMessage.ID.make(input.call.assistantMessageID)
      // A point read, not the whole projected history. The call carries the message
      // that owns it, so decoding every message to find one part would make a step
      // cost O(session) per tool instead of O(1).
      // Message ids are looked up on their own, so the session has to be checked too: a call from
      // another session must not resolve here.
      const owner = yield* store.message(assistantMessageID)
      const part =
        owner?.sessionID === input.sessionID && owner.message.type === "assistant"
          ? // findLast to agree with the projector, which writes tool updates the same way.
            owner.message.content.findLast(
              (item): item is SessionMessage.AssistantTool =>
                item.type === "tool" && item.id === input.call.id,
            )
          : undefined
      // The call has to be in the log already: the attempt that produced it recorded its input
      // before handing it over. Missing means the log moved under us (a fence), and running a tool
      // whose call is not recorded would leave an orphan result.
      if (!part || part.type !== "tool")
        return yield* Effect.die(
          `Tool call ${input.call.id} is not recorded on session ${input.sessionID}`,
        )
      // At-least-once: a duplicate dispatch landing after the result did must not run anything.
      if (part.state.status !== "pending" && part.state.status !== "running")
        return { outcome: "already-settled" } as ToolCallResult
      const agent = yield* agents.select(session.agent)
      const materialization = yield* tools.materialize(agent.info?.permissions)
      // `running` means a dispatch already published Tool.Called and was about to run the tool, so
      // the side effect may have happened and nothing that reads the log afterwards can tell. Only
      // a tool that declares itself repeatable runs again. The rest are reported unknown and the
      // model decides, because re-running the `git push` that may already have landed is the worse
      // failure. The attempt number would say the same thing far less precisely: it counts every
      // way a dispatch can die, including the ones that never reached the tool.
      if (part.state.status === "running" && !materialization.idempotent(input.call.name)) {
        yield* events.publish(SessionEvent.Tool.Failed, {
          sessionID: input.sessionID,
          timestamp: yield* DateTime.now,
          assistantMessageID,
          callID: input.call.id,
          error: { type: "unknown", message: "The outcome of this tool call is unknown" },
          provider: { executed: false },
        })
        return { outcome: "unknown" } as ToolCallResult
      }
      // The durable record that this call is being run, published before the tool can do anything.
      // It is also the last point a fenced dispatch dies at: under a superseded owner this publish
      // fails and the tool never runs, instead of running and losing its result.
      yield* events.publish(SessionEvent.Tool.Called, {
        sessionID: input.sessionID,
        timestamp: yield* DateTime.now,
        assistantMessageID,
        callID: input.call.id,
        tool: input.call.name,
        input: record(input.call.input),
        // Deferred calls are never provider-executed: those are filtered out before the hand-off.
        provider: { executed: false },
      })
      const settlement = yield* materialization
        .settle({
          sessionID: input.sessionID,
          agent: agent.id,
          assistantMessageID,
          call: LLMEvent.toolCall({
            id: input.call.id,
            name: input.call.name,
            input: input.call.input,
          }),
        })
        .pipe(
          // The tool itself ran: what failed is storing its output. Letting that fail the dispatch
          // would retry a side effect that already happened and finally tell the model the call was
          // interrupted, which is not what happened to it. The whole-step path reports the same
          // reason the same way. A decline is a defect, so it passes through this untouched.
          Effect.catch((error) => Effect.succeed({ failure: error })),
          // A decline is the user stopping the turn, not a tool that failed. It is named here
          // rather than raised as a bare interrupt, so nothing downstream has to infer from the
          // absence of a cancellation what the user meant.
          Effect.catchCause((cause) =>
            isUserDeclined(cause)
              ? Effect.fail(new SessionRunDeclinedError({ sessionID: input.sessionID }))
              : Effect.failCause(cause),
          ),
        )
      if ("failure" in settlement) {
        const reason =
          settlement.failure instanceof Error
            ? settlement.failure.message
            : String(settlement.failure)
        yield* Effect.uninterruptible(
          events.publish(SessionEvent.Tool.Failed, {
            sessionID: input.sessionID,
            timestamp: yield* DateTime.now,
            assistantMessageID,
            callID: input.call.id,
            error: { type: "unknown", message: `Tool execution failed: ${reason}` },
            provider: { executed: false },
          }),
        )
        return { outcome: "failed" } as ToolCallResult
      }
      // The tool has run by here, so losing the result to an interrupt would hide a
      // side effect that already happened.
      yield* Effect.uninterruptible(emitToolResult(events, {
        sessionID: input.sessionID,
        assistantMessageID,
        callID: input.call.id,
        result: settlement.result,
        output: settlement.output,
        outputPaths: settlement.outputPaths,
        // Deferred calls are never provider-executed: those are filtered out before the hand-off.
        provider: { executed: false },
      }))
      return { outcome: "settled" } as ToolCallResult
    })

    const runStep = Effect.fn("SessionRunner.runStep")(function* (input: StepInput) {
      const prologue = yield* stepPrologue(input)
      if (prologue.kind === "settled") return prologue.result
      const result = yield* runTurn(input.sessionID, prologue.promotion, input.step)
      return yield* stepContinuation(input.sessionID, result.needsContinuation, result.step)
    })

    const runModelCall = Effect.fn("SessionRunner.runModelCall")(function* (input: StepInput) {
      const prologue = yield* stepPrologue(input)
      if (prologue.kind === "settled")
        return { kind: "settled", result: prologue.result } as ModelCallResult
      const result = yield* runTurn(input.sessionID, prologue.promotion, input.step, true)
      return {
        kind: "called",
        step: result.step,
        calls: result.calls,
        settlement: result.settlement,
        assistantMessageID: result.assistantMessageID,
        needsContinuation: result.needsContinuation,
      } as ModelCallResult
    })

    return Service.of({
      run,
      runStep,
      runModelCall,
      runToolCall,
      sealStep,
    })
  }),
)

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [
    EventV2.node,
    llmClient,
    AgentV2.node,
    ToolRegistry.node,
    SessionRunnerModel.node,
    SessionStore.node,
    Location.node,
    SystemContextRegistry.node,
    SkillGuidance.node,
    ReferenceGuidance.node,
    Config.node,
    Snapshot.node,
    SnapshotSync.node,
    Database.node,
  ],
})
