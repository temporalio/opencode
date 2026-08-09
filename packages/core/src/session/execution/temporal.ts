export * as SessionExecutionTemporal from "./temporal"

import { fileURLToPath } from "node:url"
import { Cause, Effect, Exit, Layer } from "effect"
import { Client, Connection, WithStartWorkflowOperation } from "@temporalio/client"
import { ApplicationFailure } from "@temporalio/activity"
import { NativeConnection, Worker } from "@temporalio/worker"

import { LocationServiceMap } from "../../location-service-map"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionRunner } from "../runner"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { ContextSnapshotDecodeError } from "../error"
import {
  makeActivities,
  makeStepActivities,
  type DrainInput,
  type StepDrainInput,
  type StepDrainResult,
} from "./temporal-activities"
import type { SessionInput } from "../input"
import { encodeRunError, decodeRunError } from "./run-error-codec"
import * as WF from "./temporal-workflow"

const ADDRESS = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
const NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? "default"
const TASK_QUEUE = process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? "opencode-session-exec"
const workflowId = (id: string) => `session-exec-${id}`

// temporal = one activity per turn; temporal-turn = one activity per step (the model call + its
// tools), with the step loop as workflow control flow.
const PER_STEP = process.env.OPENCODE_SESSION_EXECUTION === "temporal-turn"
const WORKFLOW = PER_STEP ? WF.sessionTurn : WF.sessionExecution
const WORKFLOW_TYPE = PER_STEP ? "sessionTurn" : "sessionExecution"

// The v2 RunError union has no generic member, so a run failure surfaced across the durable
// boundary is carried as a ContextSnapshotDecodeError with the original text in `details`. Faithful
// per-member reconstruction (Schema round-trip of the exact tagged error) is a further follow-up.
const toRunError = (sessionID: SessionSchema.ID, e: unknown): SessionRunner.RunError => {
  // Walk the failure chain (WorkflowUpdateFailedError -> ActivityFailure -> ApplicationFailure) to
  // the encoded run error the activity attached, and reconstruct the exact tagged error.
  let node: any = e
  for (let depth = 0; node && depth < 6; depth++) {
    if (Array.isArray(node.details) && node.details.length > 0) {
      const decoded = decodeRunError(node.details[0])
      if (decoded) return decoded
      return new ContextSnapshotDecodeError({ sessionID, details: `session run failed: ${node.message}` })
    }
    node = node.cause
  }
  return new ContextSnapshotDecodeError({ sessionID, details: `session run failed: ${(e as any)?.message ?? String(e)}` })
}

/**
 * A Temporal-backed SessionExecution. It makes each session a durable workflow:
 *   - wake      -> signalWithStart(wake)   (start while idle, or coalesce into the running run)
 *   - resume    -> signalWithStart(force)  (force one drain even with no eligible input)
 *   - interrupt -> signal(interrupt)       (cancels the workflow's scope -> aborts the drain)
 *   - active    -> the set of sessions this process has started
 *
 * The drain itself (SessionRunner.run for the whole turn) is exactly the local coordinator's body,
 * run inside a Temporal activity. Because turn state lives in the durable event log, a worker crash
 * is recovered by re-running the activity: it re-reads recorded history and continues.
 */
const layer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    // The app context the local drain runs in: providing it, then the per-location layer, supplies
    // SessionRunner and all of its dependencies.
    const ctx = yield* Effect.context<SessionStore.Service | LocationServiceMap.Service>()

    const drain = async (input: DrainInput, signal: AbortSignal): Promise<void> => {
      const exit = await Effect.runPromiseExit(
        Effect.gen(function* () {
          const session = yield* store.get(SessionSchema.ID.make(input.sessionID))
          if (!session) return
          yield* SessionRunner.Service.use((runner) =>
            runner.run({ sessionID: session.id, force: input.force }),
          ).pipe(Effect.provide(locations.get(session.location)))
        }).pipe(Effect.provide(ctx), Effect.scoped),
        { signal },
      )
      if (Exit.isSuccess(exit)) return
      const cause = exit.cause
      if (Cause.hasInterruptsOnly(cause)) throw new Error("session run interrupted")
      // A genuine run error is thrown non-retryable so Temporal surfaces it (to resume) rather than
      // retrying; only crashes / task timeouts (never thrown here) go through the retry policy. The
      // error is encoded faithfully in `details` so the caller can reconstruct the exact RunError.
      const squashed = Cause.squash(cause) as { _tag?: string; message?: string }
      const encoded = encodeRunError(squashed)
      throw ApplicationFailure.create({
        message: squashed?.message ?? Cause.pretty(cause),
        type: squashed?._tag ?? "SessionRunError",
        nonRetryable: true,
        details: encoded === undefined ? undefined : [encoded],
      })
    }

    // Per-step drain: run exactly one step of the turn (used by temporal-turn mode). Same context
    // and error encoding as the whole-turn drain; returns the next loop state to the workflow.
    const stepDrain = async (input: StepDrainInput, signal: AbortSignal): Promise<StepDrainResult> => {
      const exit = await Effect.runPromiseExit(
        Effect.gen(function* () {
          const session = yield* store.get(SessionSchema.ID.make(input.sessionID))
          if (!session) return { ran: false, continue: false, step: input.step, promotion: null }
          const r = yield* SessionRunner.Service.use((runner) =>
            runner.runStep({
              sessionID: session.id,
              step: input.step,
              promotion: (input.promotion ?? undefined) as SessionInput.Delivery | undefined,
              first: input.first,
              force: input.force,
            }),
          ).pipe(Effect.provide(locations.get(session.location)))
          return { ran: r.ran, continue: r.continue, step: r.step, promotion: r.promotion ?? null }
        }).pipe(Effect.provide(ctx), Effect.scoped),
        { signal },
      )
      if (Exit.isSuccess(exit)) return exit.value
      const cause = exit.cause
      if (Cause.hasInterruptsOnly(cause)) throw new Error("session run interrupted")
      const squashed = Cause.squash(cause) as { _tag?: string; message?: string }
      const encoded = encodeRunError(squashed)
      throw ApplicationFailure.create({
        message: squashed?.message ?? Cause.pretty(cause),
        type: squashed?._tag ?? "SessionRunError",
        nonRetryable: true,
        details: encoded === undefined ? undefined : [encoded],
      })
    }

    // Worker connection (native) hosts the runContinuation activity + the workflow.
    const nativeConn = yield* Effect.acquireRelease(
      Effect.promise(() => NativeConnection.connect({ address: ADDRESS })),
      (conn) => Effect.promise(() => conn.close().catch(() => {})),
    )
    const worker = yield* Effect.promise(() =>
      Worker.create({
        connection: nativeConn,
        namespace: NAMESPACE,
        taskQueue: TASK_QUEUE,
        workflowsPath: fileURLToPath(new URL("./temporal-workflow.ts", import.meta.url)),
        activities: { ...makeActivities(drain), ...makeStepActivities(stepDrain) },
      }),
    )
    const runHandle = worker.run()
    runHandle.catch(() => {})
    yield* Effect.addFinalizer(() =>
      Effect.promise(async () => {
        worker.shutdown()
        await runHandle.catch(() => {})
      }),
    )

    // Client connection drives the per-session workflows.
    const clientConn = yield* Effect.acquireRelease(
      Effect.promise(() => Connection.connect({ address: ADDRESS })),
      (conn) => Effect.promise(() => conn.close().catch(() => {})),
    )
    const client = new Client({ connection: clientConn, namespace: NAMESPACE })
    const SESSION_PREFIX = "session-exec-"

    const drive = (id: SessionSchema.ID) =>
      Effect.promise(() =>
        client.workflow.signalWithStart(WORKFLOW, {
          taskQueue: TASK_QUEUE,
          workflowId: workflowId(id),
          args: [id],
          signal: WF.wake,
          signalArgs: [],
        }),
      )

    yield* Effect.logInfo("SessionExecutionTemporal ready").pipe(
      Effect.annotateLogs({ address: ADDRESS, taskQueue: TASK_QUEUE, workflow: WORKFLOW_TYPE }),
    )

    return SessionExecution.Service.of({
      // Durable and restart-surviving: the open per-session workflows in Temporal ARE the active
      // set (unlike a process-local Set, which is empty after a restart).
      active: Effect.promise(async () => {
        const ids = new Set<SessionSchema.ID>()
        for await (const wf of client.workflow.list({
          query: `WorkflowType = '${WORKFLOW_TYPE}' AND ExecutionStatus = 'Running'`,
        })) {
          if (wf.workflowId.startsWith(SESSION_PREFIX)) {
            ids.add(SessionSchema.ID.make(wf.workflowId.slice(SESSION_PREFIX.length)))
          }
        }
        return ids
      }),
      wake: (id) => drive(id).pipe(Effect.asVoid),
      // resume = coordinator.run: drive a forced run via an Update-with-Start and AWAIT its result,
      // so a run error is surfaced to the caller (as a RunError) instead of being swallowed.
      resume: (id) =>
        Effect.tryPromise({
          try: async () => {
            const startOp = new WithStartWorkflowOperation(WORKFLOW, {
              taskQueue: TASK_QUEUE,
              workflowId: workflowId(id),
              args: [id],
              workflowIdConflictPolicy: "USE_EXISTING",
            })
            await client.workflow.executeUpdateWithStart(WF.resume, {
              startWorkflowOperation: startOp,
              args: [],
            })
          },
          catch: (e) => toRunError(id, e),
        }),
      interrupt: (id) =>
        Effect.promise(() =>
          client.workflow
            .getHandle(workflowId(id))
            .signal(WF.interrupt)
            .catch(() => {}),
        ),
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node],
})
