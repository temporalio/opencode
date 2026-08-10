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

// Role split so the worker fleet can run separately from the HTTP server. `both` (default) hosts the
// activity worker AND the workflow client in one process (the serve process). `client` makes serve
// drive workflows without hosting a worker; `worker` runs a standalone activity worker with no HTTP
// surface (see packages/server/src/worker.ts).
const ROLE = process.env.OPENCODE_TEMPORAL_ROLE ?? "both"
const HOST_WORKER = ROLE !== "client"
const HOST_CLIENT = ROLE !== "worker"

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
      if (Cause.hasInterruptsOnly(cause)) {
        // Two interrupt sources: Temporal cancellation (the AbortSignal fired -- rethrow its reason
        // so the attempt records Cancelled, not Failed) and an internal halt like a user declining a
        // permission (the signal did NOT fire). The latter must be non-retryable, or the workflow
        // re-drives a turn the user explicitly stopped.
        if (signal.aborted)
          throw signal.reason instanceof Error ? signal.reason : new Error("session run interrupted")
        throw ApplicationFailure.create({
          message: "session run halted (user declined)",
          type: "SessionRunDeclined",
          nonRetryable: true,
        })
      }
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
      if (Cause.hasInterruptsOnly(cause)) {
        // Same split as the whole-turn drain: cancellation rethrows its reason (records Cancelled),
        // an internal user-decline halt is non-retryable.
        if (signal.aborted)
          throw signal.reason instanceof Error ? signal.reason : new Error("session run interrupted")
        throw ApplicationFailure.create({
          message: "session run halted (user declined)",
          type: "SessionRunDeclined",
          nonRetryable: true,
        })
      }
      const squashed = Cause.squash(cause) as { _tag?: string; message?: string }
      const encoded = encodeRunError(squashed)
      throw ApplicationFailure.create({
        message: squashed?.message ?? Cause.pretty(cause),
        type: squashed?._tag ?? "SessionRunError",
        nonRetryable: true,
        details: encoded === undefined ? undefined : [encoded],
      })
    }

    // Worker connection (native) hosts the runContinuation activity + the workflow. Skipped in
    // client-only role so serve can run without an embedded worker.
    if (HOST_WORKER) {
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
    }

    const SESSION_PREFIX = "session-exec-"

    // Worker-only process: it hosts activities but drives no workflows, so the client methods are
    // unused. Return a service whose driving methods fail loudly if something unexpectedly calls them.
    if (!HOST_CLIENT) {
      yield* Effect.logInfo("SessionExecutionTemporal worker ready").pipe(
        Effect.annotateLogs({ address: ADDRESS, taskQueue: TASK_QUEUE, workflow: WORKFLOW_TYPE, role: ROLE }),
      )
      const clientOnly = Effect.die("SessionExecution client is not hosted when OPENCODE_TEMPORAL_ROLE=worker")
      return SessionExecution.Service.of({
        active: Effect.succeed(new Set<SessionSchema.ID>()),
        wake: () => clientOnly,
        resume: () => clientOnly,
        interrupt: () => clientOnly,
      })
    }

    // Client connection drives the per-session workflows.
    const clientConn = yield* Effect.acquireRelease(
      Effect.promise(() => Connection.connect({ address: ADDRESS })),
      (conn) => Effect.promise(() => conn.close().catch(() => {})),
    )
    const client = new Client({ connection: clientConn, namespace: NAMESPACE })

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
      // set (unlike a process-local Set, which is empty after a restart). Both workflow types are
      // queried so sessions survive a mode switch, and the result is intersected with this store's
      // sessions because visibility is namespace-wide (other deployments sharing the namespace must
      // not appear as ghosts). Visibility is eventually consistent: a just-woken session can lag
      // here by about a second.
      active: Effect.gen(function* () {
        const found = yield* Effect.promise(async () => {
          const ids: SessionSchema.ID[] = []
          for await (const wf of client.workflow.list({
            query: `(WorkflowType = 'sessionExecution' OR WorkflowType = 'sessionTurn') AND ExecutionStatus = 'Running'`,
          })) {
            if (wf.workflowId.startsWith(SESSION_PREFIX)) {
              ids.push(SessionSchema.ID.make(wf.workflowId.slice(SESSION_PREFIX.length)))
            }
          }
          return ids
        })
        const ids = new Set<SessionSchema.ID>()
        for (const id of found) if (yield* store.get(id)) ids.add(id)
        return ids
      }),
      wake: (id) => drive(id).pipe(Effect.asVoid),
      // resume = coordinator.run: drive a forced run via an Update-with-Start and AWAIT its result,
      // so a run error is surfaced to the caller (as a RunError) instead of being swallowed.
      resume: (id) =>
        Effect.tryPromise({
          try: async () => {
            const attempt = () => {
              const startOp = new WithStartWorkflowOperation(WORKFLOW, {
                taskQueue: TASK_QUEUE,
                workflowId: workflowId(id),
                args: [id],
                workflowIdConflictPolicy: "USE_EXISTING",
              })
              return client.workflow.executeUpdateWithStart(WF.resume, {
                startWorkflowOperation: startOp,
                args: [],
              })
            }
            try {
              await attempt()
            } catch (e) {
              // The long-lived workflow self-completes after its idle timeout; an update admitted
              // in that instant fails against the just-completed run instead of starting a fresh
              // one. Retry once so the caller gets a real run, not the race.
              const message = String((e as { message?: unknown })?.message ?? "")
              if (!/already completed|not found/i.test(message)) throw e
              await attempt()
            }
          },
          catch: (e) => toRunError(id, e),
        }),
      interrupt: (id) =>
        Effect.tryPromise({
          try: () => client.workflow.getHandle(workflowId(id)).signal(WF.interrupt),
          catch: (e) => e,
        }).pipe(
          Effect.catch((e) => {
            const message = String((e as { message?: unknown })?.message ?? e)
            // An idle session's workflow has already completed; nothing to interrupt is fine. A
            // genuine delivery failure must not be silent: the user asked for a stop.
            if (/already completed|not found/i.test(message)) return Effect.void
            return Effect.logWarning("session interrupt signal failed").pipe(
              Effect.annotateLogs({ sessionID: id, error: message }),
            )
          }),
        ),
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node],
})
