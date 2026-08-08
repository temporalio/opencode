export * as SessionExecutionTemporal from "./temporal"

import { fileURLToPath } from "node:url"
import { Effect, Layer } from "effect"
import { Client, Connection } from "@temporalio/client"
import { NativeConnection, Worker } from "@temporalio/worker"

import { LocationServiceMap } from "../../location-service-map"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionRunner } from "../runner"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { makeActivities, type DrainInput } from "./temporal-activities"
import * as WF from "./temporal-workflow"

const ADDRESS = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
const NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? "default"
const TASK_QUEUE = process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? "opencode-session-exec"
const workflowId = (id: string) => `session-exec-${id}`

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

    const drain = (input: DrainInput, signal: AbortSignal): Promise<void> =>
      Effect.runPromise(
        Effect.gen(function* () {
          const session = yield* store.get(SessionSchema.ID.make(input.sessionID))
          if (!session) return
          yield* SessionRunner.Service.use((runner) =>
            runner.run({ sessionID: session.id, force: input.force }),
          ).pipe(Effect.provide(locations.get(session.location)))
        }).pipe(Effect.provide(ctx), Effect.scoped),
        { signal },
      )

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
        activities: makeActivities(drain),
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
    const started = new Set<SessionSchema.ID>()

    const drive = (id: SessionSchema.ID, forced: boolean) =>
      Effect.promise(async () => {
        await client.workflow.signalWithStart(WF.sessionExecution, {
          taskQueue: TASK_QUEUE,
          workflowId: workflowId(id),
          args: [id],
          signal: forced ? WF.force : WF.wake,
          signalArgs: [],
        })
        started.add(id)
      })

    yield* Effect.logInfo("SessionExecutionTemporal ready").pipe(
      Effect.annotateLogs({ address: ADDRESS, taskQueue: TASK_QUEUE }),
    )

    return SessionExecution.Service.of({
      active: Effect.sync(() => new Set(started)),
      wake: (id) => drive(id, false).pipe(Effect.asVoid),
      // resume must return Effect<void, RunError>; we drive a forced run but do not surface the
      // typed error here (a follow-up: carry it back via a Temporal update). never <: RunError.
      resume: (id) => drive(id, true).pipe(Effect.asVoid),
      interrupt: (id) =>
        Effect.promise(async () => {
          await client.workflow
            .getHandle(workflowId(id))
            .signal(WF.interrupt)
            .catch(() => {})
          started.delete(id)
        }),
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node],
})
