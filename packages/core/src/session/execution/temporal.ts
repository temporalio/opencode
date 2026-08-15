export * as SessionExecutionTemporal from "./temporal"

import { fileURLToPath } from "node:url"
import { Effect, Layer } from "effect"
import { Client, Connection, WithStartWorkflowOperation } from "@temporalio/client"
// Imported lazily inside the worker branch: the worker package drags webpack and swc (it bundles
// the workflow from source at startup), which a compiled binary can neither bundle nor run. A
// packaged serve runs OPENCODE_TEMPORAL_ROLE=client next to standalone workers instead.

import { LocationServiceMap } from "../../location-service-map"
import { EventV2 } from "../../event"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { makeStepActivities } from "./temporal-activities"
import { makeDrains } from "./drain"
import { WorktreeMaterializer } from "./worktree"
import { toRunError } from "./run-error-codec"
import * as WF from "./temporal-workflow"

// Classify an interrupt-signal delivery error. "already completed"/"not found" means an idle
// session's workflow has already closed -- nothing to interrupt, a no-op. Anything else is a genuine
// control-plane failure: the user's stop was not delivered, and it must NOT be reported as success.
export function classifyInterruptError(e: unknown): "ignore" | "fail" {
  const message = String((e as { message?: unknown })?.message ?? e)
  return /already completed|not found/i.test(message) ? "ignore" : "fail"
}

const ADDRESS = process.env.TEMPORAL_ADDRESS ?? "127.0.0.1:7237"
const NAMESPACE = process.env.TEMPORAL_NAMESPACE ?? "default"
const TASK_QUEUE = process.env.OPENCODE_TEMPORAL_TASK_QUEUE ?? "opencode-session-exec"
const workflowId = (id: string) => `session-exec-${id}`

// One activity per step (the model call + its tools), with the step loop as workflow control flow.
// Workflows start by the string type, never the function: a minified (packaged) client would
// otherwise register the mangled function name as the type and no worker would match it.
const WORKFLOW_TYPE = "sessionTurn"

// Role split so the worker fleet can run separately from the HTTP server. `both` (default) hosts the
// activity worker AND the workflow client in one process (the serve process). `client` makes serve
// drive workflows without hosting a worker; `worker` runs a standalone activity worker with no HTTP
// surface (see packages/server/src/worker.ts).
const ROLE = process.env.OPENCODE_TEMPORAL_ROLE ?? "both"
const HOST_WORKER = ROLE !== "client"
const HOST_CLIENT = ROLE !== "worker"

/**
 * A Temporal-backed SessionExecution. It makes each session a durable workflow:
 *   - wake      -> signalWithStart(wake)   (start while idle, or coalesce into the running run)
 *   - resume    -> signalWithStart(force)  (force one drain even with no eligible input)
 *   - interrupt -> signal(interrupt)       (cancels the workflow's scope -> aborts the drain)
 *   - active    -> the set of sessions this process has started
 *
 * The drain runs one step (SessionRunner.runStep) inside a Temporal activity, looped by the
 * workflow. Because turn state lives in the durable event log, a worker crash is recovered by
 * re-running the activity: it re-reads recorded history and continues. (Local mode instead drives
 * whole turns with SessionRunner.run on the SessionRunCoordinator; both share SessionRunner.)
 */
const layer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    // The app context the local drain runs in: providing it, then the per-location layer, supplies
    // SessionRunner and all of its dependencies.
    const ctx = yield* Effect.context<SessionStore.Service | LocationServiceMap.Service>()
    const events = yield* EventV2.Service
    const worktrees = yield* WorktreeMaterializer.Service

    // The per-step drain (drain.ts) wraps SessionRunner.runStep for the activity boundary. Local
    // mode runs whole turns through SessionRunner.run on the coordinator; both share SessionRunner.
    const { stepDrain } = makeDrains({ store, locations, ctx, events, worktrees })

    // Worker connection (native) hosts the runTurnStep activity + the workflow. Skipped in
    // client-only role so serve can run without an embedded worker.
    if (HOST_WORKER) {
      const { NativeConnection, Worker } = yield* Effect.tryPromise(
        () => import("@temporalio/worker"),
      ).pipe(
        Effect.catch(() =>
          Effect.die(
            "The embedded Temporal worker is unavailable in this build. Run standalone workers " +
              "(packages/server/src/worker.ts) and set OPENCODE_TEMPORAL_ROLE=client.",
          ),
        ),
      )
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
          activities: makeStepActivities(stepDrain),
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
        client.workflow.signalWithStart(WORKFLOW_TYPE, {
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
            query: `WorkflowType = 'sessionTurn' AND ExecutionStatus = 'Running'`,
          })) {
            if (wf.workflowId.startsWith(SESSION_PREFIX)) {
              ids.push(SessionSchema.ID.make(wf.workflowId.slice(SESSION_PREFIX.length)))
            }
          }
          return ids
        })
        // Point reads, but concurrent: they bypass the store permit, and the running-workflow set
        // is small. A single IN query is the upgrade if this ever grows to hundreds.
        const known = yield* Effect.forEach(
          found,
          (id) => Effect.map(store.get(id), (session) => (session ? id : undefined)),
          { concurrency: 8 },
        )
        return new Set(known.filter((id): id is SessionSchema.ID => id !== undefined))
      }),
      wake: (id) => drive(id).pipe(Effect.asVoid),
      // resume = coordinator.run: drive a forced run via an Update-with-Start and AWAIT its result,
      // so a run error is surfaced to the caller (as a RunError) instead of being swallowed.
      resume: (id) =>
        Effect.tryPromise({
          try: async () => {
            const attempt = () => {
              const startOp = new WithStartWorkflowOperation(WORKFLOW_TYPE, {
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
            // An idle session's workflow has already completed; nothing to interrupt is fine.
            if (classifyInterruptError(e) === "ignore") return Effect.void
            // A genuine delivery failure must not read as success: the stop did not happen. The
            // interface has no error channel, so surface it as a defect rather than a false success.
            const message = String((e as { message?: unknown })?.message ?? e)
            return Effect.logError("session interrupt signal failed").pipe(
              Effect.annotateLogs({ sessionID: id, error: message }),
              Effect.andThen(Effect.die(`session interrupt delivery failed for ${id}: ${message}`)),
            )
          }),
        ),
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node, EventV2.node, WorktreeMaterializer.node],
})
