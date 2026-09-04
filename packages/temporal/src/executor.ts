export * as SessionExecutionTemporal from "./executor"

import { fileURLToPath } from "node:url"
import { hostname } from "node:os"
import { Effect, Layer, Option } from "effect"
import { Client, Connection, WithStartWorkflowOperation } from "@temporalio/client"
// Imported lazily inside the worker branch: the worker package drags webpack and swc (it bundles
// the workflow from source at startup), which a compiled binary can neither bundle nor run. A
// packaged serve runs OPENCODE_TEMPORAL_ROLE=client next to standalone workers instead.

import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { EventV2 } from "@opencode-ai/core/event"
import { makeGlobalNode } from "@opencode-ai/core/effect/app-node"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { makeStepActivities, makeSteppedTurnActivities } from "./activities"
import { makeDrains } from "./drain"
import { makeL2Drains, makeScheduleDrains } from "./l2-drain"
import { queueForWorktree, queueForWorker } from "./queue"
import { Database } from "@opencode-ai/core/database/database"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { eq } from "drizzle-orm"
import { WorktreeMaterializer } from "@opencode-ai/core/session/execution/worktree"
import { toRunError } from "@opencode-ai/core/session/execution/run-error-codec"
import * as WF from "./workflow"
import { TemporalConfig } from "./config"
import { WORKFLOW_TYPE, WORKFLOW_ID_PREFIX, workflowId } from "./protocol"

// Classify an interrupt-signal delivery error. "already completed"/"not found" means an idle
// session's workflow has already closed -- nothing to interrupt, a no-op. Anything else is a
// genuine
// control-plane failure: the user's stop was not delivered, and it must NOT be reported as success.
export function classifyInterruptError(e: unknown): "ignore" | "fail" {
  const message = String((e as { message?: unknown })?.message ?? e)
  return /already completed|not found/i.test(message) ? "ignore" : "fail"
}

/**
 * A Temporal-backed SessionExecution. It makes each session a durable workflow:
 *   - wake      -> signalWithStart(wake)             (start while idle, or coalesce into the run)
 *   - resume    -> executeUpdateWithStart(resume)    (force one drain and await its result)
 *   - interrupt -> signal(interrupt)                 (cancels the current turn's drain scope; the
 *                                                      workflow keeps serving later wakes/resumes)
 *   - active    -> the running per-session workflows (visibility-backed)
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
    const db = (yield* Database.Service).db
    // The app context the local drain runs in: providing it, then the per-location layer, supplies
    // SessionRunner and all of its dependencies.
    const ctx = yield* Effect.context<SessionStore.Service | LocationServiceMap.Service>()
    // Provided by an embedder or a test, env otherwise; nothing is read at module load.
    const config = Option.getOrElse(yield* Effect.serviceOption(TemporalConfig.Service), TemporalConfig.fromEnv)
    const { address: ADDRESS, namespace: NAMESPACE, taskQueue: TASK_QUEUE } = config
    const HOST_WORKER = config.role !== "client"
    const HOST_CLIENT = config.role !== "worker"
    // Same knob local mode honors; the workflow sandbox cannot read env, so the client forwards the
    // override as a workflow argument.
    const IDLE_TIMEOUT = config.idleTimeout
    const STEPPED = config.stepped === true
    // Only the client can read whether the store is shared, so whether a step's tools may overlap
    // is decided here and rides the workflow input.
    const SERIAL_TOOLS = config.serialTools === true
    const AFFINITY = config.worktreeAffinity === true
    // The tree this process serves when affinity is on. A serve process with an embedded worker is
    // already sitting in it, so the process directory is the right default.
    const SERVED_WORKTREE = config.worktree ?? process.cwd()
    // Which queue a worker polls. With affinity off this is the one shared queue and any worker can
    // draw any session, rebuilding the tree if it has to.
    const POLL_QUEUE = AFFINITY ? queueForWorktree(TASK_QUEUE, SERVED_WORKTREE) : TASK_QUEUE
    // The queue this worker polls on its own, so a step can be sent back to it. Keyed by host as
    // well as directory: two containers serve `/project` and share none of it. Only workers have
    // one, and only they report it, so a client-only process never pins a step to itself.
    const STEP_QUEUE =
      HOST_WORKER && config.stepAffinity !== false
        ? queueForWorker(TASK_QUEUE, hostname(), SERVED_WORKTREE)
        : undefined
    // Which queue a session's workflow runs on. Keyed on the PROJECT worktree, not the session's
    // directory: `worktrees.ensure` rebuilds the project tree, so keying on the directory a session
    // happened to start in would split one physical tree across a queue per subdirectory, and a
    // session started from a subfolder would wait on a queue nobody polls.
    const queueFor = (id: SessionSchema.ID) =>
      AFFINITY
        ? Effect.gen(function* () {
            const session = yield* store.get(id)
            if (!session) return TASK_QUEUE
            const project = yield* db
              .select({ worktree: ProjectTable.worktree })
              .from(ProjectTable)
              .where(eq(ProjectTable.id, session.projectID))
              .get()
              .pipe(Effect.orDie)
            return project ? queueForWorktree(TASK_QUEUE, project.worktree) : TASK_QUEUE
          })
        : Effect.succeed(TASK_QUEUE)
    // Before anything is accepted, not after: every one of these fails as something else later, and
    // the failure lands on whoever prompted the session rather than on whoever deployed it.
    const problems = TemporalConfig.preflight(config)
    for (const problem of problems) yield* Effect.logError(`configuration: ${problem}`)
    if (problems.length > 0) yield* Effect.die(`this deployment cannot serve sessions: ${problems[0]}`)

    const events = yield* EventV2.Service
    const worktrees = yield* WorktreeMaterializer.Service

    // The per-step drain (drain.ts) wraps SessionRunner.runStep for the activity boundary. Local
    // mode runs whole turns through SessionRunner.run on the coordinator; both share SessionRunner.
    const { stepDrain } = makeDrains({ store, locations, ctx, events, worktrees })
    // The stepped mode's three drains. Registered unconditionally: which mode a session runs is a
    // property of its workflow input, so a worker has to be able to serve either.
    const l2 = makeL2Drains({ store, locations, ctx, events, worktrees, stepQueue: STEP_QUEUE })
    // What a schedule fires into: admitting a prompt is a row in the store, and a workflow cannot
    // write one. Registered on every worker, because a firing lands wherever one is polling.
    const schedules = makeScheduleDrains({ db, events, ctx })

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
        Effect.promise(() => NativeConnection.connect(TemporalConfig.connectionOptions(config))),
        (conn) => Effect.promise(() => conn.close().catch(() => {})),
      )
      const worker = yield* Effect.promise(() =>
        Worker.create({
          connection: nativeConn,
          namespace: NAMESPACE,
          taskQueue: POLL_QUEUE,
          workflowsPath: fileURLToPath(new URL("./workflow.ts", import.meta.url)),
          activities: {
            ...makeStepActivities(stepDrain),
            ...makeSteppedTurnActivities(l2),
            promptSession: schedules.promptDrain,
          },
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

      // A second poller, on this worker's own queue, for the steps pinned to it. Activities only:
      // the workflow runs wherever it was started, and only the work that has to come back here is
      // addressed here. Without it a pin has nobody to answer it and every step pays the
      // schedule-to-start wait before falling back.
      if (STEP_QUEUE) {
        const pinnedWorker = yield* Effect.promise(() =>
          Worker.create({
            connection: nativeConn,
            namespace: NAMESPACE,
            taskQueue: STEP_QUEUE,
            activities: {
              ...makeStepActivities(stepDrain),
              ...makeSteppedTurnActivities(l2),
              promptSession: schedules.promptDrain,
            },
          }),
        )
        const pinnedHandle = pinnedWorker.run()
        pinnedHandle.catch(() => {})
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            pinnedWorker.shutdown()
            await pinnedHandle.catch(() => {})
          }),
        )
      }
    }

    // Worker-only process: it hosts activities but drives no workflows, so the client methods are
    // unused. Return a service whose driving methods fail loudly if something unexpectedly calls
    // them.
    if (!HOST_CLIENT) {
      yield* Effect.logInfo("SessionExecutionTemporal worker ready").pipe(
        Effect.annotateLogs({
          address: ADDRESS,
          taskQueue: POLL_QUEUE,
          workflow: WORKFLOW_TYPE,
          role: config.role,
          ...(AFFINITY ? { worktree: SERVED_WORKTREE } : {}),
        }),
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
      Effect.promise(() => Connection.connect(TemporalConfig.connectionOptions(config))),
      (conn) => Effect.promise(() => conn.close().catch(() => {})),
    )
    const client = new Client({ connection: clientConn, namespace: NAMESPACE })

    const drive = (id: SessionSchema.ID) =>
      Effect.flatMap(queueFor(id), (taskQueue) =>
        Effect.promise(() =>
          client.workflow.signalWithStart(WORKFLOW_TYPE, {
            taskQueue,
            workflowId: workflowId(id),
            args: [
              id,
              {
                startWithWake: true,
                idleTimeout: IDLE_TIMEOUT,
                stepped: STEPPED,
                serialTools: SERIAL_TOOLS,
              } satisfies WF.SessionTurnOptions,
            ],
            signal: WF.wake,
            signalArgs: [],
          }),
        ),
      )

    yield* Effect.logInfo("SessionExecutionTemporal ready").pipe(
      Effect.annotateLogs({
        address: ADDRESS,
        taskQueue: TASK_QUEUE,
        workflow: WORKFLOW_TYPE,
        ...(AFFINITY
          ? { worktreeAffinity: true, pollQueue: POLL_QUEUE, worktree: SERVED_WORKTREE }
          : {}),
      }),
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
            if (wf.workflowId.startsWith(WORKFLOW_ID_PREFIX)) {
              ids.push(SessionSchema.ID.make(wf.workflowId.slice(WORKFLOW_ID_PREFIX.length)))
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
        Effect.flatMap(queueFor(id), (resumeQueue) =>
          Effect.tryPromise({
            try: async () => {
              const attempt = () => {
                const startOp = new WithStartWorkflowOperation(WORKFLOW_TYPE, {
                  taskQueue: resumeQueue,
                  workflowId: workflowId(id),
                  // startWithWake=false: a fresh resume-with-start must not manufacture a wake
                  // drain;
                  // its forced drain comes from the resume update. Ignored when USE_EXISTING joins
                  // a
                  // running workflow (which keeps its own state).
                  args: [
                    id,
                    {
                      startWithWake: false,
                      idleTimeout: IDLE_TIMEOUT,
                      stepped: STEPPED,
                      serialTools: SERIAL_TOOLS,
                    } satisfies WF.SessionTurnOptions,
                  ],
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
        ),
      interrupt: (id) =>
        Effect.tryPromise({
          try: () => client.workflow.getHandle(workflowId(id)).signal(WF.interrupt),
          catch: (e) => e,
        }).pipe(
          Effect.catch((e) => {
            // An idle session's workflow has already completed; nothing to interrupt is fine.
            if (classifyInterruptError(e) === "ignore") return Effect.void
            // A genuine delivery failure must not read as success: the stop did not happen. The
            // interface has no error channel, so surface it as a defect rather than a false
            // success.
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
  deps: [
    SessionStore.node,
    LocationServiceMap.node,
    EventV2.node,
    WorktreeMaterializer.node,
    Database.node,
  ],
})
