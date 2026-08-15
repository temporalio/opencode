import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { httpClient } from "@opencode-ai/core/effect/app-node-platform"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventV2 } from "@opencode-ai/core/event"
import { Credential } from "@opencode-ai/core/credential"
import { PermissionSaved } from "@opencode-ai/core/permission/saved"
import { PtyTicket } from "@opencode-ai/core/pty/ticket"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { LocationServiceMap } from "@opencode-ai/core/location-service-map"
import { SessionExecutionLocal } from "@opencode-ai/core/session/execution/local"
import { SessionExecutionTemporal } from "@opencode-ai/core/session/execution/temporal"
import { ToolOutputStore } from "@opencode-ai/core/tool-output-store"
import { HttpRouter, HttpServer } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Layer, Option } from "effect"
import { Api } from "./api"
import { ServerAuth } from "./auth"
import { handlers } from "./handlers"
import { authorizationLayer } from "./middleware/authorization"
import { schemaErrorLayer } from "./middleware/schema-error"
import { PtyEnvironment } from "./pty-environment"
import { layer as locationLayer } from "./location"
import { sessionLocationLayer } from "./middleware/session-location"

const applicationServices = LayerNode.group([
  Database.node,
  EventV2.node,
  httpClient,
  ToolOutputStore.cleanupNode,
  SessionV2.node,
  PermissionSaved.node,
  PtyTicket.node,
  Credential.node,
  PtyEnvironment.node,
  LocationServiceMap.node,
])

export function createRoutes(password?: string) {
  return makeRoutes(
    password
      ? ServerAuth.Config.configLayer({ username: "opencode", password: Option.some(password) })
      : ServerAuth.Config.layer,
  )
}

export function createEmbeddedRoutes() {
  return makeRoutes(ServerAuth.Config.configLayer({ username: "opencode", password: Option.none() }))
}

// The application-service context (no HTTP surface), with the execution engine selected by env.
// Shared by the HTTP routes and the standalone worker entrypoint (src/worker.ts) so both build the
// exact same context.
export function createServiceLayer() {
  // The factory: two modes. "temporal" runs each session as a per-step Temporal workflow
  // (execution/temporal.ts); anything else runs it in-process on the proven SessionRunCoordinator
  // (execution/local.ts) -- no server, no ports. Both drive SessionRunner over the same durable
  // event log; the local coordinator owns the wake/resume/interrupt lifecycle and is shared with
  // the v1 server path, so it is the well-exercised default.
  const executionNode =
    process.env.OPENCODE_SESSION_EXECUTION === "temporal"
      ? SessionExecutionTemporal.node
      : SessionExecutionLocal.node
  return AppNodeBuilder.build(applicationServices, [[SessionExecution.node, executionNode]])
}

// The context for a standalone worker (src/worker.ts). Same services as serve, but with
// SessionExecution as a built member so constructing the layer eagerly starts the Temporal worker
// (with OPENCODE_TEMPORAL_ROLE=worker there is no HTTP handler to pull it in lazily).
export function createWorkerLayer() {
  // A standalone worker only makes sense in temporal mode.
  return AppNodeBuilder.build(LayerNode.group([applicationServices, SessionExecution.node]), [
    [SessionExecution.node, SessionExecutionTemporal.node],
  ])
}

function makeRoutes<AuthError, AuthServices>(auth: Layer.Layer<ServerAuth.Config, AuthError, AuthServices>) {
  const serviceLayer = createServiceLayer()

  return HttpApiBuilder.layer(Api, { openapiPath: "/openapi.json" }).pipe(
    Layer.provide(handlers),
    Layer.provide(sessionLocationLayer),
    Layer.provide(locationLayer),
    Layer.provide(authorizationLayer),
    Layer.provide(schemaErrorLayer),
    Layer.provide(auth),
    Layer.provide(serviceLayer),
  )
}

export const routes = createRoutes()

export const webHandler = () =>
  HttpRouter.toWebHandler(routes.pipe(Layer.provide(HttpServer.layerServices)), { disableLogger: true })
