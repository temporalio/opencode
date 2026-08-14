# AI-399: Local options for Temporal-integrated agents

Some customers integrate Temporal into their agents and want the same agent loop to run without
Temporal, e.g. shipped desktop software. This evaluates the candidate paths and recommends what to
tell customers and what to build. It lives on this branch because the fork around it is first-hand
evidence: one engine, two execution modes (in-process, one Temporal activity per step), selected
by an env var.

## The requirement, sharpened

"Run without Temporal" hides three different customer asks, and the right answer differs:

1. **Same loop, no infrastructure at all** (desktop/CLI): no server process, no ports, minimal
   footprint. Durability degrades gracefully (a checkpoint file beats nothing).
2. **Same loop, no OPERATED infrastructure** (an appliance: one machine the customer operates,
   e.g. an on-prem server box or an edge device): a local process is fine if it is zero-admin.
   Full Temporal semantics wanted.
3. **Same CODE, both worlds**: the vendor ships one codebase; cloud deployments get durability,
   desktop gets local. The dominant engineering constraint is preventing drift between the paths.

The motivating desktop customer (Cursor) rejects a bundled server process, so archetype 1 must be
answered in-process.

## Paths

### A. Shim Temporal at the language layer

Replace the Temporal SDK surface the agent code touches (`workflow.*`, `proxyActivities`,
signals/updates/timers) with a local implementation, so unmodified workflow code runs in-process.

- Exists today: customers have added shims like this in their own code (per the ticket), paying
  the cost themselves.
- Durability: whatever the shim persists. A faithful shim needs event-sourced replay to recover
  mid-workflow, which is the hard part of Temporal, reimplemented.
- Signals/Updates: re-implemented on the shim's event loop; semantic-drift risk is high
  (buffering, ordering, update validators, cancellation scopes).
- Complexity: high and permanent for a general shim; the shim chases the SDK surface every
  release, and determinism constraints stay imposed on local code that gets nothing for them. A
  shim scoped to the app's actual control surface (enumerated below) is much smaller, at the cost
  of being app-specific.
- Where it shines: an existing Temporal-first codebase that cannot be refactored, needing a local
  mode quickly.

### B. Shim in the Rust core

Implement a local mode under the core SDK so all core-based SDKs (TypeScript, Python, .NET, Ruby)
get it at once.

Research findings (sdk-core is now `temporalio/sdk-rust`; MIT):

- **The insertion point is clean and already public.** `ConnectionOptions.service_override`
  (`crates/client/src/options_structs.rs`) routes every gRPC call through a supplied callback
  instead of the network, and the C ABI already plumbs it (`grpc_override_callback` in
  `sdk-core-c-bridge`), so .NET/Ruby could intercept in-process today. Python does not expose it;
  TypeScript unverified. Why the hook exists is unverified (possibly proxying/testing).
- **Determinism, state machines, and replay live in core, not the server**, so a shim does NOT
  reimplement the hard part. What it must implement is the SERVICE: task matching with long-poll
  semantics, history append/read, workflow task lifecycle, server-side timers, activity retry and
  four timeout types, signals/queries/updates (update's multi-stage lifecycle), ID reuse/conflict
  policies, continue-as-new, child workflows, cancellation.
- **The honest size estimate is the Java time-skipping test server**: a from-scratch in-memory
  service reimplementation, GraalVM-compiled, consumed by Python/.NET/TS/Ruby, and still short of
  parity after years (`sdk-java#1804`, a Temporal employee asking for the real dev server in
  tests because `listWorkflowExecutions` is missing).
- Core's replay worker (`init_replay_worker`) proves the plumbing tolerates a non-network client,
  but it consumes pre-recorded history only; it is not a local runtime.

Read: the seam is cheap, the payload is a second implementation of the Temporal service, and
Temporal's existing second implementation has not reached parity. Only worth doing as an owned
product commitment ("embedded Temporal"), not as a workaround.

### C. Plugin pattern: one loop, swappable execution

Factor the agent so the LOOP is pure and the execution substrate is injected; Temporal is one
substrate, a local runner is another.

**Evidence 1: this fork.** opencode's v2 engine event-sources every session to a store and exposes
a substitutable `SessionExecution` interface (`active`/`wake`/`resume`/`interrupt`, four methods).
The stock implementation is an in-process coordinator; this fork adds a Temporal-backed one
(`OPENCODE_SESSION_EXECUTION=temporal`, one activity per step) plus a shared store so
any worker resumes any session. Because durability lives in the engine's event log, the LOCAL mode
is already crash-recoverable without Temporal; Temporal adds supervised retries, worker
distribution, restart-surviving visibility, and cross-process interrupt/resume. The swap point is
the execution supervisor, not the state store. Verified end to end in this branch (crash tests,
failover tests, an independent architecture review).

**Evidence 2: `agent-harness` (AI-363, TypeScript).** A pure loop state machine plus an `Effects`
interface (`callModel`/`runTools`/`onEvent`) implemented twice: local mode (direct calls, atomic
write-then-rename checkpoint file, crash-resume) and durable mode (workflow + activities with
measured defaults). About 100 lines per runner around a shared core. This is the TypeScript
prototype the ticket's "has been implemented" refers to. Local mode gives up retries across
process death mid-tool, multi-worker capacity, and the audit trail; it keeps the same loop, tools,
prompts, and checkpoint crash-resume.

**Evidence 3: Temporal already ships a dual-mode ADK integration.** The ticket points at ADK as
the example; the official integration (`pip install "temporalio[google-adk]"`,
`temporalio.contrib.google_adk_agents`, experimental) is shipped and dual-mode. Its mechanism is
an ambient check, not a factory:

- ADK's own seam is the `Runner` + `BaseSessionService` (in-memory, sqlite, database, vertex
  implementations), so local ADK needs no infrastructure.
- The Temporal integration does not replace the Runner. It wraps the model seam (`TemporalModel`
  runs the call as an `invoke_model` activity) and the tool seam (`activity_tool` dispatches via
  `workflow.execute_activity`), and each wrapper degrades to a direct in-process call when
  `temporalio.workflow.in_workflow()` is false. Determinism helpers branch on the same predicate
  (clock and ID providers, via hooks Temporal landed upstream in ADK,
  `google.adk.platform.{time,uuid}`).
- One agent definition, two execution modes, no second code path for the user. Sharp edges worth
  telling customers: MCP toolsets cannot auto-fall-back (the caller must supply
  `not_in_workflow_toolset`), and durable mode is STRICTER than local (whole session state must
  serialize within payload limits), so local-only testing can pass and then fail under Temporal.

The ambient check is the compatibility variant of this pattern, not the recommended shape. It
exists because Temporal does not own ADK's composition root, and because a workflow cannot receive
a live object graph as input, so the check detects which behavior is legal in the current
environment. Its costs are structural: the mode is invisible at call sites, the conditional
repeats in every wrapper, the type system cannot enforce a complete local graph, and where uniform
degradation is impossible the failure is a runtime raise (the MCP edge above) instead of a
construction-time requirement. When you own the composition root, prefer a factory that returns
the interface: both implementations above do exactly that (one binding selects the
`SessionExecution` implementation; one `Effects` value selects the runner), and a missing local
implementation then fails at construction, not mid-run. The two shapes compose: a substrate
factory at the agent-definition level with the ambient check kept only as a safety net would
also close the MCP hole in ADK-shaped integrations.

Characteristics of the family:

- Durability: local = what the local runner persists (checkpoint file is coarse; an event-sourced
  store is fine-grained and close to Temporal-grade for single-machine crashes). Temporal = full.
- Signals/Updates: the app defines the interface both modes honor. No pretense of Temporal's
  generic protocol locally, and no drift, because the loop is the same code.
- Complexity: lowest sustained cost of all paths; the seam is app-defined and small, or (ADK
  style) hidden inside integration wrappers. The cost is up-front design; not a bolt-on for an
  existing Temporal-first codebase, except where Temporal ships the integration.

### D. Run the Temporal dev server locally

Ship `temporal server start-dev` alongside the app; the agent stays a plain Temporal application.

Facts (research verified on release artifacts, v1.8.2, 2026-07; local measurements on this
machine's dev build):

| Metric | Value |
|---|---|
| Release binary (darwin arm64) | 127.5 MiB uncompressed, 37.5 MiB compressed (this machine's 237 MB was a dev build; use the release number) |
| Platforms | macOS/Linux/Windows, amd64+arm64, one static Go binary incl. server, CLI, Web UI |
| Cold start to healthy | ~780 ms (`--headless`) |
| RSS idle | ~102-139 MB (this machine 102 MB; research 139 MB on v1.31.2) |
| SQLite file | ~0.6 MB empty |
| Persistence | `--db-filename` required; the DEFAULT is in-memory and loses everything on exit |
| Embedding-friendly flags | `--headless`, `--db-filename`, `--port`, `--ip`, repeatable `--namespace`, `--sqlite-pragma`, `--dynamic-config-value`; http/metrics ports default to random free |
| License | MIT (verified in release tarball and via GitHub API) |

On this app the desktop number is 297 MB RSS, one process, local mode. A bundled server was
measured once to close the desktop question and is not a configuration anyone ships: desktop is
local mode, and temporal mode's server is Cloud or a fleet. The standalone figures above are the
appliance-archetype numbers.

- **Fidelity is the differentiator: it is the real server against SQLite, not an emulator.**
  Research verified multi-namespace and Nexus endpoints work and persist. Signals, updates,
  queries, schedules, search attributes are the real implementations. No drift, ever.
- **Positioning is the weakness.** The binary itself prints a not-for-production warning (added
  deliberately, `cli#689`); the embedded-server docs page says testing and development only;
  limits are real (SQLite single writer, `NumHistoryShards: 1`, all roles in one process).
  Restate and Inngest bless their single-node binaries for production; Temporal is the only one
  in the comparison set whose local mode is officially disowned.
- Redistribution: no licensing issue (MIT). A real option for the appliance archetype. The
  motivating desktop customer does not want a bundled server process, so this path is parked for
  desktop.

### E. Integrate a local option into the Temporal Agent Harness

Not an independent runtime: it is where a choice among A/C/D becomes product. The harness today is
Temporal-native (agents ARE workflows; approvals, Code Mode, callback tools, and the event stream
ride Temporal primitives). A local mode via C would make the harness's public abstractions (agent
definition, tools, approval policy, event stream) the swap seam with a non-Temporal transport
locally (in-process bus, identical schemas). The ADK integration's `in_workflow()` mechanism is
the shipped precedent for how the harness's Temporal-aware pieces could degrade in-process.

## One supervisor, two drivers (demonstrated on this branch)

The two-implementations objection to the plugin pattern is real: the loop is written once, but the
SUPERVISOR contract (drains serialize, wakes coalesce, interrupt cancels, resume surfaces the
error) existed twice, once as the local coordinator and once as the workflow, and the independent
review found bugs precisely in the duplicated copy. This branch now closes that gap:

- `workflow-core.ts`: the supervisor, written ONCE, over a six-primitive `WorkflowRuntime`
  interface (`condition`, signal handlers, update handlers, the drain calls, cancellation,
  `isCancellation`). Temporal's sandbox had already forced it to be pure, which is what makes it
  executable anywhere.
- `temporal-workflow.ts`: the Temporal driver; the real SDK provides the six primitives, the
  drains run as activities.
- `local-driver.ts` (the default mode; `OPENCODE_SESSION_EXECUTION=temporal` selects the Temporal
  driver instead): the in-process driver; plain promises provide the primitives (a polled
  `condition`, method-call signals, an `AbortController` for cancellation), the drains run
  directly. No server, no worker, no ports.
- `drain.ts` and the error codec are shared modules, so turn semantics and typed errors are
  byte-identical in both modes; contract tests drive the shared supervisor in-process (a turn
  settles, the exact tagged `RunError` crosses the same encode/decode path, interrupt cancels, an
  idle supervisor retires), and the worker smoke proves the same file still bundles in the
  Temporal sandbox.

The factory still hides the choice, and it is now exactly two modes: the in-process driver by
default, `temporal` for the server-backed one. The earlier whole-turn and stock-coordinator modes
were folded away once the shared supervisor made them redundant. This is the factory-shaped answer
to the ADK integration's ambient check, demonstrated.

## Can the whole SDK surface be covered this way?

The micro-driver needed six primitives because the supervisor uses six. Extending it across
`@temporalio/workflow`'s API splits cleanly:

- **Mechanical for live execution:** `sleep`/timers (`setTimeout`), `workflow.now` (`Date.now`),
  `random`/`uuid4` (plain random: determinism only matters for replay, which a local driver never
  does), queries (read a handler map), `patched`/`deprecatePatch` (constant true / no-op),
  `sideEffect` (run the function), search attributes and memo (a local map), logging sinks
  (console), child workflows and external handles (spawn sibling drivers, route signals through an
  in-process registry), continue-as-new (re-invoke the function with the new arguments).
- **The fundamental line is replay.** Temporal recovers workflow-VARIABLE state after a crash by
  replaying history; a local driver has no history, so in-flight workflow variables and pending
  timers die with the process. Covering that is not a shim, it is the embedded-service payload of
  path B.

Two consequences. First, the design rule for dual-mode apps: keep durable truth in an app-owned
log and treat workflow variables as ephemeral. This branch already obeys it (supervisor state is
reconstructible; turn state is in the event store; re-drives are log-based), which is why the
local driver loses nothing that matters on a desktop crash, and it is the rule to hand any
customer taking this path. Second, the honest scope statement: a whole-SDK local runtime is
achievable for LIVE semantics as a bounded engineering effort, but replay-grade durability of
workflow-local state is exactly where "cover the SDK surface" becomes "build embedded Temporal",
and should be decided as that (path B), not approached incrementally by accident.

## What the local mode actually has to support: the control surface

Measured from opencode's protocol (the session group plus human-in-the-loop groups), the session
control surface a desktop agent product exposes is enumerable, roughly a dozen verbs:

- Run control: `prompt` (with two delivery semantics: steer into the running turn, or queue after
  it), `interrupt`, `wait` (await settlement).
- Human-in-the-loop: permission reply (`once`/`always`/reject-with-correction), agent questions
  (typed ask/answer).
- Session mutation: `switchAgent`, `switchModel`, `compact`, `revert` (stage/clear/commit).
- Observation: history, context, live event stream, active set.

This is far richer than a cancel button, and far smaller than Temporal's generic signal/update
protocol. Every verb maps to a signal/update/query in Temporal mode and an in-process call
locally; the event-sourced store is what lets both modes serve the observation verbs identically.
This bounds path A (a scoped shim is a dozen verbs, not the SDK) and explains why path C stays
cheap: the interface already exists in any real product.

## Prior art: how others answer local-without-the-big-server

| Pattern | Example | Mechanism | Gives up |
|---|---|---|---|
| Pluggable store, same process | LangGraph (`checkpointer=`: memory/sqlite/postgres); DBOS (Python defaults to SQLite) | one interface, N stores | LangGraph memory: restart durability; DBOS local: multi-process recovery |
| Same binary, different config | Restate (single binary, RocksDB); Inngest (`inngest dev`, in-memory by default; `inngest start` for prod) | run the real thing small | fault tolerance of a cluster; a process always runs |
| Emulator | Azure Durable Task Scheduler emulator | separate implementation | fidelity; explicitly not production |
| In-process protocol reimplementation | Resonate `LocalNetwork` (server state machine over dicts); Temporal's Java test server | exact semantics, zero deps | maintaining two implementations forever |

Temporal's dev server is the second pattern with one difference: Restate and Inngest bless their
single-node story for production; Temporal explicitly does not.

## Demand evidence

The ask is real, old, and largely unanswered publicly:

- `temporalio/temporal#298` "run Temporal as an embedded library", opened 2020 by Maxim Fateev
  ("For small scale on prem deployments... would be really great"), closed 2026 pointing at
  `start-dev` with not-for-production caveats; requesters explicitly disputed the closure (the
  actual ask was an in-process, NATS-style embedded server).
- Three desktop/OEM forum threads (2023-2025: Wails desktop app, standalone Go binary packaging,
  bundled-Temporal upgrade sequencing) with zero staff replies among them.
- The only substantive edge guidance is a 2025 forum answer (IoT/submarine): run the single
  binary on the device, federate with Nexus; which sits against docs saying SQLite single-process
  is testing/development only.
- Two abandoned commitments: Maxim in 2020 ("Desktop applications... we absolutely going to
  create it. No ETA"); Temporalite's maintainer in 2022 ("the vision is for Temporalite to be
  used in production contexts"). Temporalite is archived; `temporal#3366` (SQLite in production)
  is still open.

## Comparison

| | A: language shim | B: rust-core shim | C: plugin pattern (incl. ADK-style fallback) | D: local dev server |
|---|---|---|---|---|
| What it is | reimplement the SDK API in the app's language so Temporal-shaped code runs with no server; the customer owns it | embed a real Temporal service in the SDK core behind `service_override`; one in-process backend under every language | the loop behind a small execution interface; a startup factory picks Temporal or in-process, same code both modes | bundle the real dev server binary next to the app, which starts and supervises it |
| Durability (crash mid-turn) | partial: what the shim persists; faithful replay = reimplementing Temporal | full IF built (it IS an embedded service) | partial: checkpoint-file coarse; event-sourced local store near-full for one machine | full (with `--db-filename`; default is in-memory) |
| Resource needs | lightest (in-process) | in-process; core carries matching+history | lightest (in-process; store is a file) | ~102-139 MB RSS second process, ~128 MiB disk (appliance archetype; not a desktop configuration) |
| Signal/Update support | re-implemented, drift-prone | full IF built | the app's control surface (~a dozen verbs), honored identically by both modes | full |
| Complexity / maintainability | high, permanent (chases SDK surface per language); bounded if scoped to the control surface | highest; a second Temporal service implementation (Java test server: years, still short of parity) | lowest sustained; up-front loop design, or shipped by Temporal (ADK) | near-zero code; packaging+lifecycle burden |
| Feature fidelity | subset, hand-built | full IF built | app semantics, not Temporal's | full: the real server (multi-namespace, Nexus verified) |
| Code-drift between modes | medium (same code, different semantics) | low | none for the loop; ADK caveat: durable mode is stricter (serialization), so local-pass/durable-fail exists | none |
| Desktop packaging | best | good | best | worst (bundle+supervise a server); MIT, no licensing issue |
| Upgrade path to Temporal Cloud | same code, repoint | same code, repoint | swap the factory / connect the client | same code, repoint |
| Exists today | yes, by customers in their own code | no (seam exists: `service_override`; payload does not) | yes, three times: this fork, agent-harness, and the shipped ADK integration | yes (shipped CLI) |

## Recommendations

1. **For customers on a framework Temporal integrates with (ADK today): point at the shipped
   integration.** The `in_workflow()` fallback is the productized version of the plugin pattern:
   one agent definition, two modes, maintained by Temporal. Write up the two sharp edges (MCP
   toolsets need an explicit local implementation; durable mode is stricter than local, so test
   both modes).
2. **For customers designing their own loop: recommend the plugin pattern (C).** Two working
   implementations here show the cost is one small interface. Enumerate the control surface (it
   is about a dozen verbs in a real product) and event-source the session when local durability
   matters, rather than reimplementing Temporal's replay.
3. **For existing Temporal-first agents on machines the customer controls (appliance,
   single-machine): D**, with `--db-filename` and process supervision. Full fidelity, no
   licensing issue. Off the table for the desktop archetype by customer preference.
4. **A is the fallback** when neither refactoring (C) nor a second process (D) is acceptable:
   scope the shim to the control surface, not the SDK, and accept the drift risk.
5. **B is a product decision, not a customer recommendation.** The seam exists
   (`service_override`, already in the C bridge); the payload is a second Temporal service
   implementation, and our own Java test server shows the parity cost. The demand evidence
   (six years of #298, unanswered desktop/OEM threads, two abandoned commitments) says there is a
   real, unclaimed "embedded Temporal" position; claiming it means owning that implementation
   indefinitely. That decision belongs to product, informed by this doc.
6. **Harness (E): apply C at the harness API layer**, as a factory returning the harness's
   execution interface, chosen once at the composition root. Not ambient `in_workflow()` checks:
   we own this composition root, so the local implementation should be enforced at construction
   time. The ADK integration is the precedent for the wrapper technique where we do not own the
   root, not for the harness's own design.

## Sources

Primary: this branch (implementation + measurements); `agent-harness` (AI-363);
`temporalio/sdk-rust` source; `temporalio/sdk-python` `contrib/google_adk_agents` source; a
downloaded v1.8.2 CLI release binary, run headless.

Referenced: adk.dev/integrations/temporal · temporal.io/blog/google-adk-temporal-integration-bts ·
temporalio/samples-python `google_adk_agents` · google/adk-python `sessions/` ·
docs.temporal.io/self-hosted-guide/embedded-server · docs.temporal.io/cli/server ·
temporalio/cli#689 · temporalio/temporal#298 · temporalio/temporal#3366 ·
temporalio/sdk-java#1804 · temporalio/temporalite-archived · community.temporal.io threads
17424, 12914, 10125, 18362 · LangGraph persistence docs · Restate architecture docs · DBOS
database docs · Inngest dev-server docs · Azure Durable Functions storage-providers docs ·
resonatehq/resonate-sdk-py `network/local.py`.
