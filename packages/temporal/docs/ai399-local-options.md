# AI-399: Local options for Temporal-integrated agents

Status: DRAFT. Sections marked [research pending] are being filled; measurements are final.

Some customers integrate Temporal into their agents and want the same agent loop to run without
Temporal, e.g. shipped desktop software. This evaluates the candidate paths and recommends what to
tell customers and what to build. It lives on this branch because the fork around it is first-hand
evidence: one engine, three execution modes (in-process, one activity per turn, one per step),
selected by an env var.

## The requirement, sharpened

"Run without Temporal" hides three different customer asks, and the right answer differs:

1. **Same loop, no infrastructure at all** (desktop/CLI): no server process, no ports, minimal
   footprint. Durability degrades gracefully (a checkpoint file beats nothing).
2. **Same loop, no OPERATED infrastructure** (single-machine server, appliance): a local process
   is fine if it is zero-admin. Full Temporal semantics wanted.
3. **Same CODE, both worlds**: the vendor ships one codebase; cloud deployments get durability,
   desktop gets local. The dominant engineering constraint is preventing drift between the paths.

## Paths

### A. Shim Temporal at the language layer

Replace the Temporal SDK surface the agent code touches (`workflow.*`, `proxyActivities`,
signals/updates/timers) with a local implementation, so unmodified workflow code runs in-process.

- Durability: whatever the shim persists. A faithful shim needs event-sourced replay to recover
  mid-workflow, which is the hard part of Temporal, reimplemented.
- Signals/Updates: re-implemented on the shim's event loop; semantic-drift risk is high
  (buffering, ordering, update validators, cancellation scopes).
- Complexity: high and permanent; the shim chases the SDK surface every release, and determinism
  constraints stay imposed on local code that gets nothing for them.
- Where it shines: an existing Temporal-first codebase that cannot be refactored, needing a local
  mode quickly, using a small enumerable SDK subset.
- The ticket's "has been implemented" covers two things (per the assignee): our own TypeScript
  prototype, and shims customers have added inside their own codebases. Strictly, the TS prototype
  (`agent-harness`, AI-363) swaps at an app-defined seam, which this doc classifies as path C;
  the customer-authored variants are true path A: they fake the SDK surface their code touches and
  carry the drift risk described above. The distinction matters because the two have opposite
  maintenance profiles.

### B. Shim in the Rust core

Implement a local mode under sdk-core so all core-based SDKs (TypeScript, Python, .NET, Ruby) get
it at once: the core's server-facing surface backed by an embedded, in-process implementation.

- One implementation serves many languages, unlike A (per-language).
- It is materially an embedded single-tenant Temporal server: task matching, history, timers,
  replay. The question is whether it beats shipping the dev server (D) once the work is done.
- Adjacent precedent: the Java SDK's time-skipping test server; the CLI dev server.
- [research pending: sdk-core's exact seam, prior maintainer discussions, feasibility.]

### C. Plugin pattern: one loop, swappable execution (two first-hand implementations)

Factor the agent so the LOOP is pure and the execution substrate is injected; Temporal is one
substrate, a local runner is another.

**Evidence 1: this fork.** opencode's v2 engine event-sources every session to a store and exposes
a substitutable `SessionExecution` interface (`active`/`wake`/`resume`/`interrupt`, four methods).
The stock implementation is an in-process coordinator; this fork adds a Temporal-backed one
(`OPENCODE_SESSION_EXECUTION=temporal` per turn, `temporal-turn` per step) plus a shared store so
any worker resumes any session. Because durability lives in the engine's event log, the LOCAL mode
is already crash-recoverable without Temporal; Temporal adds supervised retries, worker
distribution, restart-surviving visibility, and cross-process interrupt/resume. The swap point is
the execution supervisor, not the state store. Verified end to end in this branch (crash tests,
failover tests, an independent architecture review).

**Evidence 2: `agent-harness` (AI-363, TypeScript).** A pure loop state machine plus an `Effects`
interface (`callModel`/`runTools`/`onEvent`) implemented twice: local mode (direct calls, atomic
write-then-rename checkpoint file, crash-resume) and durable mode (workflow + activities with
measured defaults). About 100 lines per runner around a shared core. Local mode gives up retries
across process death mid-tool, multi-worker capacity, and the audit trail; it keeps the same loop,
tools, prompts, and checkpoint crash-resume.

- Durability: local = what the local runner persists (checkpoint file is coarse; an event-sourced
  store is fine-grained and close to Temporal-grade for single-machine crashes). Temporal = full.
- Signals/Updates: the app defines the interface both modes honor (here: wake, interrupt, one
  awaited resume update). No pretense of Temporal's generic protocol locally, and no drift,
  because the loop is the same code.
- Complexity: lowest sustained cost of all paths; the seam is app-defined and small. The cost is
  up-front design: it is not a bolt-on for an existing Temporal-first codebase.
- [research pending: ADK's runner/session-service seam as the pattern's external example; the
  OpenAI Agents SDK integration shape.]

### D. Run the Temporal dev server locally

Ship `temporal server start-dev` alongside the app; the agent stays a plain Temporal application.

Measured here (macOS arm64):

| Metric | Value |
|---|---|
| Binary size | 237 MB (CLI incl. server and embedded UI assets) |
| Cold start to healthy | ~780 ms (`--headless`) |
| RSS idle, empty | ~102 MB |
| RSS after 20 workflow starts | ~128 MB |
| SQLite file | 568 KB empty; 620 KB after 20 workflow starts |
| Persistence | `--db-filename` required; the default is in-memory and loses everything on exit |

Marginal cost measured on THIS app (same engine, same machine):

| Configuration | RSS |
|---|---|
| opencode serve, stock local mode | 297 MB |
| opencode serve, `temporal-turn` (embedded worker) | 494 MB |
| dev server alongside | +123 MB |
| Total for full Temporal semantics locally | ~617 MB across two processes, +237 MB disk |

- Durability and signal/update fidelity: full Temporal semantics, the only path with no gap.
- Complexity for the vendor: near zero code; the cost moves to packaging and lifecycle (bundling
  a 237 MB binary, process supervision, ports, upgrades).
- Redistribution: no licensing issue; shipping it inside a desktop app is a real option. However,
  the motivating desktop customer (Cursor) does not want a bundled server process, so this path is
  parked for the desktop archetype and stays the answer for the appliance/single-machine one.
- [research pending: feature gaps vs the real server.]

### E. Integrate a local option into the Temporal Agent Harness

Not an independent runtime: it is where a choice among A/C/D becomes product. The harness today is
Temporal-native (agents ARE workflows; approvals, Code Mode, callback tools, and the event stream
ride Temporal primitives). A local mode via C would make the harness's public abstractions (agent
definition, tools, approval policy, event stream) the swap seam with a non-Temporal transport
locally (in-process bus, identical schemas); approvals and the event stream are exactly what
desktop users still want.

## Comparison

Scoring: full / partial / none, with the load-bearing caveat inline. [two cells pending research]

| | A: language shim | B: rust-core shim | C: plugin pattern | D: local dev server |
|---|---|---|---|---|
| Durability (crash mid-turn) | partial: what the shim persists; faithful replay = reimplementing Temporal | full IF built (it IS an embedded server) | partial: checkpoint-file coarse; event-sourced local store near-full for one machine | full (with `--db-filename`) |
| Resource needs | lightest (in-process) | in-process, but core carries history+matching | lightest (in-process; store is a file) | heaviest: ~102-123 MB RSS second process, 237 MB disk, ~+200 MB embedded worker |
| Signal/Update support | re-implemented, drift-prone | full IF built | app-defined subset, honored identically by both modes | full |
| Complexity / maintainability | high, permanent (chases SDK surface per language) | highest once, then per-core; a product, not a patch | lowest sustained; up-front loop design | near-zero code; packaging+lifecycle burden |
| Feature fidelity (timers, retries, CAN, child wfs, replay debug) | subset, hand-built | full IF built | not applicable locally (app semantics, not Temporal's) | full minus [research: dev-server gaps] |
| Code-drift risk between modes | medium (same code, different semantics) | low (same code, same semantics) | none for the loop (same code); seam is small | none (same code, same semantics) |
| Desktop packaging | best | good | best | worst (bundle+supervise a server) |
| Upgrade path to Temporal Cloud | same code, repoint | same code, repoint | swap the factory | same code, repoint |
| Offline | yes | yes | yes | yes (local server) |
| Exists today | yes, by customers in their own code | no | yes, twice (this fork; agent-harness) | yes (shipped CLI) |

## Recommendations (draft)

1. **New agent designs: recommend C.** Two working implementations show the cost is one small
   interface; it is the only path with near-zero sustained maintenance and no semantic pretense.
   When local durability matters, event-source the session in the app (this fork's shape) rather
   than reimplementing Temporal's replay.
2. **Existing Temporal-first agents on a machine the customer controls (appliance,
   single-machine server): D.** Full fidelity today; the cost is packaging, not code. Requires
   `--db-filename` and lifecycle supervision. No licensing issue. For the DESKTOP archetype this
   is off the table by customer preference (no bundled server process), which is exactly why C
   (or A as the fallback) carries that case.
3. **A is the fallback** when neither refactoring (C) nor a second process (D) is acceptable:
   scope it to the SDK subset actually used and accept drift risk.
4. **B is product strategy, not a customer workaround**: an embedded Temporal across all
   core-based SDKs. Evaluate only if we want to productize "Temporal without the server".
5. **Harness (E): apply C at the harness API layer** so harness users get a local mode without
   forking the Temporal-native internals.

## Open questions

- What signal/update surface do desktop agents actually need? Evidence here says small and
  enumerable: wake, interrupt, one awaited update (this fork); approvals and steering (harness).
