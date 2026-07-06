# DONE — Completed Work Log

Reverse chronological. Every finished step gets one entry: date, what was done, requirement IDs touched, commit hash(es).

## 2026-07-06 — Increment 2: flow model + engine lifecycle

* **Flow file model + canonical serializer** (Flow-File-Format §2-3, §7.6): `engine/flow/file.go` (FlowFile/Graph/Node/Wire/ErrorPolicy/Layout/subflow-interface structs matching the spec's field names exactly), `engine/flow/canonical.go` (sorted keys via a `json.Number` generic round-trip, nodes/wires/env sorted by id/name, 2-space indent). Proven a stable fixed point and independent of input array order by tests using the spec's own example flow.
* **Validator** (§7 subset): `engine/flow/validate.go` — unique node/wire ids, wire endpoint existence and port direction (including the implicit `error` output port), registered node types, ENG-100 streaming-mode-requires-a-source-node. Connection-ref resolution and JSON-Schema config validation (§7 rules 2-3) deferred (see TODO.md).
* **Node execution** (`engine/flow/node.go`, `registry.go`, `runner.go`): `Source`/`Processor` interfaces behind a global type registry; the runner recovers panics at the node boundary (ARC-150) and applies ERR-100's uniform per-node error policy (fail / retry-with-backoff / errorPort / discard), building the ERR-100 error object (message/code/node/stack/attempt) and error datagram for the errorPort case.
* **Graph instantiation + hot deploy** (ENG-140, `engine/flow/graph.go`): `Deploy` wires nodes together over `engine/bus` and restarts only nodes whose own definition or incident wiring changed; `bus.Wire` object identity is tracked per (node, input port) independently of node-instance identity so a kept node never writes into an abandoned wire when a neighbor restarts. `NodeStats` exposes start counts for hot-deploy observability.
* **Three trivial node types**: `engine/nodes/inject` (CON-600-style manual/repeat trigger), `engine/nodes/set` (PROC-110 minimal literal field ops), `engine/nodes/debuglog` (DBG-110 minimal, logs via `slog`).
* **CLI**: `datapipe deploy <flow.json> [-for <duration>]` embeds the engine in-process (ARC-130 all-in-one style); `examples/inject-set-log.flow.json` demonstrates inject → set → debug-log — verified manually, logs the processed datagram once per second.
* **Verified**: `TestENG140_HotDeployRestartsOnlyModifiedNodes` proves a redeploy changing only one node's config restarts just that node while its untouched neighbors keep running and the pipeline keeps delivering with the new config in effect; `make lint`, `make build`, `make test` pass; every new package passes under `go test -race`. `(ef2e8cb)`

## 2026-07-06 — ADR review (partial): ADR-001 accepted, ADR-003 deferred

* Holger approved **ADR-001** (Go engine core) — `docs/Architecture.md` status updated to accepted.
* **ADR-003** (SECS/GEM native-vs-sidecar): decision explicitly deferred to the Increment 11 HSMS spike, not a blocker for Increment 1 — this matches what Development-Plan.md already assumed for that ADR. `(docs/Architecture.md updated in 555ac19)`
* Remaining ADRs (002, 004–009) not reviewed yet; none block Increment 1/2.

## 2026-07-06 — Increment 1: datagram + internal bus

* **`engine/datagram`** (DGM-100..140, DGM-160): `Header`/`Source`/`Payload`/`Datagram` envelope, `Quality` with worst-of `Combine` (DGM-140), `Batch` (DGM-130), `New`/`NewCaused` constructors propagating correlation/causation ids end to end (DGM-160), `Clone` implementing BUS-140 independent-copy semantics (deep-copies tags; binary payloads at/above `DefaultBinaryRefThreshold` (256 KiB) shared by reference per DGM-120, copied below it).
* **`engine/bus`** (BUS-100/110/140): `Wire` — bounded ring-buffer queue, in-order per-wire delivery, all four BUS-110 overflow policies (block, drop-oldest, drop-newest, sample-every-Nth) with context-cancellable blocking and delivered/dropped metrics; `FanOut` (clones per destination) and `FanIn` (merges n wires, interleaves in arrival order).
* **`engine/ctxstore`** (ENG-120): node/flow/global-scoped context store behind a pluggable `Store` interface; `MemoryStore` backend; state keyed by (scope, flowId, nodeId, name) so it survives redeploys that keep the same node id; `Keys`/`Delete` for editor/API inspection.
* **`tests/`**: new Go module (added to `go.work`) with a 3-node in-process pipeline (source → processor → sink over two `Wire`s); `TestThreeNodePipelineCorrectness` proves ordering + lineage; `BenchmarkThreeNodePipeline` measured **~1.16M dgm/s** on dev hardware, well above the Increment 1 target of ≥50k dgm/s.
* **CI**: `go test -race` now runs across every Go module (including `tests`); the benchmark runs in CI as report-only (the NFR-100 >10% regression gate activates from Increment 6 per the plan).
* **Verified locally**: `make lint`, `make build`, `make test`, `make bench` all pass; every package test suite passes under `-race`. `(555ac19)`

## 2026-07-06 — Increment 0: repo skeleton, CI, walking skeleton

* **Monorepo layout** per `docs/Architecture.md` §4: Go workspace (`go.work`) with modules `proto/gen/go`, `engine`, `controlplane`, `cli`, `sdk`; pnpm workspace for `ui/`.
* **Protobuf toolchain** (ARC/ADR-007): `proto/` managed by buf; `datapipe.runtime.v1.RuntimeRegistryService` (Register/Heartbeat) is the first slice of the runtime↔control-plane protocol; generated Go code committed, CI checks it isn't stale.
* **Engine runtime walking skeleton**: `engine/cmd/runtime` — `/healthz` endpoint, gRPC client that registers and heartbeats with the control plane, shared exponential-backoff reconnect helper (`engine/internal/backoff`) meant for reuse by connectors (CON-130).
* **Control plane walking skeleton**: `controlplane/cmd/controlplane` — gRPC server implementing `RuntimeRegistryService` (in-memory registry) plus a Postgres-backed `/healthz`.
* **CLI and SDK placeholders**: `cli/cmd/datapipe` (`version` subcommand), `sdk/go` stub — filled in from Increment 3 and Increment 6+ respectively.
* **UI scaffold**: `ui/` — Vite + React + TypeScript (strict mode), oxlint, minimal placeholder page; canvas work starts Increment 4.
* **Docker/Compose**: `deploy/controlplane.Dockerfile`, `deploy/runtime.Dockerfile`, `deploy/docker-compose.yml` bring up postgres + control plane + runtime with health checks; `make dev` is the one-command path (root `Makefile`: dev/build/test/lint/proto targets).
* **CI**: `.github/workflows/ci.yml` — buf lint + generate-drift check, Go build/vet/test/golangci-lint (`.golangci.yml`) across all Go modules, ui lint/build, and a docker-compose smoke test asserting all three services go healthy.
* **Verified locally**: `make lint`, `make build`, `make test` all pass; `docker compose up --build` brings postgres/controlplane/runtime to healthy and the runtime registers over gRPC (confirmed via logs and `/healthz` on all three services). `(1b58804)`
* **First push to origin** confirmed working (`main` already tracking `origin/main`, `eb849d1`/`330811d` present on remote) — no separate credential step needed in this environment.

## 2026-07-06 — Documentation package and repo setup

* **Requirements specification** written: `DataPipe-Requirements-Specification.md` — 25 sections, ~200 requirements with IDs, MUST/SHOULD/MAY and P1–P3 priorities; includes Node-RED/n8n reference analysis, datagram model, full connector catalog (incl. REST API client CON-315 and enterprise bus connectors CON-700..770 / SNK-155 added on review), editor UI, live debugging, dual execution model, edge/fleet, security, SDK, NFRs, release phasing. `(330811d)`
* **Architecture document**: `docs/Architecture.md` — per-component stack comparison and recommendation (Go engine + control plane, goja script sandbox, React/React Flow UI, PostgreSQL, optional NATS JetStream), 9 ADRs, monorepo layout, cross-cutting rules. `(330811d)`
* **Flow file format**: `docs/Flow-File-Format.md` — canonical JSON contract (flows, subflows, connections, profiles, validation rules, versioning). `(330811d)`
* **Development plan**: `docs/Development-Plan.md` — 12 increments with "Done when" acceptance lines. `(330811d)`
* **CLAUDE.md** — project context, ground rules, conventions for Claude Code sessions. `(330811d)`
* **Git repo initialized**: origin `https://github.com/1uedev/DataPipe.git`, branch `main`, `.gitignore`; push-after-each-increment workflow documented in CLAUDE.md and Development-Plan. `(330811d, eb849d1)` — first authenticated push still open (see TODO).
