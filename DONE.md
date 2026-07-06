# DONE — Completed Work Log

Reverse chronological. Every finished step gets one entry: date, what was done, requirement IDs touched, commit hash(es).

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
