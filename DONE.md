# DONE — Completed Work Log

Reverse chronological. Every finished step gets one entry: date, what was done, requirement IDs touched, commit hash(es).

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
