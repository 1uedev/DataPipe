# DataPipe — Project Context for Claude Code

DataPipe is a web-based visual data flow platform (Node-RED + n8n class) focused on industrial data streaming: connectors (MQTT, OPC-UA, SECS/GEM, Modbus, REST, files, SQL/NoSQL/graph/vector DBs, enterprise buses) feed typed datagrams through an internal bus into processors and sinks, authored on a drag-and-drop canvas with live per-node data inspection.

## Read these before non-trivial work

| Document | Purpose |
|---|---|
| `DataPipe-Requirements-Specification.md` | WHAT to build. All requirement IDs (ARC/DGM/CON/MAP/BUS/PROC/SNK/UI/DBG/ENG/EDGE/VCS/ERR/SEC/OBS/SDK/NFR/API-xxx) live here |
| `docs/Architecture.md` | Stack decisions (Go engine + control plane, React/React Flow UI, Postgres, optional NATS), component boundaries, repo layout, ADRs |
| `docs/Flow-File-Format.md` | The core JSON contract between editor, control plane, runtime — implement against this, never invent fields |
| `docs/Development-Plan.md` | Increment order and "Done when" acceptance lines — build in this order |
| `TODO.md` | The live working queue — always pick the top unchecked item |
| `DONE.md` | Log of completed steps with requirement IDs and commit hashes |

## Ground rules

1. **Requirement IDs everywhere**: every PR/commit description and every test for a requirement cites its ID (e.g. `TestBUS110_BackpressureDropOldest`). If code contradicts the spec, stop and flag it — don't silently deviate.
2. **Spec-first for contracts**: changes to REST API (OpenAPI in `docs/api/`), gRPC (`proto/`), flow format, or node config schemas land in the contract file before implementation.
3. **No secrets in flows or exports** — flows reference connection ids, connections reference credential ids (SEC-120). Any code path serializing a credential value outside the vault is a bug.
4. **Determinism**: flow files serialize canonically (sorted keys, id-ordered arrays, 2-space indent) and must round-trip byte-identically (Flow-File-Format §7.6).
5. **Engine rules**: connectors use the shared reconnect/cancellation helpers (CON-130), never own retry loops; nodes communicate only via datagrams or the context store API; a panicking node must never take down the runtime (ARC-150 — recover at node boundary).
6. **Backpressure is sacred**: nothing buffers unboundedly. Every queue has a limit and a configured overflow policy (BUS-110); drops are counted in metrics.

## Git workflow

Remote: `https://github.com/1uedev/DataPipe.git` (origin, branch `main`).

* Commit after every completed work step with a message citing the requirement IDs touched; **push to origin after each step/increment** so the remote always reflects the latest state.
* Never commit secrets, credentials, or `.env` files (see `.gitignore`); flow exports must be secret-free by construction (SEC-120).
* If a push fails for lack of credentials in the current environment, finish the commit locally and tell Holger to push — never leave work uncommitted.
* **After every completed step**: move the finished item from `TODO.md` to `DONE.md` (date, summary, requirement IDs, commit hash) and add any newly discovered follow-up work to `TODO.md`. These two files are part of the same commit as the work itself.

## Stack and layout (from Architecture.md)

Go ≥ latest stable for `engine/`, `controlplane/`, `cli/`, `sdk/`; TypeScript + React + React Flow in `ui/`; Protobuf in `proto/` is the source of truth for runtime and plugin protocols; PostgreSQL (SQLite for all-in-one mode); NATS JetStream only behind the durability/scale-out feature flag. Monorepo: Go workspace + pnpm workspace.

## Conventions

* Go: standard `gofmt`/`golangci-lint` config in repo; table-driven tests; contexts passed explicitly; errors wrapped with `%w` and typed where matched on.
* TypeScript: strict mode; no `any` in exported signatures; components colocate stories/tests; UI strings via i18n keys only (en+de).
* Tests: unit tests beside code; integration tests with containerized targets in `tests/`; benchmarks in `tests/bench` — a >10% throughput regression fails CI (NFR-100).
* Node config UIs are generated from JSON Schema in the node manifest — never hand-build a config form in the editor for a specific node type.

## Commands

(fill in as the repo takes shape — keep this section current)

```
make dev          # compose up: control plane + runtime + postgres + ui dev server
make test         # all unit tests
make itest        # integration tests (needs docker)
make bench        # benchmark suite
make lint         # go + ts linters
```

## Current state

Increments 0–5 done: monorepo skeleton + CI (0); datagram/bus/ctxstore (1); flow model, node runner, hot deploy, inject/set/debug-log nodes, CLI (2); control plane (auth/RBAC, credential vault, audit log, REST API, deploy orchestration) (3); React Flow editor (canvas, palette, schema-driven config, undo/redo, deploy) (4); live debugging — DebugChannel gRPC stream, per-node ring buffers + rate-limited live forwarding, debug sidebar node, WebSocket relay, node/wire inspector, wire pulse + counters, design-time run-once + data pinning (5). Next step: Increment 6 of `docs/Development-Plan.md` (first real connectors: MQTT, HTTP/REST, schedule, files, Postgres, bus topics). Update this section whenever an increment completes.
