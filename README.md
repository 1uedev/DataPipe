# DataPipe

A web-based visual data flow platform for industrial data streaming — Node-RED + n8n class. Connectors (MQTT, HTTP/REST, files, SQL (Postgres/MySQL/MSSQL/SQLite), MongoDB, Redis, Kafka, S3, WebSocket, raw TCP/UDP/Serial, Modbus, OPC-UA today; SECS/GEM planned) feed typed datagrams through an internal bus into processors and sinks, authored on a drag-and-drop canvas with live per-node data inspection.

**Status:** in active development. Increments 0–10 of the [development plan](docs/Development-Plan.md) are done: engine (datagram bus, backpressure, hot deploy), control plane (auth/RBAC, encrypted credential vault, audit log, REST API), React editor with schema-driven config forms, live debugging (inspector, debug sidebar, wire counters), a broad connector library (MQTT, HTTP, files, SQL, MongoDB, Redis, Kafka, S3, WebSocket, raw sockets/serial, Modbus, OPC-UA), a processor library (script sandbox, calculator, window/aggregate, switch, filter, merge/join, split/batch, loop, try/catch, lookup, state) built on one sandboxed JavaScript expression language used platform-wide, triggered workflows (webhook-triggered execution tracking with concurrency limits and timeouts, browsable per-node execution history, re-run from start or a failed node, dead letters, flow-level error handlers, crash recovery), edge runtime + fleet management (per-device enrollment tokens, a durable store-and-forward queue so an edge device keeps accepting data through a network outage, CPU/memory/flow-count health reporting, runtime groups with group-targeted deploys, a static `linux/arm64` edge build), observability (Prometheus metrics, structured JSON logs with per-flow level, a built-in monitoring dashboard, alerting hooks with webhook delivery, full config backup/restore via CLI or API), version control (portable flow/project import-export with secret-free connection remapping, named environment profiles resolved at deploy time), and in-product onboarding (an interactive first-flow tutorial, a template gallery).

## Quick start

```bash
make dev        # docker compose: postgres + control plane + runtime, plus the UI dev server
```

Editor at http://localhost:5173 — bootstrap admin credentials come from `DATAPIPE_ADMIN_USERNAME`/`DATAPIPE_ADMIN_PASSWORD` (dev defaults in `deploy/docker-compose.yml`; change for anything non-local).

## Documentation

| Document | Purpose |
|---|---|
| [User Guide](docs/User-Guide.md) | Using the editor: flows, nodes, connections, live debugging |
| [Admin Guide](docs/Admin-Guide.md) | Installation, configuration, users/RBAC, credentials, backup |
| [Requirements Specification](DataPipe-Requirements-Specification.md) | Full requirement catalog with IDs |
| [Architecture](docs/Architecture.md) | Stack decisions, component boundaries, ADRs |
| [Flow File Format](docs/Flow-File-Format.md) | The JSON contract between editor, control plane, runtime |
| [Expression Language](docs/Expression-Language.md) | The JavaScript expression syntax used platform-wide in node config fields |
| [Development Plan](docs/Development-Plan.md) | Increment order with acceptance criteria |
| [TODO](TODO.md) / [DONE](DONE.md) | Live working queue and completed-work log |

## Repository layout

`engine/` Go flow runtime · `controlplane/` Go API server · `ui/` React editor · `proto/` gRPC contracts · `sdk/` plugin SDK · `cli/` command line tool · `deploy/` Docker/compose · `tests/` integration + benchmarks · `docs/` all documentation.

## Development

```bash
make test       # Go + UI unit tests
make itest      # integration tests (docker)
make lint       # go + ts + proto linters
make bench      # benchmark suite
```

Contributions follow [CLAUDE.md](CLAUDE.md): requirement IDs in commits and tests, contract files before implementation, no secrets outside the vault.
