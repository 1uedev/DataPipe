# DataPipe

A web-based visual data flow platform for industrial data streaming — Node-RED + n8n class. Connectors (MQTT, HTTP/REST, files, PostgreSQL today; OPC-UA, Modbus, Kafka, SECS/GEM planned) feed typed datagrams through an internal bus into processors and sinks, authored on a drag-and-drop canvas with live per-node data inspection.

**Status:** in active development. Increments 0–7 of the [development plan](docs/Development-Plan.md) are done: engine (datagram bus, backpressure, hot deploy), control plane (auth/RBAC, encrypted credential vault, audit log, REST API), React editor with schema-driven config forms, live debugging (inspector, debug sidebar, wire counters), the first seven connector families, and a processor library (script sandbox, calculator, window/aggregate, switch, filter, merge/join, split/batch, loop, try/catch, lookup, state) built on one sandboxed JavaScript expression language used platform-wide.

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
