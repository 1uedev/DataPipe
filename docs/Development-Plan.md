# DataPipe — Phased Development Plan

**Basis:** Requirements spec v1.0, Architecture.md, Flow-File-Format.md.
Each increment is independently buildable, testable, and demoable. Requirement IDs in parentheses are the acceptance scope. Order matters: every increment builds only on previous ones.

## Increment 0 — Repo and walking skeleton

Monorepo per Architecture §4; CI (lint, test, build); protobuf toolchain; `docker compose up` starts empty control plane + runtime + Postgres; runtime registers via gRPC; health endpoint.
**Done when:** fresh clone → one command → all services healthy; CI green.

## Increment 1 — Datagram + internal bus (the heart)

`engine/datagram` (DGM-100..140 incl. batches, quality, lineage ids), `engine/bus` (BUS-100/110/140: wires, bounded queues, backpressure policies, fan-out/fan-in), context store (ENG-120, memory backend).
**Done when:** unit tests cover all envelope semantics and overflow policies; bench: ≥ 50k dgm/s through a 3-node in-process pipeline on dev hardware.

## Increment 2 — Flow model + engine lifecycle

Flow file parser/validator/canonical serializer (Flow-File-Format §2–§8, round-trip test), graph instantiation, node lifecycle (create/start/stop), hot deploy of modified nodes (ENG-140), error policies (ERR-100), resource guardrails (ENG-150). First three trivial nodes: `inject`, `set`, `debug-log`.
**Done when:** CLI deploys a flow file to the runtime; inject→set→log runs; modified-node redeploy keeps untouched nodes running (test proves it).

## Increment 3 — Control plane core + REST API

OpenAPI-first: projects, flows CRUD, versions with immutable history (VCS-110), deploy orchestration, connections + encrypted credentials (SEC-120), local-user auth + RBAC roles (SEC-100/110), audit log (SEC-140). SQLite + Postgres backends.
**Done when:** full flow lifecycle works via REST only (ARC-110); permissions enforced in API tests; secrets never appear in any response or export.

## Increment 4 — Editor MVP

React app: canvas (React Flow), palette, node config panels generated from JSON Schema, wiring with validation, undo/redo, copy/paste, subflow-less editing, deploy button with scope choice + pre-deploy validation UI (UI-100..170, UI-200/210), light/dark (UI-300), en+de (UI-310).
**Done when:** a user builds and deploys inject→set→debug entirely in the browser; usability check with one target-persona user.

## Increment 5 — Live debugging (the differentiator)

Debug data channel runtime→control plane→browser (sampled, DBG-170); node/wire inspector with ring buffer (DBG-100); debug sidebar node (DBG-110); wire pulse animation + counters (DBG-120); design-time single-node execution + data pinning (DBG-130).
**Done when:** clicking any node shows live datagrams < 500 ms after they pass; 10k dgm/s flow keeps UI responsive (sampling proven by test).

## Increment 6 — First real connectors (vertical slice)

MQTT in/out (CON-200, SNK-110), HTTP in/webhook + response (CON-300, SNK-170), HTTP/REST client (CON-315/SNK-160 P1 scope), schedule (CON-330), file watch + CSV/JSON readers (CON-400/410 partial), SQL Postgres source+sink (CON-500, SNK-190), bus in/out named topics (CON-600, SNK-220, BUS-120). Connection test button (CON-140), reconnect helper (CON-130), selection preview (MAP-110).
**Done when:** demo flow "MQTT sensor → window avg → Postgres" runs 24 h unattended; every connector has integration tests against containerized targets.

## Increment 7 — Processor library P1

Script node with goja sandbox + limits (PROC-100), convert (PROC-120), template (PROC-130), calculator (PROC-200), window/aggregate (PROC-210), switch/filter/merge/split/loop/delay (PROC-300..350), try/catch scope (PROC-370), lookup + cache (PROC-400), state node (PROC-410), sub-flow call + subflow UI (PROC-160, UI-140), expression language complete (MAP-130/150).
**Done when:** spec's Section 10 P1 table fully green in the node test harness; expression docs published.

## Increment 8 — Triggered workflows

Execution tracking, history with per-node I/O, re-run from node (ENG-130, DBG-140), error flows + DLQ + stop-and-error (ERR-120/130/140), concurrency limits, crash recovery marking (ERR-150).
**Done when:** webhook-triggered workflow with a failing node shows inspectable history and re-runs successfully after fix.

## Increment 9 — Edge runtime + fleet

Edge build (static binary, EDGE-110), enrollment + fleet UI (EDGE-120), outbound-only mgmt channel (ARC-210), store-and-forward (EDGE-130), flow assignment to runtime groups (UI-220).
**Done when:** edge device on ARM64 runs a flow through a 30-min network cut with zero data loss to the server sink; footprint ≤ 512 MB device proven.

## Increment 10 — Remaining P1 connectors + hardening

OPC-UA (CON-210 P1 modes + namespace browser), Modbus (CON-230), Kafka (CON-260), raw TCP/UDP/serial + binary parse (CON-290, PROC-120 binary), Excel/XML readers, MySQL/MSSQL/SQLite, MongoDB, Redis, S3 files, WebSocket (CON-320), remaining sinks. Observability P1 (OBS-100..150), import/export (VCS-130), env profiles (VCS-140), onboarding tutorial (UI-330).
**Done when:** MVP exit criteria of spec Section 23 met, incl. Section 3.3 success criteria 1, 2, 4; 7-day soak test started (NFR-130).

## Increment 11 — SECS/GEM (parallel track from Inc. 6 onward)

HSMS transport → SECS-II codec + message dictionary → GEM host capabilities (CON-220), report builder UI (MAP-100), host action sink (SNK-130). Test against an equipment simulator from day one; decide native-vs-sidecar per ADR-003 after the HSMS spike.
**Done when:** GEM establish-communication, event report setup, and trace collection work against the simulator and one real/reference equipment.

## Working agreements for every increment

1. Spec-first: OpenAPI/proto/JSON-Schema changes land before implementation.
2. Every requirement ID touched gets a test naming that ID.
3. Benchmarks run in CI from Increment 1 on; NFR-100 regression gate active from Increment 6.
4. Demo at the end of each increment against the "Done when" line — no partial credit.
