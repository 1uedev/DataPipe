# DataPipe — Architecture Document

**Version:** 1.0 (Draft) · **Date:** 2026-07-06 · **Basis:** DataPipe-Requirements-Specification.md v1.0

This document turns the stack-agnostic requirements into concrete architecture and technology decisions. Requirement IDs (ARC-xxx, ENG-xxx, ...) refer to the specification.

---

## 1. Component overview

```
┌────────────────────────────────────────────────────────────────┐
│  Browser: Editor Frontend (SPA)                                │
│  canvas · palette · config panels · debug inspector · admin    │
└───────────────┬──────────────────────────┬─────────────────────┘
                │ REST (OpenAPI)           │ WebSocket (live)
┌───────────────▼──────────────────────────▼─────────────────────┐
│  Control Plane (API server)                                    │
│  auth/RBAC · flow store + versions · deploy orchestration ·    │
│  credential vault · fleet mgmt · execution history · metrics   │
└───────┬───────────────────────────────────────────┬────────────┘
        │ gRPC/WebSocket (runtime protocol,          │
        │ runtime-initiated for edge)                │
┌───────▼────────────────┐              ┌────────────▼───────────┐
│  Server Runtime(s)     │   bus link   │  Edge Runtime(s)       │
│  engine · internal bus │◄────────────►│  same engine, small    │
│  connector plugins     │              │  footprint, buffering  │
└───────┬────────────────┘              └────────────┬───────────┘
        │                                            │
   PostgreSQL (config/history)                 machines: OPC-UA,
   optional NATS (durable topics,              SECS/GEM, MQTT,
   queue mode, bus links)                      Modbus, serial ...
```

## 2. Per-component technology comparison and recommendation

### 2.1 Execution runtime (server + edge) — the critical decision

| Criterion | Node.js/TypeScript | Go | .NET 8+ | Rust |
|---|---|---|---|---|
| Industrial protocol libraries | ◎ node-opcua (best OSS OPC-UA), mqtt.js, modbus-serial, kafkajs; SECS: weak, own implementation needed | ○ gopcua (client ok), paho, modbus ok; SECS: none usable | ◎ OPC UA .NET Standard (reference impl.), MQTTnet, NModbus, **secs4net** (solid SECS/GEM) | △ open62541 bindings, rumqtt; SECS: none |
| Throughput target (10k dgm/s, NFR-100) | ○ reachable with worker threads, care needed | ◎ | ◎ | ◎ |
| Latency p99 < 50 ms | ○ GC/event-loop acceptable | ◎ | ◎ | ◎ |
| Edge footprint (512 MB device, EDGE-110) | △ ~100–150 MB RSS + node binary | ◎ single static binary, ~20 MB | ○ AOT-compiled ~40–80 MB | ◎ |
| Script node (PROC-100: user JS/TS) | ◎ native, easy sandboxing (isolated-vm) | △ embed goja (slower JS) | ○ embed ClearScript/V8 or Jint | △ embed deno_core/QuickJS |
| Plugin SDK ergonomics for community (SDK-100) | ◎ npm ecosystem, JS is the automation lingua franca | ○ | ○ | △ |
| Team/AI-assisted velocity | ◎ | ◎ | ◎ | ○ |

**Recommendation: Go for the engine core, with an embedded JavaScript sandbox (goja) for script nodes, and out-of-process plugin support for protocol gaps.**

Reasoning: the engine's dominant requirements are streaming throughput, backpressure, per-flow isolation (goroutines + channels map 1:1 onto wires and queues, ARC-150/BUS-110), and a tiny static edge binary (EDGE-110) — Go is strongest exactly there, and one codebase serves server and edge. The two library gaps are closed explicitly:

* **SECS/GEM (CON-220)**: no ecosystem has a great OSS answer except .NET (secs4net). Decision: implement HSMS/SECS-II natively in Go (the protocols are well-specified binary framing — a good fit), or run secs4net in a .NET sidecar plugin via the out-of-process plugin API. Start native; the sidecar is the fallback.
* **OPC-UA (CON-210)**: gopcua covers client subscribe/read/write/browse (sufficient for P1); for the P2 OPC-UA *server* sink, wrap open62541 (C) or use a sidecar.

Rejected alternatives: pure TypeScript fails the edge footprint and CPU-bound windowing targets without significant complexity (worker orchestration); .NET is a close second (best industrial libraries) and remains the designated sidecar language for SECS if native proves too costly; Rust maximizes performance but minimizes contributor and plugin-author reach.

### 2.2 Control plane (API server)

**Recommendation: Go, same repository, separate binary.** Shares datagram/flow-model code with the runtime, one language for the whole backend, single-binary on-prem install (ARC-130: control plane + runtime can run in one process for small installs — "all-in-one" mode). Auth via OIDC library, RBAC in middleware (SEC-100/110).

### 2.3 Editor frontend

**Recommendation: React + TypeScript.**

* Canvas: **React Flow (xyflow)** — proven for exactly this UI category (n8n uses a Vue equivalent), custom node rendering, ports, edge routing, minimap, 500-node performance achievable with virtualization (NFR-110). Fallback if limits are hit: custom WebGL/canvas layer behind the same component API.
* State: Zustand or Redux Toolkit; server cache: TanStack Query; live channel: native WebSocket with a thin protocol client.
* Config panels generated from node config schemas (JSON Schema → form renderer) so SDK plugins get UIs for free (SDK-100).
* Component library: headless (Radix) + Tailwind, light/dark themes (UI-300), i18n via ICU messages (UI-310).

### 2.4 Persistence

| Data | Store | Notes |
|---|---|---|
| Flows, versions, projects, users, RBAC, connections, fleet | **PostgreSQL** | JSONB for flow documents; also the SQLite option for all-in-one/edge-less small installs |
| Credentials | PostgreSQL, envelope-encrypted (AES-256-GCM, KMS/keyfile master key) | SEC-120; external vault P2 |
| Execution history + per-node I/O (DBG-140) | PostgreSQL, partitioned tables, size-capped payload blobs | retention jobs |
| Durable bus topics, queue mode, bus links (BUS-130, ENG-200, ARC-230) | **NATS JetStream**, embedded/optional | pure in-process bus needs no broker; JetStream only when durability/scale-out is enabled — keeps single-binary installs simple |
| Edge store-and-forward (EDGE-130) | Embedded KV/log (bbolt or Pebble) | size/time-bounded ring |
| Metrics | Prometheus exposition (OBS-100); embedded TSDB optional for the built-in dashboard (OBS-110) | |

### 2.5 Runtime ↔ control plane protocol

gRPC over TLS, runtime-initiated (edge dials out, ARC-210), with streams for: deploy commands, flow status, health, debug/inspection data (sampled, DBG-170), log shipping. Protobuf schemas versioned in `/proto`.

### 2.6 Plugin model (SDK-100 ff.)

* **In-process Go plugins** for first-party connectors (compiled in; Go's plugin system is not used — connectors are statically registered but SDK-shaped, SDK-120).
* **Out-of-process plugins** (any language) speaking a documented gRPC plugin protocol — this is the community/custom path (install without restart, SDK-110) and the sidecar path for .NET SECS or Python nodes. HashiCorp go-plugin pattern.
* Node config schema: JSON Schema + UI hints → editor renders forms automatically.

### 2.7 Datagram in memory and on the wire

In-memory: Go struct per DGM-100 with copy-on-write payload for fan-out (BUS-140); binary payloads by reference over a shared buffer pool above 256 KB (DGM-120). Wire encoding for bus links and out-of-process plugins: Protobuf (DGM-170) with canonical JSON mapping for the editor/debug views.

## 3. Key architectural decisions (ADR summary)

| # | Decision | Status |
|---|---|---|
| ADR-001 | Go engine core, one codebase for server and edge runtime | proposed |
| ADR-002 | JS script nodes via embedded goja; Python via out-of-process plugin (P2) | proposed |
| ADR-003 | SECS/GEM implemented natively in Go (HSMS first); .NET secs4net sidecar as fallback | proposed |
| ADR-004 | React + React Flow editor; config UIs generated from JSON Schema | proposed |
| ADR-005 | PostgreSQL as system store; SQLite mode for all-in-one installs | proposed |
| ADR-006 | NATS JetStream optional for durability/scale-out; in-process bus default | proposed |
| ADR-007 | gRPC runtime protocol, runtime-initiated connections | proposed |
| ADR-008 | Out-of-process gRPC plugin protocol for third-party/custom nodes | proposed |
| ADR-009 | Monorepo (Go workspace + pnpm workspace), single versioned release train | proposed |

## 4. Repository structure (monorepo)

```
datapipe/
├── CLAUDE.md
├── docs/                    # this doc, spec, flow-format, dev plan, ADRs/
├── proto/                   # gRPC + datagram protobuf (source of truth)
├── engine/                  # Go: runtime core
│   ├── bus/                 # internal bus, backpressure, topics
│   ├── datagram/            # envelope, batches, serialization
│   ├── flow/                # graph model, deploy/lifecycle, state
│   ├── nodes/               # built-in processors (Section 10)
│   ├── connectors/          # sources/sinks, one package each
│   ├── script/              # goja sandbox
│   └── edge/                # store-and-forward, mgmt client
├── controlplane/            # Go: API server, auth, store, fleet
├── ui/                      # React editor + admin
├── sdk/                     # plugin SDK: Go lib + gRPC protocol + docs
├── cli/                     # datapipe CLI (API-130)
├── deploy/                  # docker compose, helm, all-in-one build
└── tests/                   # e2e + benchmark suite (NFR-100/130)
```

## 5. Cross-cutting implementation rules

1. Every feature lands behind its requirement ID; PR descriptions cite IDs.
2. The public REST API is generated from OpenAPI specs in `docs/api/` — spec first, then handlers (ARC-110).
3. All connector I/O uses context-based cancellation and the reconnect helper (CON-130) — no connector implements its own retry loop.
4. No node accesses another node's state; all cross-node communication is datagrams or the context store API (ENG-120).
5. Benchmarks (`tests/bench`) run in CI on every merge to main; regression > 10% fails the build (NFR-100).
