# DataPipe — Software Requirements Specification (SRS)

**Project:** DataPipe — Web-based Visual Data Flow and Integration Platform
**Version:** 1.0 (Draft)
**Date:** 2026-07-06
**Author:** Holger Gerlach
**Status:** Draft for review

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Reference Analysis: Node-RED and n8n](#2-reference-analysis-node-red-and-n8n)
3. [Product Vision and Scope](#3-product-vision-and-scope)
4. [Users and Personas](#4-users-and-personas)
5. [System Architecture Requirements](#5-system-architecture-requirements)
6. [The Datagram Model (Internal Bus Message Format)](#6-the-datagram-model)
7. [Functional Requirements — Connectors (Sources)](#7-functional-requirements--connectors-sources)
8. [Functional Requirements — Data Selection and Mapping](#8-functional-requirements--data-selection-and-mapping)
9. [Functional Requirements — Internal Data Bus](#9-functional-requirements--internal-data-bus)
10. [Functional Requirements — Processing Nodes (Work Items)](#10-functional-requirements--processing-nodes-work-items)
11. [Functional Requirements — Sinks (Outputs)](#11-functional-requirements--sinks-outputs)
12. [Functional Requirements — Flow Editor UI](#12-functional-requirements--flow-editor-ui)
13. [Functional Requirements — Live Data Inspection and Debugging](#13-functional-requirements--live-data-inspection-and-debugging)
14. [Functional Requirements — Execution Engine](#14-functional-requirements--execution-engine)
15. [Functional Requirements — Deployment Topologies and Edge](#15-functional-requirements--deployment-topologies-and-edge)
16. [Functional Requirements — Projects, Versioning, Collaboration](#16-functional-requirements--projects-versioning-collaboration)
17. [Functional Requirements — Error Handling and Reliability](#17-functional-requirements--error-handling-and-reliability)
18. [Functional Requirements — Security](#18-functional-requirements--security)
19. [Functional Requirements — Observability and Administration](#19-functional-requirements--observability-and-administration)
20. [Functional Requirements — Extensibility (Plugin SDK)](#20-functional-requirements--extensibility-plugin-sdk)
21. [Non-Functional Requirements](#21-non-functional-requirements)
22. [Public APIs](#22-public-apis)
23. [Release Phasing (MVP → Full Product)](#23-release-phasing)
24. [Out of Scope](#24-out-of-scope)
25. [Glossary](#25-glossary)

---

## 1. Introduction

### 1.1 Purpose

This document specifies the complete requirements for **DataPipe**, a new web-based, visual data flow and integration platform. DataPipe combines the strengths of two established open-source tools:

* **Node-RED**: low-latency, event-driven wiring of hardware, protocols, and message streams, with live message inspection on every wire.
* **n8n**: triggered business workflow automation with rich per-node data mapping, sub-workflows, robust error handling, and horizontal scaling via queue mode.

DataPipe's primary focus is **industrial data streaming**: continuous acquisition from machines and shop-floor systems (MQTT, SECS/GEM, OPC-UA and others), real-time transformation and routing of that data through a well-defined **internal bus with typed datagrams**, and delivery to storage systems, files, or downstream buses. Event/trigger-based workflows are supported as a second execution mode.

This specification is **technology-stack agnostic**. It defines WHAT the system must do, not which programming language, framework, or database implements it. Where architecture is discussed, it constrains structure and interfaces, not products.

### 1.2 Requirement Notation

Each requirement has a unique ID (`<AREA>-<number>`), a priority, and normative language:

* **MUST** — mandatory (MVP unless stated otherwise)
* **SHOULD** — important, expected in the full product
* **MAY** — optional / future enhancement

Priorities: **P1** (MVP), **P2** (Version 1.x), **P3** (later/optional).

### 1.3 Definitions (short)

* **Flow**: a directed graph of nodes connected by wires, authored on the canvas.
* **Node / Work Item**: a single processing unit in a flow (source, processor, or sink).
* **Wire**: a connection between an output port and an input port.
* **Datagram**: the standardized message envelope traveling on the internal bus (Section 6).
* **Connector**: a node type that communicates with an external system or protocol.
* **Edge Runtime**: a lightweight execution engine deployed close to machines, managed centrally.

A full glossary is in Section 25.

---

## 2. Reference Analysis: Node-RED and n8n

This section summarizes the functionality of the two reference tools. DataPipe requirements in later sections are derived from this analysis.

### 2.1 Node-RED — capabilities to replicate

| Capability | Description | DataPipe requirement |
|---|---|---|
| Event-driven flow engine | Messages flow through nodes as they arrive; no explicit "run" needed; flows run continuously after deploy | Section 14 (streaming mode) |
| Browser-based canvas | Drag and drop nodes from a palette, wire them, deploy with one click | Section 12 |
| Live debug sidebar | `debug` nodes and wire inspection show real message payloads as they pass | Section 13 |
| Palette / node ecosystem | 4000+ community nodes installable at runtime without restart | Section 20 |
| Subflows | Group nodes into a reusable node with its own properties and instance parameters | UI-140 ff. |
| Context store | State scoped to node, flow, or global; pluggable persistence (memory, file) | ENG-120 ff. |
| Protocol depth | First-class MQTT, HTTP, WebSocket, TCP/UDP, serial; contrib nodes for OPC-UA, Modbus, S7, BACnet | Section 7 |
| Function node | Inline JavaScript on messages with full message access | PROC-100 |
| Deploy granularity | Full deploy, modified-flows deploy, modified-nodes deploy without stopping the rest | ENG-140 |
| Environment variables & credentials | Per-flow env vars, encrypted credential store | Section 18 |
| Projects + git | Flow source under version control | Section 16 |
| Headless admin API | REST API to deploy flows, manage palette | Section 22 |

Known Node-RED weaknesses DataPipe must fix: single-threaded runtime with limited horizontal scaling, weak multi-user story (one shared editor), no built-in RBAC, no typed message schema, limited flow-level error handling, no native multi-tenant or fleet management (FlowFuse sells this as an add-on product).

### 2.2 n8n — capabilities to replicate

| Capability | Description | DataPipe requirement |
|---|---|---|
| Trigger-based execution | Workflows start from webhooks, cron schedules, polling, manual runs, or app events | Section 14 (trigger mode) |
| Items model | Each node receives/returns an array of items; nodes iterate automatically | Section 6 (datagram batches) |
| Data pinning & step execution | Pin sample data to a node, execute single nodes during design | DBG-130 |
| Expressions & mapping UI | Drag fields from previous node output into parameter fields; expression language on every parameter | Section 8 |
| Error workflows | Global error trigger, per-node retry/continue-on-fail, stop-and-error node | Section 17 |
| Sub-workflows | Call reusable workflows with defined input/output contracts | PROC-160 |
| Queue mode | Main instance + worker processes over a message broker for horizontal scaling | ENG-200 |
| Credentials management | Central encrypted credentials, OAuth2 flows, sharing rules | Section 18 |
| Versioning | Workflow version history with diff and rollback | Section 16 |
| 400+ integrations | SaaS/API connectors with per-operation parameter UIs | Section 7.9 |
| AI/agent nodes | LLM chains, agents, vector store nodes | Section 7.7, P3 |
| Loops, merge, switch, wait | Rich control-flow primitives incl. time-based wait and human-in-the-loop | Section 10 |

Known n8n weaknesses DataPipe must fix: not designed for continuous high-frequency streaming (every execution is persisted), no industrial protocols, no edge story, per-execution overhead too high for sensor data at >100 Hz.

### 2.3 Consequence: the DataPipe dual execution model

DataPipe MUST support both paradigms natively (detailed in Section 14):

1. **Streaming flows** (Node-RED style): always-on, low-latency, datagram-at-a-time or micro-batch, backpressure-aware. Used for machine data acquisition.
2. **Triggered workflows** (n8n style): started by trigger nodes, each execution tracked with full history, retries, and inspectable per-node input/output.

A single flow MAY combine both: a streaming section can emit an event that starts a triggered workflow, and vice versa.

---

## 3. Product Vision and Scope

### 3.1 Vision statement

> DataPipe lets an engineer connect any industrial or IT data source to any destination in minutes, by dragging connectors and processors onto a canvas, wiring them together, and watching the real data flow live through every wire — from a single edge device next to a machine up to a multi-tenant cloud installation managing thousands of flows.

### 3.2 In scope

* Web-based visual flow editor (drag and drop, live inspection).
* Execution engine with streaming and trigger modes.
* Connector framework with the source/sink catalog of Section 7 and 11.
* Internal bus with a formally defined datagram model.
* Edge runtimes with central fleet management.
* Deployments: on-premise single tenant, cloud SaaS multi tenant, hybrid with edge.
* Security (RBAC, credentials, audit), observability, plugin SDK, public APIs.

### 3.3 Success criteria (measurable)

* An engineer with no prior training builds a working "OPC-UA → filter → SQL database" flow in under 15 minutes.
* Sustained throughput of at least 10,000 datagrams/second per runtime instance on commodity hardware (4 cores, 8 GB RAM) for a pass-through flow.
* End-to-end latency (source connector in → sink connector out, simple transform) under 50 ms at the 99th percentile in streaming mode.
* Edge runtime installable on a device with 1 CPU core / 512 MB RAM.
* A third-party developer creates and installs a custom connector using the SDK in under one day.

---

## 4. Users and Personas

| Persona | Description | Key needs |
|---|---|---|
| **Automation engineer** (primary) | Connects machines (SECS, OPC-UA, MQTT) to IT systems; not a full-time software developer | Visual editing, protocol depth, live data view, robust deploys |
| **Integration developer** | Builds complex transformations, custom nodes, scripts | Script nodes, SDK, git integration, APIs |
| **Data engineer** | Routes machine data into databases, lakes, and analytics buses | Batching, schema mapping, throughput, delivery guarantees |
| **Plant IT administrator** | Operates the platform on premise, manages users and edge fleet | RBAC, audit, monitoring, backup, offline installation |
| **SaaS platform operator** | Runs the multi-tenant cloud offering | Tenant isolation, quotas, billing hooks, zero-downtime upgrades |
| **Viewer / process engineer** | Looks at flows and live values, changes nothing | Read-only mode, dashboards, clear visualization |

---

## 5. System Architecture Requirements

The following are structural requirements; concrete technology choices are made in a later architecture document.

### 5.1 Component decomposition

**ARC-100 (MUST, P1)** — The system consists of at least these separable components:

1. **Editor Frontend**: browser application (SPA) providing canvas, palette, configuration panels, admin UI.
2. **Control Plane (API Server)**: authentication, flow storage, deployment orchestration, user/tenant management, fleet management.
3. **Execution Runtime**: engine that runs deployed flows. Multiple runtimes can register with one control plane.
4. **Edge Runtime**: reduced-footprint variant of the execution runtime for edge devices.
5. **Internal Bus**: transport layer for datagrams between nodes and, when configured, between runtimes.
6. **Persistence layer**: storage for flow definitions, versions, credentials (encrypted), execution history, and configuration.

**ARC-110 (MUST, P1)** — The Editor Frontend communicates with the Control Plane exclusively through documented APIs (REST/WebSocket or equivalent). No functionality may exist in the UI that is not achievable through the public API.

**ARC-120 (MUST, P1)** — Execution Runtimes MUST be able to run flows autonomously if the Control Plane is temporarily unreachable (headless operation). Deployed flows survive control-plane outages and runtime restarts.

**ARC-130 (MUST, P1)** — All components MUST be deployable as containers; on-premise installation without internet access (air-gapped) MUST be possible.

**ARC-140 (SHOULD, P2)** — Runtimes are horizontally scalable: a flow or a set of flows can be assigned to a specific runtime or runtime group; triggered workflows can be distributed across worker pools (n8n queue-mode equivalent).

**ARC-150 (MUST, P1)** — Node execution is isolated per flow such that a crashing node or flow does not take down other flows on the same runtime.

**ARC-160 (SHOULD, P2)** — Multi-tenancy: tenants are isolated at data, credential, flow, and execution level. Cross-tenant access is impossible by construction, not by filtering.

### 5.2 Communication topology

**ARC-200 (MUST, P1)** — Editor ↔ Control Plane: request/response plus a live channel (e.g. WebSocket) for deploy status, debug streams, and runtime health.

**ARC-210 (MUST, P1)** — Edge Runtime ↔ Control Plane: the edge initiates the connection (outbound only, firewall friendly), authenticates with per-device credentials, receives flow deployments, and streams health/debug data back. Store-and-forward buffering when disconnected (see EDGE-130).

**ARC-220 (MUST, P1)** — Node ↔ Node within one runtime: in-process internal bus (Section 9), zero network overhead.

**ARC-230 (SHOULD, P2)** — Node ↔ Node across runtimes: a "bus link" mechanism transparently bridges the internal bus over the network (broker-based or brokerless), so a flow can be split across edge and server.

---

## 6. The Datagram Model

The datagram is the single, mandatory message format on the internal bus. Every source connector produces datagrams; every processor consumes and produces datagrams; every sink consumes them. This replaces Node-RED's untyped `msg` object and n8n's item arrays with one formally defined envelope.

### 6.1 Datagram structure

**DGM-100 (MUST, P1)** — A datagram consists of exactly these parts:

```jsonc
{
  "header": {
    "id": "01J9XK...",              // unique, sortable ID (e.g. ULID), set at creation
    "correlationId": "01J9XK...",   // groups datagrams belonging to one logical event/request
    "causationId": "01J9XJ...",     // id of the datagram that caused this one (lineage)
    "timestamp": "2026-07-06T09:31:22.123456Z", // creation time, UTC, µs precision
    "sourceTimestamp": "2026-07-06T09:31:22.100000Z", // time at the physical source, if known
    "source": {
      "flowId": "flow-abc",
      "nodeId": "node-17",
      "runtimeId": "edge-fab2-line3",
      "connector": "opcua",
      "origin": "opc.tcp://plc1:4840/ns=2;s=Line3.Temperature" // protocol-specific address
    },
    "schemaRef": "temperature-reading@2",   // optional reference into the schema registry
    "contentType": "application/json",      // encoding of the payload
    "quality": "GOOD",                      // GOOD | UNCERTAIN | BAD | STALE (OPC-UA aligned)
    "priority": 4,                          // 0 (highest) .. 9 (lowest), default 4
    "ttl": null,                            // optional expiry (ms); expired datagrams are dropped and counted
    "tags": { "site": "fab2", "line": "3" } // free string key/value routing metadata
  },
  "payload": { },                            // the actual data (any JSON value, or binary, see DGM-120)
  "trace": [                                 // optional per-hop trace, only populated in debug mode
    { "nodeId": "node-17", "in": "...", "out": "...", "durationUs": 412 }
  ]
}
```

**DGM-110 (MUST, P1)** — `header.id`, `header.timestamp`, and `header.source` are always present and set by the runtime; nodes cannot forge them but MAY read them. All other header fields are readable and writable by processing nodes.

**DGM-120 (MUST, P1)** — Payloads support: JSON values (object, array, string, number incl. 64-bit integers and IEEE-754 doubles, boolean, null), binary blobs (with `contentType`), and typed timestamps. Binary payloads (images, raw protocol frames, files) MUST be handled by reference above a configurable size threshold (default 256 KB) so that large blobs do not travel through every node copy.

**DGM-130 (MUST, P1)** — **Batches**: a batch is an ordered list of datagrams sharing a batch header. Nodes declare whether they operate datagram-at-a-time (the engine iterates batches automatically, like n8n items) or batch-aware (windowing, aggregation, bulk insert). Batching/unbatching nodes exist as explicit work items.

**DGM-140 (MUST, P1)** — **Quality propagation**: quality is carried end to end. Processors combining multiple inputs derive the worst input quality by default; nodes MAY override this. Sinks can be configured to reject or specially route non-GOOD data.

**DGM-150 (SHOULD, P2)** — **Schema registry**: the platform hosts named, versioned schemas (JSON Schema as the normative format). A datagram MAY reference a schema; nodes and the editor use it for validation, typed mapping UIs, and autocomplete. Schema evolution rules (backward/forward compatibility checks) are enforced on schema updates.

**DGM-160 (MUST, P1)** — **Lineage**: `correlationId`/`causationId` allow reconstructing which input datagram produced which outputs across the whole flow. The debug UI (Section 13) visualizes this.

**DGM-170 (SHOULD, P2)** — Serialization on bus links (cross-runtime) uses a compact binary encoding; JSON is the human-facing representation. The wire encoding is an implementation choice but MUST preserve all datagram semantics including 64-bit integers and binary payloads.

---

## 7. Functional Requirements — Connectors (Sources)

General rules for all connectors, then the mandatory catalog.

### 7.1 General connector requirements

**CON-100 (MUST, P1)** — Every connector is a node with: a configuration panel (connection settings referencing a central credential/connection object, not inline secrets), a data selection section (Section 8), status indication on the canvas (connected / connecting / error / disabled, with tooltip detail), and defined output ports.

**CON-110 (MUST, P1)** — Connection definitions (host, port, security settings, credentials) are managed centrally and referenced by name from connector nodes, so many nodes share one connection and credentials rotate in one place.

**CON-120 (MUST, P1)** — Every connector maps received data into datagrams: payload carries the data, `sourceTimestamp` and `quality` are filled from the protocol where available, `origin` identifies the protocol-specific address (topic, node id, table, file path...).

**CON-130 (MUST, P1)** — Automatic reconnection with exponential backoff and jitter; connection state changes emit system events (visible in monitoring and usable as flow triggers).

**CON-140 (MUST, P1)** — A "test connection" button in the editor validates configuration against the live system and, where meaningful, browses the namespace (OPC-UA nodes, MQTT topic samples, DB tables, file preview).

**CON-150 (SHOULD, P2)** — Per-connector rate limiting and overload behavior (drop-oldest, drop-newest, block, sample) configurable; drops are counted and visible in metrics.

### 7.2 Industrial protocol sources

**CON-200 MQTT (MUST, P1)** — MQTT 3.1.1 and 5.0 client. Subscribe with wildcard topics; QoS 0/1/2; TLS incl. client certificates; shared subscriptions; MQTT 5 properties exposed in header tags; optional JSON/Sparkplug B payload decoding (Sparkplug: P2, including birth/death certificate handling and metric expansion into individual datagrams).

**CON-210 OPC-UA (MUST, P1)** — OPC-UA client per IEC 62541. Security policies incl. Basic256Sha256, user token and certificate auth. Modes: (a) subscription/monitored items with configurable sampling and publishing intervals, deadband filters; (b) polled reads; (c) method calls; (d) event/alarm subscription (P2); (e) history read (P2). Namespace browser in the editor for point selection. Status codes map to datagram quality.

**CON-220 SECS/GEM (MUST, P1)** — Semiconductor equipment interface: HSMS (SEMI E37) transport MUST, SECS-I serial (SEMI E4) SHOULD (P2). SECS-II (SEMI E5) message encoding/decoding with a message dictionary; GEM (SEMI E30) capabilities: establish communications, equipment status variable collection (SVID), collection event reports (CEID/RPTID configuration through the UI), alarm management, remote commands, recipe management (P2), trace data collection. The connector can act as **host**; equipment-side emulation MAY be provided for testing (P3). Raw SxFy access for non-GEM messages MUST be possible.

**CON-230 Modbus (MUST, P1)** — Modbus TCP and RTU (serial) master: read/write coils, discrete inputs, holding/input registers; polling groups with independent intervals; data type decoding (int16/32/64, float, string, bit fields, byte/word order options).

**CON-240 Siemens S7 (SHOULD, P2)** — S7 protocol (ISO-on-TCP) reads/writes for S7-300/400/1200/1500 data blocks, merkers, inputs/outputs, with symbolic addressing where available.

**CON-250 EtherNet/IP (SHOULD, P2)** — CIP explicit messaging, tag read/write for Allen-Bradley/Rockwell controllers.

**CON-260 Kafka (MUST, P1)** — Consumer with consumer groups, offset management (earliest/latest/committed), key/value deserialization (JSON, string, binary, Avro via schema registry P2), headers into tags.

**CON-270 AMQP (SHOULD, P2)** — AMQP 0-9-1 (RabbitMQ) and AMQP 1.0 consumers.

**CON-280 BACnet (MAY, P3)** — BACnet/IP object read and COV subscription for building automation.

**CON-290 Serial / TCP / UDP raw (MUST, P1)** — Raw byte stream sources with framing options (delimiter, fixed length, length prefix, timeout) feeding binary payload datagrams; a companion parser node library decodes them (Section 10).

### 7.3 IT / web sources

**CON-300 HTTP In / Webhook (MUST, P1)** — Configurable HTTP(S) endpoints (path, methods, auth: none/basic/header key/HMAC signature validation/OAuth2 (P2)); request becomes a datagram (body, headers, query); paired response node allows synchronous replies. Test URLs vs. production URLs like n8n.

**CON-310 HTTP Poller (MUST, P1)** — Periodic HTTP requests with templated URL/headers/body, pagination strategies (cursor, offset, link header), change detection (ETag/Last-Modified/hash) to emit only new data.

**CON-315 REST API Client (MUST, P1)** — Generic, full-featured REST API connector usable as source (polling/pagination per CON-310), mid-flow processor (request per datagram), and sink. Features: all HTTP methods, path/query/header/body templating from expressions, auth profiles (basic, bearer, API key, OAuth2 client credentials and authorization code (P2), mTLS client certificates), request/response content types (JSON, XML, form, multipart, binary), response parsing into datagrams with JSONPath/XPath record selection, retry with backoff, per-host rate limiting and connection pooling, cookie/session handling (P2). **OpenAPI/Swagger import (SHOULD, P2)**: upload or URL-load an OpenAPI 3.x definition and the connector generates operation pickers with typed parameter forms, so an engineer selects an endpoint like a native connector operation instead of hand-building requests.

**CON-320 WebSocket (MUST, P1)** — Client (connect out) and server (accept in) modes.

**CON-330 Schedule/Cron (MUST, P1)** — Time triggers: cron expressions, fixed intervals, calendar rules incl. time zones; emits a trigger datagram.

**CON-340 Email (SHOULD, P2)** — IMAP polling (new mail incl. attachments as binary payloads).

**CON-350 gRPC (MAY, P3)** — Server streaming / unary endpoints from `.proto` upload.

### 7.4 File sources

**CON-400 File Watcher (MUST, P1)** — Watch local/mounted directories (and SFTP/FTP(S) P2, S3-compatible object storage P1): patterns, recursive, events (created/modified/deleted), stability check (file complete), post-actions (move/rename/delete/keep + marker).

**CON-410 File Readers (MUST, P1)** — Parsing work items for: **CSV/TSV** (delimiter, quoting, encoding incl. UTF-8/Latin-1, header row handling, type inference or explicit column typing, streaming for large files, malformed row policy), **Excel** (.xlsx/.xls: sheet selection by name/index, range, header row, formula results, dates), **JSON** (single doc, JSON Lines, array streaming with JSONPath root selection), **XML** (XPath-based record splitting, namespace support, attribute/element mapping, streaming for large files), **Parquet** (SHOULD, P2), fixed-width text (SHOULD, P2). Every reader emits one datagram per record by default or a single batch, configurable.

### 7.5 Database sources

**CON-500 SQL (MUST, P1)** — Connectors for at least PostgreSQL, MySQL/MariaDB, Microsoft SQL Server, Oracle (P2), SQLite; generic JDBC/ODBC-style fallback (P2). Modes: one-shot query, periodic query with incremental column (id/timestamp watermark kept in durable node state), and query-on-datagram (lookup). Parameterized statements only (no string concatenation), streaming cursors for large result sets, row → datagram mapping with type fidelity (decimal, datetime with zone, binary).

**CON-510 Change Data Capture (SHOULD, P2)** — Log-based CDC for PostgreSQL (logical replication) and MySQL (binlog); SQL Server CDC tables (P3). Emits insert/update/delete datagrams with before/after images.

**CON-520 NoSQL (MUST, P1)** — MongoDB (find/aggregate, change streams P2), Redis (get/scan, pub/sub subscribe, streams), Cassandra (SHOULD, P2), Elasticsearch/OpenSearch query (SHOULD, P2), InfluxDB/TimescaleDB time-series query (SHOULD, P2).

**CON-530 Graph DB (SHOULD, P2)** — Neo4j (Cypher queries, parameterized; results as record datagrams); generic Gremlin endpoint (MAY, P3).

**CON-540 Vector DB (SHOULD, P2)** — Qdrant, pgvector; Pinecone/Weaviate/Milvus (P3). Operations: similarity search (vector or text+embedding-node), fetch by id, filtered search. Results carry score and payload.

### 7.6 Internal sources

**CON-600 (MUST, P1)** — **Bus In**: subscribes to named internal bus topics (Section 9), enabling flow-to-flow communication. **Flow Trigger In**: entry point of a callable sub-workflow. **Manual Inject**: editor button that emits a configurable test datagram (Node-RED inject node), incl. repeat interval option.

### 7.7 AI sources/processors (P3, MAY)

Embedding generation node, LLM prompt node, agent node with tool bindings to other flows. Kept out of MVP; the plugin SDK must not preclude them.

### 7.8 Enterprise bus / messaging connectors

Consumers for the messaging middleware commonly found in enterprise landscapes (ESB/MOM). Kafka (CON-260) and AMQP/RabbitMQ (CON-270) are specified in Section 7.2; the following complete the catalog. All follow the general rules of Section 7.1, map broker metadata (message id, correlation id, headers/properties, queue/topic) into datagram header and tags, and support acknowledgment modes aligned with guaranteed delivery (BUS-150).

**CON-700 JMS (SHOULD, P2)** — JMS 2.0 style connectivity for at least Apache ActiveMQ/Artemis and IBM MQ: queues and topics, durable and shared subscriptions, message selectors, transacted sessions, JMS header/property mapping, request/reply via temporary queues and JMSCorrelationID.

**CON-710 Azure Service Bus (SHOULD, P2)** — Queues and topics/subscriptions, peek-lock with settlement (complete/abandon/dead-letter), sessions (FIFO per session key), scheduled messages, access to the built-in dead-letter sub-queue.

**CON-720 AWS SQS / SNS (SHOULD, P2)** — SQS standard and FIFO queues (long polling, visibility timeout, batch receive/delete, message attributes), SNS subscription ingestion via SQS or HTTPS endpoint.

**CON-730 NATS / JetStream (SHOULD, P2)** — Core NATS subscribe (subjects, wildcards, queue groups) and JetStream durable consumers with acknowledgment and replay from a given sequence/time.

**CON-740 Google Pub/Sub (MAY, P3)** — Pull subscriptions with acknowledgment deadlines and ordering keys.

**CON-750 Solace PubSub+ (MAY, P3)** — SMF/JCSMP or AMQP-based queue and topic consumption.

**CON-760 MSMQ (MAY, P3)** — Read from Microsoft Message Queuing for legacy Windows integration (requires Windows runtime host).

**CON-770 ESB platform endpoints (MAY, P3)** — Documented integration patterns (not necessarily dedicated connectors) for MuleSoft, TIBCO, and SAP PI/PO via their standard protocol facades (JMS, AMQP, REST, SOAP); SOAP/WSDL client node with WSDL import generating typed operation forms (SHOULD, P2 — also serves general web-service integration).

### 7.9 SaaS/API integration catalog (SHOULD, P2 onward)

A growing catalog of application connectors (ERP/MES first: SAP OData, REST-based MES; then messaging: Slack, Teams, email send). Built entirely on the plugin SDK (Section 20) to prove its sufficiency. Breadth is explicitly secondary to industrial depth.

---

## 8. Functional Requirements — Data Selection and Mapping

The user's core interaction: for each connector, choose WHAT data to retrieve and HOW it lands in the datagram; for each processor/sink, map incoming datagram fields onto parameters.

**MAP-100 (MUST, P1)** — Every source connector offers a **selection UI** appropriate to its protocol: OPC-UA namespace tree with checkbox selection and per-node sampling settings; MQTT topic list with live topic discovery from observed traffic; SECS/GEM report builder (pick SVIDs/CEIDs from the equipment's capability set); SQL query editor with schema browser, syntax highlighting, and result preview; file readers with column/field preview from a sample file; NoSQL/graph/vector query editors with result preview.

**MAP-110 (MUST, P1)** — Every selection UI has a **preview**: "fetch sample now" shows real data (max N records) in table and raw JSON view before the flow is deployed.

**MAP-120 (MUST, P1)** — **Field mapping**: source fields map to datagram payload fields via a mapping table (source path → target path, with rename, omit, constant, and type cast). Default is pass-through. The mapping UI shows live sample values next to every field (n8n style drag-from-input-panel SHOULD, P2).

**MAP-130 (MUST, P1)** — **Expression language**: every node parameter can be a literal or an expression over the incoming datagram (`payload.*`, `header.*`, `tags.*`), flow/global context, environment variables, and a standard function library (string, math, date/time with time zones, array, object, conversion, hashing). One documented expression syntax platform-wide. Expressions are sandboxed and cannot perform I/O.

**MAP-140 (SHOULD, P2)** — Schema-aware autocomplete in expression fields, driven by the schema registry or by sampled data.

**MAP-150 (MUST, P1)** — Type casting rules are explicit and documented (string↔number, epoch↔ISO time, base64↔binary, etc.); cast failures follow the node's error policy (Section 17), never silent coercion to `null`.

---

## 9. Functional Requirements — Internal Data Bus

**BUS-100 (MUST, P1)** — Within a runtime, wires between nodes are channels of the internal bus carrying datagrams with at-most-once, in-order delivery per wire by default, and bounded queues per input port.

**BUS-110 (MUST, P1)** — **Backpressure**: when a node's input queue is full, upstream is signaled. Per wire, the overflow policy is configurable: block source (default for durable sources like Kafka/SQL), drop-oldest, drop-newest, or sample (keep every n-th). All drops are counted and exposed as metrics and canvas warnings.

**BUS-120 (MUST, P1)** — **Named bus topics**: flows publish datagrams to named topics ("Bus Out" node) and subscribe ("Bus In" node) with wildcard support and tag-based filtering — decoupled flow-to-flow communication within a runtime, and across runtimes via bus links (ARC-230).

**BUS-130 (SHOULD, P2)** — **Durable topics**: a named topic can be declared durable (persisted, replayable with consumer offsets), giving n8n-style decoupling plus recovery after restarts. Retention by time and size.

**BUS-140 (MUST, P1)** — Fan-out (one output wired to n inputs) delivers an independent copy (copy-on-write allowed as optimization); fan-in (n outputs wired to one input) interleaves in arrival order.

**BUS-150 (SHOULD, P2)** — Delivery guarantee upgrade: for flows marked "guaranteed", the engine provides at-least-once delivery from durable source to sink with acknowledgment propagation, so a runtime crash does not lose in-flight datagrams (see ERR-160 for idempotency guidance).

---

## 10. Functional Requirements — Processing Nodes (Work Items)

The built-in library of processors that alter, calculate, transform, route, and enrich datagrams. All are stream-capable unless marked otherwise.

### 10.1 Transformation

**PROC-100 Script (MUST, P1)** — Inline code node (at minimum JavaScript/TypeScript; Python SHOULD, P2) with full read/write access to datagram, batch, node state, and flow/global context; sandboxed (CPU/memory/time limits, no filesystem or network access by default; admin-grantable capabilities). Multiple output ports addressable from code. Console output appears in the debug sidebar.

**PROC-110 Set/Change (MUST, P1)** — Declarative field operations without code: set, rename, move, copy, delete fields; apply expressions; set header fields and tags.

**PROC-120 Convert (MUST, P1)** — Format conversions as dedicated nodes: JSON↔XML, CSV↔records, binary parse/serialize (configurable binary layout: offsets, types, endianness, bit fields — for CON-290 raw frames), base64, compression (gzip/zip), character encodings.

**PROC-130 Template (MUST, P1)** — Render text (strings, reports, SQL, markup) from datagrams with a logic-capable template syntax.

**PROC-140 Schema Validate (SHOULD, P2)** — Validate payload against a registry schema; route valid/invalid to separate ports.

### 10.2 Calculation and aggregation

**PROC-200 Calculator (MUST, P1)** — Derived values via expressions (unit conversion, scaling, polynomial linearization, arbitrary math incl. statistical functions).

**PROC-210 Window/Aggregate (MUST, P1)** — Tumbling, sliding, and session windows by time or count, grouped by key (expression); aggregates: min, max, avg, sum, count, first, last, stddev, percentile, collect-to-list. Watermark/lateness handling for out-of-order source timestamps (SHOULD, P2).

**PROC-220 Smooth/Filter Signal (SHOULD, P2)** — Moving average, exponential smoothing, deadband, rate-of-change limiter, debounce — typical sensor conditioning.

**PROC-230 Statistics/SPC (MAY, P3)** — Control charts values (X̄/R, Cpk) as streaming computations.

### 10.3 Routing and control flow

**PROC-300 Switch/Route (MUST, P1)** — Rule-based routing to n output ports (comparisons, regex, type checks, quality checks, tag matches, expression predicates; first-match or all-matches mode).

**PROC-310 Filter (MUST, P1)** — Pass/drop by predicate; includes "report by exception" (only forward when value changed beyond deadband/interval).

**PROC-320 Merge/Join (MUST, P1)** — Modes: concatenate streams; combine-latest by key; join two streams on key with time window and inner/left semantics; batch-merge parallel branches of one correlationId (n8n merge equivalent).

**PROC-330 Split/Batch (MUST, P1)** — Split arrays/batches into single datagrams; collect singles into batches by count/time/size; chunk with overlap (P2).

**PROC-340 Loop (MUST, P1)** — Iterate over items with loop-back wiring and guaranteed termination (max iterations); n8n "loop over items" equivalent for triggered workflows.

**PROC-350 Delay/Throttle (MUST, P1)** — Fixed/expression delay, rate limiting (n per interval, per key optional), queue with scheduled release.

**PROC-360 Wait (SHOULD, P2)** — In triggered workflows: wait for time, for an external event/webhook resume, or for human approval (web form link); execution state persists durably while waiting.

**PROC-370 Try/Catch scope (MUST, P1)** — Visual error scope: nodes inside the scope route their errors to a catch port (see Section 17).

### 10.4 Enrichment and state

**PROC-400 Lookup (MUST, P1)** — Enrich datagrams from SQL/NoSQL/HTTP/static table with in-memory cache (TTL, max entries) and cache-miss policy.

**PROC-410 State/Context (MUST, P1)** — Read/write named state: node-, flow-, and global-scoped key/value store; memory or persistent backend, pluggable (Node-RED context equivalent); atomic update operations.

**PROC-420 Deduplicate (SHOULD, P2)** — Drop duplicates by key expression within a time/count window (durable option).

**PROC-160 Sub-flow Call (MUST, P1)** — Invoke a reusable flow (with declared input/output contract) synchronously (await result) or fire-and-forget; parameters passed explicitly; recursion depth limited.

---

## 11. Functional Requirements — Sinks (Outputs)

Sinks mirror the source catalog; only sink-specific behavior is listed.

**SNK-100 General (MUST, P1)** — Every sink reports per-datagram success/failure to the engine (drives guaranteed delivery and error routing); supports batch writes where the target allows (configurable batch size/flush interval); template-driven target addressing (topic, table, path from expressions).

**SNK-110 MQTT Out (MUST, P1)** — Publish with QoS, retain, MQTT 5 properties; topic from expression; Sparkplug B publishing (P2).

**SNK-120 OPC-UA Write (MUST, P1)** — Write values and call methods; **OPC-UA Server** sink exposing flow data as an OPC-UA namespace (SHOULD, P2).

**SNK-130 SECS/GEM Host actions (MUST, P1)** — Send remote commands, ECID updates, arbitrary SxFy messages via the host connection.

**SNK-140 Modbus/S7/EtherNet-IP Write (per source priority)** — Write registers/tags with write-confirmation into datagram result.

**SNK-150 Kafka/AMQP Produce (MUST P1 / SHOULD P2)** — Key, partition, headers, serialization mirroring the consumers.

**SNK-155 Enterprise Bus Produce (SHOULD, P2)** — Producer counterparts to the Section 7.8 consumers: JMS send (queue/topic, headers/properties, request/reply), Azure Service Bus send (incl. sessions and scheduled enqueue), SQS/SNS publish (message attributes, FIFO group/dedup ids), NATS/JetStream publish (subject from expression, ack await); Google Pub/Sub, Solace, MSMQ per their source priority. Delivery confirmation feeds SNK-100 success/failure reporting.

**SNK-160 HTTP / REST Request (MUST, P1)** — Full HTTP client node (also usable mid-flow as processor), sharing the capabilities of the REST API Client (CON-315) incl. auth profiles, OpenAPI-generated operation forms (P2), retry with backoff, pagination helper, response → datagram; connection pooling and per-host rate limits. SOAP/WSDL client per CON-770 (SHOULD, P2).

**SNK-170 WebSocket Out / HTTP Response (MUST, P1)** — Push to connected WS clients; respond to CON-300 requests.

**SNK-180 File Writers (MUST, P1)** — CSV/JSON/JSON-Lines/XML/Parquet(P2)/Excel(P2) writers with append/rotate strategies (size, time, record count), atomic write (temp + rename), target: local/mounted, S3-compatible (P1), SFTP (P2).

**SNK-190 SQL (MUST, P1)** — Insert/update/upsert/delete with parameter mapping from payload; bulk insert; transaction per batch; generated-key return; arbitrary statement execution for DDL/maintenance (permission-gated).

**SNK-200 NoSQL/Time-series (MUST, P1)** — MongoDB insert/update/upsert; Redis set/publish/stream-add; InfluxDB/Timescale write with tag/field mapping (SHOULD, P2); Elasticsearch/OpenSearch index (SHOULD, P2).

**SNK-210 Graph/Vector (SHOULD, P2)** — Neo4j merge/create with parameterized Cypher; vector upsert (id, vector, payload) to CON-540 targets.

**SNK-220 Bus Out (MUST, P1)** — Publish to named internal bus topics (BUS-120), the "handoff to another databus" requirement; plus bridges to external buses via the Kafka/AMQP/MQTT sinks and the enterprise bus producers of SNK-155.

**SNK-230 Notification (SHOULD, P2)** — Email (SMTP), Slack/Teams webhooks — primarily for alerting from error flows.

---

## 12. Functional Requirements — Flow Editor UI

Design principles: **clean, calm, immediately understandable**. The n8n canvas is the visual benchmark (generous spacing, clear typography, subtle grid); Node-RED's live-data affordances are the interaction benchmark. No feature may require editing raw JSON.

### 12.1 Canvas and palette

**UI-100 (MUST, P1)** — Infinite canvas with pan (space-drag/trackpad), zoom (10–400%, ctrl+scroll, fit-to-flow), grid snap (toggleable), minimap (toggleable).

**UI-110 (MUST, P1)** — **Palette**: categorized, searchable node library (search by name, description, protocol); favorites; recently used; drag onto canvas to instantiate. Nodes not yet installed appear in search with an install action (permission-gated) (SHOULD, P2).

**UI-120 (MUST, P1)** — **Nodes on canvas** show: icon + color by category (sources green, processors blue, sinks orange, control violet — final palette per style guide), user-assignable name, status dot (running/error/disabled) with message, small live indicators: datagrams/sec and last-value snippet (toggleable per node).

**UI-130 (MUST, P1)** — **Wiring**: drag from output port to input port; auto-routed curves; valid targets highlighted while dragging; quick-insert node on a wire (drop node onto wire); wire labels showing throughput in debug mode; multi-select and move; detach/reattach wires.

**UI-140 (MUST, P1)** — **Subflows**: select nodes → "create subflow"; subflow gets its own tab, declared input/output ports, and instance parameters (typed: string, number, bool, enum, credential ref); instances update when the subflow changes, with version pinning (SHOULD, P2).

**UI-150 (MUST, P1)** — **Organization**: multiple flow tabs per project; visual groups (colored, labeled, collapsible P2); sticky note/comment nodes with markdown; per-node description shown in a side panel.

**UI-160 (MUST, P1)** — Standard editing: undo/redo (min 100 steps), copy/paste (as portable JSON, also across projects/browsers), duplicate, delete with wire healing (reconnect through), keyboard-first operation (documented shortcuts), context menus.

**UI-170 (MUST, P1)** — **Config panel**: selecting a node opens a right-hand panel (not modal) with configuration, description, and node documentation tab; required-field validation with inline errors before deploy; every parameter field toggles literal ⇄ expression (MAP-130).

**UI-180 (SHOULD, P2)** — Auto-layout assistance (align, distribute, tidy selected), and a read-only auto-generated flow documentation view (nodes, settings, wiring as a printable page).

### 12.2 Deploy and lifecycle from the editor

**UI-200 (MUST, P1)** — One-click deploy with scope choice: full, modified flows only, modified nodes only (Node-RED semantics); pre-deploy validation (broken wires, missing credentials, config errors) blocks with a navigable error list.

**UI-210 (MUST, P1)** — Per-flow and per-node enable/disable without deleting; visible disabled styling.

**UI-220 (MUST, P1)** — Deploy target selection: which runtime/edge (group) runs this flow; the canvas shows runtime assignment per flow tab; split flows over bus links visualize the boundary (P2 with ARC-230).

**UI-230 (SHOULD, P2)** — Concurrent editing safety: per-flow locking with presence indicators ("Anna is editing"); full CRDT-based co-editing is P3/MAY.

### 12.3 Look and feel

**UI-300 (MUST, P1)** — Light and dark theme; responsive down to 13" laptops; primary browsers: current Chrome, Edge, Firefox, Safari (2 latest majors).

**UI-310 (MUST, P1)** — Localization-ready UI (externalized strings); shipped languages: English (P1), German (P1), others P3.

**UI-320 (SHOULD, P2)** — Accessibility: WCAG 2.1 AA for all non-canvas UI; canvas keyboard navigation best effort with documented alternatives.

**UI-330 (MUST, P1)** — Onboarding: an in-product interactive tutorial builds a first flow (inject → transform → debug); template gallery of example flows importable per connector.

---

## 13. Functional Requirements — Live Data Inspection and Debugging

The "click on it and see the data" experience — the decisive usability feature.

**DBG-100 (MUST, P1)** — **Wire/Node inspection**: clicking any node (or wire) opens the inspector showing the most recent datagrams that passed its input(s) and output(s): live-updating list, expandable JSON tree, raw/table toggle, header + payload views, copy path / copy value. Ring buffer per node (configurable, default 50 datagrams) held runtime-side; inspection works without redeploy.

**DBG-110 (MUST, P1)** — **Debug node** (Node-RED equivalent): explicit node printing selected expressions to a global debug sidebar with filtering by flow/node, pause/clear, and payload size truncation with "load full" on demand.

**DBG-120 (MUST, P1)** — **Flow animation**: in debug mode, wires visibly pulse as datagrams pass and per-wire counters/rates display; sampled above a threshold so the UI stays responsive at high rates.

**DBG-130 (MUST, P1)** — **Design-time execution** (n8n equivalents): execute a single node with a pinned/sample input; pin captured sample data to any node output so downstream configuration shows realistic values without live sources; "run once" for triggered workflows with full per-node input/output capture.

**DBG-140 (MUST, P1)** — **Execution history** (triggered workflows): every execution recorded (status, duration, trigger info, per-node in/out data with configurable retention and size limits); browse, filter, re-run from start or from failed node with same data.

**DBG-150 (SHOULD, P2)** — **Lineage view**: from any datagram in the inspector, show its causation chain across nodes (DGM-160) as a highlighted path on the canvas.

**DBG-160 (SHOULD, P2)** — **Breakpoints** on streaming wires: pause flow at a wire (with upstream backpressure), inspect, single-step datagrams, resume — permission-gated on production runtimes.

**DBG-170 (MUST, P1)** — All inspection features are permission-controlled (payloads may be sensitive) and marked read-only for viewer roles; debug data streams to the browser are sampled/rate-limited to protect the runtime.

---

## 14. Functional Requirements — Execution Engine

**ENG-100 (MUST, P1)** — Dual execution model per Section 2.3. A flow's mode is determined by its entry nodes: streaming sources create always-on flows; trigger nodes create tracked executions. Both coexist in one project and interoperate via bus topics and sub-flow calls.

**ENG-110 (MUST, P1)** — **Streaming mode**: datagram-at-a-time / micro-batch pipeline, no per-datagram persistence, target latency per Section 3.3; ordered per wire; parallelism configurable per node (default 1 to preserve order; keyed parallelism SHOULD, P2).

**ENG-120 (MUST, P1)** — **Context/state** (PROC-410 backing): node/flow/global scopes; pluggable persistence; state survives redeploys when the node id is unchanged; state is inspectable and deletable through the editor and API.

**ENG-130 (MUST, P1)** — **Triggered mode**: each trigger starts an execution with durable status (running/success/failed/waiting/cancelled), timeout, cancellation, concurrency limits per workflow (queue excess or reject), and the history of DBG-140.

**ENG-140 (MUST, P1)** — **Hot deploy**: modified-nodes deploy restarts only affected nodes; streaming connections not affected keep running; in-flight datagrams of affected nodes are drained or rerouted per a configurable policy (drain with timeout default).

**ENG-150 (MUST, P1)** — Resource guardrails per flow: max memory, max queue sizes, script CPU/time limits; violations trigger flow-level error handling, never runtime crash (ARC-150).

**ENG-160 (MUST, P1)** — Clock/scheduler correctness: time-zone aware scheduling, DST rules, monotonic timers for intervals, catch-up policy for missed schedules (skip/run-once/run-all) configurable.

**ENG-200 (SHOULD, P2)** — **Scale-out for triggered workflows**: main/worker separation over a broker (n8n queue mode equivalent), horizontal worker scaling, sticky execution affinity where nodes hold local state.

**ENG-210 (SHOULD, P2)** — **Streaming HA**: active/standby runtime pairs with fast failover for always-on flows; guaranteed-mode flows resume from durable offsets (BUS-150).

---

## 15. Functional Requirements — Deployment Topologies and Edge

**EDGE-100 (MUST, P1)** — Supported topologies: (a) single server on premise (all components on one host); (b) server + n edge runtimes; (c) cloud SaaS multi tenant (ARC-160) + customer edge runtimes; (d) fully air-gapped.

**EDGE-110 (MUST, P1)** — Edge runtime: single small binary/container (target footprint per Section 3.3), Linux x86-64 and ARM64 (Windows SHOULD, P2), local protocol connectivity (all industrial connectors of Section 7.2 run at the edge), outbound-only management connection (ARC-210).

**EDGE-120 (MUST, P1)** — **Fleet management** in the control plane: register/enroll devices (token or certificate based), inventory with health (online, CPU, memory, flow status, versions), assign flows to devices or device groups, staged rollouts (deploy to group, canary subset first P2), remote runtime upgrade (SHOULD, P2).

**EDGE-130 (MUST, P1)** — **Offline resilience**: edge flows run autonomously without control-plane connection; datagrams destined for remote sinks/bus links buffer locally (store-and-forward with size/time-bounded durable queue) and drain on reconnect; local timestamps preserved.

**EDGE-140 (SHOULD, P2)** — Edge-local mini status UI (read-only: flow status, logs, connection state) for commissioning without the central server.

---

## 16. Functional Requirements — Projects, Versioning, Collaboration

**VCS-100 (MUST, P1)** — Flows organized in **projects** (per team/plant/purpose) containing flows, subflows, connection definitions, schema references, and environment profiles. Access control at project level (Section 18).

**VCS-110 (MUST, P1)** — Automatic version history per flow: every deploy creates an immutable version with author, timestamp, comment; visual diff (nodes added/removed/changed, config field diffs); one-click rollback (as a new version).

**VCS-120 (SHOULD, P2)** — **Git integration**: project export/sync to a git repository in a stable, diff-friendly text format (deterministic ordering, credentials excluded); pull/branch/commit from the UI; CI-friendly (flows deployable from repo via API).

**VCS-130 (MUST, P1)** — **Import/export**: flows and projects as portable JSON incl. subflows and (referenced, not embedded) connection definitions; secrets are never exported; imports remap or prompt for connections/credentials.

**VCS-140 (MUST, P1)** — **Environment profiles**: named variable sets (dev/test/prod) resolved at deploy time; the same flow deploys against different profiles without modification; missing-variable check at deploy.

**VCS-150 (SHOULD, P2)** — Deployment pipeline: promote a flow version dev → test → prod with approval gates and audit trail.

---

## 17. Functional Requirements — Error Handling and Reliability

**ERR-100 (MUST, P1)** — **Per-node error policy** (uniform on every node): on error → fail flow-item (default) | retry (count, backoff base/max, jitter) | continue with error output port | discard-and-count. Error datagrams carry the original datagram plus error object (message, code, node, stack, attempt).

**ERR-110 (MUST, P1)** — **Error ports and Try/Catch scopes** (PROC-370): route errors visually to compensation/notification logic.

**ERR-120 (MUST, P1)** — **Flow-level error flows**: a designated error handler flow per project (and override per flow) receives error datagrams from unhandled node errors — n8n Error-Trigger equivalent, also for streaming flows.

**ERR-130 (MUST, P1)** — **Dead letter**: undeliverable/expired/poison datagrams go to a per-flow dead-letter topic (durable, browsable in the UI, re-injectable after fix).

**ERR-140 (MUST, P1)** — **Stop-and-error node**: deliberately fail an execution with a structured error.

**ERR-150 (MUST, P1)** — Crash recovery: runtime restart restores all deployed flows and durable state automatically; triggered executions interrupted mid-run are marked crashed and are re-runnable; guaranteed streaming flows resume per BUS-150.

**ERR-160 (MUST, P1)** — Documentation duty: every sink documents its idempotency behavior; upsert-style operations are provided wherever the target supports them so at-least-once delivery does not create duplicates.

---

## 18. Functional Requirements — Security

**SEC-100 (MUST, P1)** — **Authentication**: local accounts (strong password policy, TOTP 2FA SHOULD P2) and SSO via OIDC (P1) / SAML (P2); LDAP/AD sync (P2); API access via scoped API keys and service accounts.

**SEC-110 (MUST, P1)** — **RBAC**: built-in roles Viewer (read flows, dashboards; no payload inspection unless granted), Operator (start/stop/re-run, no editing), Editor (edit and deploy in assigned projects), Project Admin, System Admin. Custom roles from granular permissions (SHOULD, P2). Permissions scope to projects, runtimes/edge groups, connections, and debug/payload visibility (DBG-170).

**SEC-120 (MUST, P1)** — **Credential store**: secrets encrypted at rest (envelope encryption, key rotation); write-only from the UI after creation (values never re-displayed or exported); usable only by reference; per-project sharing rules; external vault integration (P2). Runtime receives only credentials referenced by its deployed flows.

**SEC-130 (MUST, P1)** — **Transport security**: TLS everywhere (UI, APIs, runtime↔control plane); certificate management for OPC-UA/MQTT/HTTPS client identities incl. CSR generation and expiry warnings.

**SEC-140 (MUST, P1)** — **Audit log**: every security-relevant action (login, permission change, credential change, deploy, flow edit, data export, debug access) with actor, time, object, before/after where feasible; tamper-evident (append-only), exportable (syslog/SIEM SHOULD, P2).

**SEC-150 (MUST, P1)** — Script sandboxing per PROC-100/ENG-150; per-tenant network egress restrictions in SaaS (allowlists) (P2 with ARC-160).

**SEC-160 (SHOULD, P2)** — Signed plugins: the platform verifies plugin package signatures; unsigned plugins require explicit admin acceptance.

---

## 19. Functional Requirements — Observability and Administration

**OBS-100 (MUST, P1)** — **Metrics** exposed in a standard scrapeable format: per node (throughput in/out, error count, drop count, queue depth, processing time percentiles), per flow, per runtime (CPU, memory, event loop/scheduler lag), per connection (state, reconnects).

**OBS-110 (MUST, P1)** — **Built-in monitoring UI**: runtime and flow health dashboard (status, rates, top error sources), connection status board, edge fleet view (EDGE-120); no external tooling required for basic operation.

**OBS-120 (MUST, P1)** — **Structured logs** (JSON) with correlation to flow/node/execution ids; per-flow log level at runtime without redeploy; log viewer in the UI with filter/search; retention configuration.

**OBS-130 (SHOULD, P2)** — Distributed tracing spans across nodes and bus links (OpenTelemetry-compatible), linked from execution history.

**OBS-140 (MUST, P1)** — **Alerting hooks**: threshold rules on metrics (error rate, queue depth, connection down, edge offline) firing to notification sinks (SNK-230) and webhooks; alert state visible in the UI.

**OBS-150 (MUST, P1)** — **Backup/restore**: consistent export of all configuration (flows, versions, users, connections; secrets encrypted) via CLI/API; documented restore procedure; scheduled backups (SHOULD, P2).

**OBS-160 (SHOULD, P2)** — Usage/quota accounting per tenant/project (executions, datagram volume, storage) with limits and billing export hooks for SaaS.

---

## 20. Functional Requirements — Extensibility (Plugin SDK)

**SDK-100 (MUST, P1)** — A documented SDK to build custom nodes (connectors, processors, sinks) with: node manifest (id, version, category, icon, ports, config schema), configuration UI generated from the config schema (custom UI panels possible P2), lifecycle hooks (create, start, stop, config-change), datagram send/receive API, state and credential access APIs, status reporting API.

**SDK-110 (MUST, P1)** — Plugins install at runtime without platform restart (palette manager UI + API); versioned; multiple versions installable with per-flow pinning (SHOULD, P2); uninstall blocked while in use (with usage list).

**SDK-120 (MUST, P1)** — All first-party connectors of Sections 7 and 11 are built on the same SDK (no privileged internal APIs) — this is the acceptance test for SDK completeness.

**SDK-130 (SHOULD, P2)** — Test harness: run a node in isolation with scripted datagram fixtures and mock credentials; usable in CI.

**SDK-140 (SHOULD, P2)** — Private registry: organizations host an internal plugin registry; the public marketplace is P3.

**SDK-150 (MAY, P3)** — Sandboxed/out-of-process plugin execution for untrusted plugins.

---

## 21. Non-Functional Requirements

### 21.1 Performance and scalability

**NFR-100 (MUST)** — Throughput/latency/footprint targets of Section 3.3 verified by an automated benchmark suite run per release.

**NFR-110 (MUST)** — Editor performance: canvas with 500 nodes on one tab remains smooth (drag < 16 ms frame budget); projects with 10,000+ total nodes open in < 5 s.

**NFR-120 (SHOULD)** — One control plane manages ≥ 1,000 edge runtimes and ≥ 10,000 deployed flows.

**NFR-130 (MUST)** — Sustained-load stability: 7-day continuous streaming soak test without memory growth or throughput degradation is a release gate.

### 21.2 Availability and data safety

**NFR-200 (MUST)** — Single-node deployment: automatic restart and full state recovery (ERR-150) within 60 s.

**NFR-210 (SHOULD)** — HA deployment option: no single point of failure for control plane and durable bus; runtime failover per ENG-210.

**NFR-220 (MUST)** — Zero-data-loss guarantee applies exactly where "guaranteed mode" (BUS-150) is enabled and its documented prerequisites hold; all other modes document their loss windows honestly.

### 21.3 Usability and documentation

**NFR-300 (MUST)** — First-flow success criterion of Section 3.3 validated by usability tests with target-persona users before 1.0.

**NFR-310 (MUST)** — Every built-in node ships with reference documentation and at least one importable example flow, available offline in the product.

**NFR-320 (MUST)** — Administrator guide covering installation (incl. air-gapped), sizing, backup/restore, hardening, and upgrade; upgrades between adjacent minor versions require no manual data migration.

### 21.4 Compatibility and compliance

**NFR-400 (MUST)** — Browsers per UI-300; server platforms: Linux x86-64/ARM64 containers; edge per EDGE-110.

**NFR-410 (SHOULD)** — Migration aids: importer for Node-RED flow JSON and n8n workflow JSON producing a best-effort DataPipe flow with a conversion report (unmapped nodes flagged) (P3 acceptable).

**NFR-420 (MUST)** — GDPR-compatible operation: data locality configurable, payload retention limits, right-to-erasure support in execution history; audit per SEC-140.

**NFR-430 (SHOULD)** — Security development lifecycle: dependency scanning, static analysis, and a published vulnerability disclosure process; penetration test before 1.0.

---

## 22. Public APIs

**API-100 (MUST, P1)** — REST API (OpenAPI-documented) covering everything the UI can do (ARC-110): projects, flows (CRUD, deploy, versions, import/export), executions (list, inspect, re-run, cancel), runtimes/fleet, connections/credentials (write-only secrets), users/roles, plugins, metrics summary.

**API-110 (MUST, P1)** — Live APIs: WebSocket (or equivalent) streams for debug/inspection data, deploy status, runtime health, and named-bus topic subscribe/publish for external integration (permission-gated).

**API-120 (MUST, P1)** — Versioned API with deprecation policy (≥ 1 minor version notice); scoped API keys per SEC-100.

**API-130 (SHOULD, P2)** — CLI for CI/CD: login, export/import, deploy, environment-profile management, backup.

---

## 23. Release Phasing

| Phase | Content (requirement priorities) | Exit criterion |
|---|---|---|
| **MVP (0.x)** | All P1 of: architecture, datagram model, editor UI, live debugging, streaming engine, triggered engine basics; connectors: MQTT, OPC-UA, Modbus, HTTP in/out, WebSocket, schedule, file watch + CSV/Excel/JSON/XML, SQL (PostgreSQL, MySQL, MSSQL, SQLite), MongoDB, Redis, Kafka, S3 files, bus in/out; core processors; security P1; single-server + edge topology | Success criteria 1, 2, 4 of Section 3.3 met; 5 pilot flows in production at one site |
| **1.0** | SECS/GEM host (CON-220) complete; fleet management complete; error handling complete; observability P1 complete; docs per NFR-3xx; soak test NFR-130 | Production-ready declaration; pen test passed |
| **1.x** | P2 items: Sparkplug B, S7, EtherNet/IP, CDC, graph/vector DBs, enterprise bus connectors (JMS, Azure Service Bus, SQS/SNS, NATS), OpenAPI import for the REST connector, SOAP/WSDL client, durable topics + guaranteed mode, queue-mode scaling, HA, git integration, pipeline promotion, schema registry, SSO extensions, vault, tracing, private plugin registry, multi-tenant SaaS GA | Per-feature acceptance tests |
| **2.0+** | P3/MAY: AI nodes, marketplace, co-editing, BACnet/gRPC, SPC, migration importers, equipment-side SECS emulation | — |

---

## 24. Out of Scope

* A general-purpose BI/dashboarding product (basic monitoring dashboards are in scope; a Grafana replacement is not; a dashboard node kit like Node-RED Dashboard MAY come later as a plugin).
* Device management beyond the DataPipe edge runtime (no full IoT device lifecycle/PKI for third-party devices).
* Data lake storage itself — DataPipe moves and transforms data; it is not the storage system.
* MES/SCADA functionality (recipe execution logic, order management); DataPipe integrates with such systems.
* Native mobile apps (responsive read-only web views suffice).

---

## 25. Glossary

| Term | Definition |
|---|---|
| Batch | Ordered list of datagrams processed as a unit (DGM-130) |
| Bus link | Network bridge of the internal bus between two runtimes (ARC-230) |
| CDC | Change Data Capture — reading a database's change log as an event stream |
| CEID / RPTID / SVID | SECS/GEM identifiers: collection event, report, status variable |
| Connector | Node communicating with an external system (source or sink) |
| Control plane | Central server component: API, storage, orchestration, fleet management |
| Datagram | Standard message envelope on the internal bus (Section 6) |
| Dead letter | Storage for datagrams that could not be processed/delivered (ERR-130) |
| Edge runtime | Lightweight execution engine near the data source (Section 15) |
| ESB / MOM | Enterprise Service Bus / Message-Oriented Middleware — messaging backbone connecting enterprise applications (Section 7.8) |
| Flow | Directed graph of nodes and wires; unit of authoring and deployment |
| GEM | Generic Equipment Model, SEMI E30 — standard behavior on top of SECS-II |
| JMS | Java Message Service — standard API for enterprise messaging (queues, topics) |
| HSMS | High-Speed SECS Message Services, SEMI E37 — SECS over TCP/IP |
| OPC-UA | OPC Unified Architecture, IEC 62541 — industrial interoperability standard |
| Quality | Datagram fitness indicator: GOOD / UNCERTAIN / BAD / STALE (DGM-140) |
| SECS-II | SEMI E5 message content standard (streams/functions, SxFy) |
| Sparkplug B | MQTT topic/payload specification for industrial data |
| Streaming flow | Always-on flow processing datagrams continuously (ENG-110) |
| Subflow | Reusable group of nodes with declared ports and parameters (UI-140) |
| Triggered workflow | Flow started per event with tracked execution history (ENG-130) |
| Watermark | Progress marker for event-time processing of out-of-order data (PROC-210) |
| Work item | Synonym for node, emphasizing processors (Section 10) |

---

*End of specification. Review notes, open questions, and change requests should be tracked against requirement IDs.*

