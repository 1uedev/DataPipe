# DataPipe — User Guide

**Covers:** development state after Increment 6 (live debugging + first real connectors) · **Audience:** flow authors and viewers
**Status note:** DataPipe is under active development. This guide describes what works today; features from the specification that are not built yet are marked *coming soon*.

## 1. What DataPipe is

DataPipe is a web-based visual data flow platform. You build flows by dragging nodes onto a canvas, wiring them together, and deploying them to a runtime that executes them continuously. As of Increment 6 you can build real pipelines: MQTT, HTTP/webhooks, schedules, file watching, and PostgreSQL are connected, and you can watch live data flow through every node in the editor.

## 2. Signing in

Open the editor in your browser (in development: `http://localhost:5173`, served by the UI dev server). Sign in with the username and password your administrator gave you. Use the buttons in the top bar to switch language (English/Deutsch), toggle light/dark theme, and log out.

What you can do depends on your role per project: **Viewer** (look, don't touch — includes no live payload inspection), **Operator** (operate flows and inspect live data, no editing), **Editor** (create, edit, deploy flows), **Project Admin** (also manage the project), **System Admin** (everything, all projects).

## 3. Projects, flows, and connections

After login you land on the **Projects** page. Inside a project you see its **flows** and its **connections**.

A **connection** is a named, reusable definition of an external system (an MQTT broker, a Postgres database): host, port, options. Connector nodes reference a connection by id instead of embedding addresses, so many nodes share one definition and credentials rotate in one place — a rotated credential is picked up by running connectors on their next reconnect, without redeploy. The project page's Connections section lets you create, list, delete, and **test** connections; "Test connection" performs a real connect attempt (currently for MQTT and Postgres) and shows you the actual error if it fails. Secrets never appear in any response. Current limits: connection config is a raw JSON field, and attaching credentials is done via the REST API (*UI coming soon*).

## 4. The flow editor

### 4.1 Canvas

The canvas is infinite: pan by dragging empty space, zoom with the mouse wheel or trackpad. Grid snapping and the minimap can be toggled from the toolbar.

### 4.2 Palette and available nodes

The palette on the left lists all node types, grouped by category and color coded (sources green, processors blue, sinks orange). It is searchable, tracks recently used types, and supports favorites. Drag a type onto the canvas to create a node.

| Node | Category | Purpose |
|---|---|---|
| **Inject** | Source | Emits a configurable test datagram, once or on an interval |
| **Schedule** | Source | Time trigger: fixed interval or cron expression |
| **MQTT In** | Source | Subscribes to broker topics (wildcards, QoS); uses an MQTT connection |
| **HTTP In** | Source | Exposes a webhook endpoint; each request becomes a datagram |
| **File Watcher** | Source | Watches directories (recursive) and parses CSV/TSV (delimiter, header, encoding), JSON, and JSON Lines; post-actions: keep/marker/move/rename/delete |
| **SQL Source** | Source | PostgreSQL queries: one-shot, periodic, or incremental with a watermark column |
| **Bus In** | Source | Subscribes to named internal bus topics (MQTT-style `+`/`#` wildcards, tag filters) |
| **Set** | Processor | Sets/changes payload fields declaratively |
| **HTTP Request** | Processor/Sink | Generic REST client: request per datagram, response merged into the flow |
| **Debug Log** | Sink | Pushes selected values to the debug sidebar (optionally also the runtime console) |
| **MQTT Out** | Sink | Publishes with QoS/retain; topic can be templated |
| **HTTP Response** | Sink | Replies to the exact HTTP In request that produced the datagram |
| **SQL Sink** | Sink | Postgres insert/upsert/update/delete/exec, transactional per batch, RETURNING merged back |
| **Bus Out** | Sink | Publishes to named internal bus topics — flow-to-flow handoff |

### 4.3 Wiring, configuring, editing

Drag from an output port to an input port to wire nodes; valid targets highlight, wires can be detached and reattached, deleting a node heals its wires. Selecting a node opens the right-hand **config panel** with a schema-generated form (required fields show inline errors) and a Description tab for notes. Nodes and whole flows can be disabled without deleting.

Shortcuts: undo/redo Cmd+Z / Shift+Cmd+Z (Ctrl on Windows/Linux, 100 steps), copy/paste Cmd+C/V, duplicate Cmd+D, delete Delete/Backspace. Copy/paste works across flows.

### 4.4 Saving and deploying

**Save** stores your draft; **Deploy** validates and pushes it to the connected runtime, which hot-swaps only changed nodes — untouched nodes keep running. Inline errors distinguish *invalid flow* (fix the listed problems) from *no runtime connected* (ask your administrator). Every deploy creates an immutable version; history browsing and rollback exist in the REST API (*editor UI coming soon*).

## 5. Watching your data live

This is DataPipe's Node-RED-style core experience, available since Increment 5. Live inspection requires the Operator role or higher.

**Inspector (per node)**: select a node and open its **Inspect** tab. You see the most recent datagrams through that node (ring buffer, 50 entries, kept runtime-side so inspection needs no redeploy) as an expandable JSON tree or raw view, with click-to-copy for any path or value; large payloads are truncated with "load full" on demand.

**Fetch sample now** (source nodes): runs the source in isolation for up to 10 records / 10 seconds and shows you real data before you deploy — use it to check a query or topic before wiring the rest.

**Run once + pinning** (processor nodes): execute a single node against a JSON payload you type in, and **pin** captured outputs to the node for reference while configuring. (Pinned data feeding downstream config forms is *coming soon* with the expression work.)

**Debug sidebar**: the bottom-docked sidebar collects everything Debug Log nodes push, filterable by node, with pause and clear.

**Live wires**: while you watch, wires pulse as datagrams pass and show delivered/dropped counters. At high rates the display is sampled to keep the UI responsive, but the counters remain exact.

## 6. A realistic example

`examples/mqtt-sensor-to-postgres.flow.json` in the repository is an importable demo: MQTT In (sensor topic) → Set (shape the record) → SQL Sink (insert into Postgres). Create an MQTT and a Postgres connection in your project, test both, load the flow, point the nodes at your connections, use Fetch sample / the Inspector to see real readings, then deploy.

## 7. Current limitations (honest list)

* Processor library is minimal: Set and HTTP Request only. Script node, calculator, window/aggregate, switch/filter/merge/split, and the full expression function library are next (Increment 7).
* Streaming flows only — triggered workflows with execution history and re-run come with Increment 8.
* No flow-level error flows or dead-letter view yet; per-node error policies (retry, error port, discard) exist in the engine.
* No subflows, visual groups, or sticky notes yet.
* Deploys go to all connected runtimes; per-runtime/edge targeting comes with fleet management (Increment 9).
* Industrial connectors beyond MQTT (OPC-UA, Modbus, Kafka, SECS/GEM) are scheduled for Increments 10–11.
