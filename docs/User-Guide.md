# DataPipe — User Guide

**Covers:** development state after Increment 9 (edge runtime + fleet) · **Audience:** flow authors and viewers
**Status note:** DataPipe is under active development. This guide describes what works today; features from the specification that are not built yet are marked *coming soon*.

## 1. What DataPipe is

DataPipe is a web-based visual data flow platform. You build flows by dragging nodes onto a canvas, wiring them together, and deploying them to a runtime. A flow's **mode** is determined by its entry nodes: flows starting with a plain source (MQTT In, Schedule, ...) run continuously (**streaming**); flows starting with a trigger node (HTTP In, Error Trigger) start one tracked, inspectable **execution** per event. Both are supported, and a rich processor library (script sandbox, calculator, window/aggregate, routing, error handling) sits between connectors either way.

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
| **File Watcher** | Source | Watches directories (recursive) and parses CSV/TSV (delimiter, header, encoding), JSON, and JSON Lines; post-actions: keep/marker/move/rename/delete |
| **SQL Source** | Source | PostgreSQL queries: one-shot, periodic, or incremental with a watermark column |
| **Bus In** | Source | Subscribes to named internal bus topics (MQTT-style `+`/`#` wildcards, tag filters) |
| **HTTP In** | Trigger | Exposes a webhook endpoint; each request starts a tracked **execution** (see §5.1) |
| **Error Trigger** | Trigger | Entry point of a flow-level error-handler flow (see §5.3); each unhandled error elsewhere starts a tracked execution here |
| **Set** | Processor | Sets/changes payload fields declaratively |
| **Script** | Processor | Sandboxed JavaScript with full read/write access to the datagram and node/flow/global state |
| **Convert** | Processor | JSON ⇄ XML ⇄ CSV ⇄ binary/base64 conversions |
| **Template** | Processor | Renders text (strings, SQL, reports) from `{{ expr }}` placeholders |
| **Calculator** | Processor | Evaluates a numeric/string expression against the payload |
| **Window/Aggregate** | Processor | Tumbling/sliding/session windows with sum/mean/min/max/etc. |
| **Switch** | Processor | Routes to one of several dynamic output ports by rule |
| **Filter** | Processor | Passes or drops based on a condition or deadband |
| **Merge/Join** | Processor | Combines two named input branches (`a`/`b`) |
| **Split/Batch** | Processor | Splits an array into items, or batches items by size/count/interval |
| **Loop** | Processor | Iterates a collection through a designated loop-back wire |
| **Delay/Throttle** | Processor | Delays or rate-limits datagrams |
| **Try/Catch** | Processor | Wraps another node type, routing its errors to a `catch` port |
| **Lookup** | Processor | Looks up a value (static table or HTTP call) with a cache |
| **State** | Processor | Reads/writes node/flow/global context store values |
| **Stop and Error** | Processor | Deliberately fails the execution with a structured message/code (see §5.2) |
| **HTTP Request** | Processor/Sink | Generic REST client: request per datagram, response merged into the flow |
| **Debug Log** | Sink | Pushes selected values to the debug sidebar (optionally also the runtime console) |
| **MQTT Out** | Sink | Publishes with QoS/retain; topic can be templated |
| **HTTP Response** | Sink | Replies to the exact HTTP In request that produced the datagram |
| **SQL Sink** | Sink | Postgres insert/upsert/update/delete/exec, transactional per batch, RETURNING merged back |
| **Bus Out** | Sink | Publishes to named internal bus topics — flow-to-flow handoff |

Any string config field can be an expression: `={{ payload.value * 2 }}` (whole value) or `"line-{{tags.line}}"` (mixed template) — see the [Expression Language](Expression-Language.md) reference.

### 4.3 Wiring, configuring, editing

Drag from an output port to an input port to wire nodes; valid targets highlight, wires can be detached and reattached, deleting a node heals its wires. Selecting a node opens the right-hand **config panel** with a schema-generated form (required fields show inline errors) and a Description tab for notes. Nodes and whole flows can be disabled without deleting.

Shortcuts: undo/redo Cmd+Z / Shift+Cmd+Z (Ctrl on Windows/Linux, 100 steps), copy/paste Cmd+C/V, duplicate Cmd+D, delete Delete/Backspace. Copy/paste works across flows.

### 4.4 Saving and deploying

**Save** stores your draft; **Deploy** validates and pushes it to the connected runtime, which hot-swaps only changed nodes — untouched nodes keep running. Inline errors distinguish *invalid flow* (fix the listed problems) from *no runtime connected* (ask your administrator). Every deploy creates an immutable version; history browsing and rollback exist in the REST API (*editor UI coming soon*).

The **deploy target** dropdown in the header (next to the deployed-version label) picks which runtime group this flow goes to — "Any runtime" (the default) broadcasts to every connected runtime, exactly like before Increment 9; picking a named group restricts the deploy to only the runtimes your administrator has assigned to that group (fleet management, §8).

## 5. Watching your data live

This is DataPipe's Node-RED-style core experience, available since Increment 5. Live inspection requires the Operator role or higher.

**Inspector (per node)**: select a node and open its **Inspect** tab. You see the most recent datagrams through that node (ring buffer, 50 entries, kept runtime-side so inspection needs no redeploy) as an expandable JSON tree or raw view, with click-to-copy for any path or value; large payloads are truncated with "load full" on demand.

**Fetch sample now** (source nodes): runs the source in isolation for up to 10 records / 10 seconds and shows you real data before you deploy — use it to check a query or topic before wiring the rest.

**Run once + pinning** (processor nodes): execute a single node against a JSON payload you type in, and **pin** captured outputs to the node for reference while configuring. (Pinned data feeding downstream config forms is *coming soon* with the expression work.)

**Debug sidebar**: the bottom-docked sidebar collects everything Debug Log nodes push, filterable by node, with pause and clear.

**Live wires**: while you watch, wires pulse as datagrams pass and show delivered/dropped counters. At high rates the display is sampled to keep the UI responsive, but the counters remain exact.

## 6. Triggered workflows

A flow that starts with a **trigger** node (currently HTTP In or Error Trigger) is a **triggered** flow: every trigger fire starts one independently tracked **execution**, browsable after the fact — this is the n8n-style half of DataPipe, alongside streaming.

### 6.1 Execution history

Open a triggered flow and click **Executions** in its header to see every run: status (running/waiting/success/failed/cancelled/crashed), trigger kind, duration, and — for a failed one — the error reason. Click into an execution for its full per-node trace: every node it passed through, with the exact input and output(s) it produced, and the structured error object (message/code/stack) for whichever node failed. Requires the Viewer role or higher (same payload-visibility rule as the live inspector).

### 6.2 Re-running after a fix

From an execution's detail view: **Re-run from start** replays the trigger's own recorded output through the flow again; **Re-run from this node** (shown on the node that failed) replays only *that* node's recorded input, so everything upstream of it is skipped. Either starts a brand-new execution (visible in the list, linked back via "re-run of ..."), so you can fix a bug (edit the node, deploy) and confirm the exact request that used to fail now succeeds — without needing to reproduce it externally. Requires the Operator role or higher.

**Cancel** stops tracking a still-running or queued execution. It does not forcibly interrupt whatever node is currently processing (a documented limitation) — it marks the execution cancelled and frees its concurrency slot for the next one.

### 6.3 Concurrency, timeouts, and dead letters

A triggered flow's Settings can cap how many executions run at once (`maxConcurrency`) and choose what happens once that cap is hit: **queue** (new triggers wait for a slot, the default) or **reject** (HTTP In answers with 429 immediately). `executionTimeoutMs` marks a runaway execution failed after that many milliseconds and frees its slot, without forcibly stopping the node still running underneath it.

A datagram a node couldn't deliver — its error policy resolved to "fail" or "discard" after retries, or its TTL expired before a node got to it — is captured as a **dead letter** instead of silently vanishing. Click **Dead Letters** in a flow's header (available for any flow, streaming or triggered) to browse them and **re-inject** one back into the node that dropped it, once you've fixed the underlying issue, or **delete** it if it's no longer relevant.

### 6.4 Flow-level error handling

Instead of (or alongside) a per-node error port, you can designate a whole flow as the error handler for another one: build a flow starting with an **Error Trigger** node configured with the target flow's id (or `*` for every flow in the project without its own override), and it receives one execution per unhandled node error elsewhere — the same `{original, error}` shape ERR-100's error port produces. Set it as a flow's `settings.errorFlow` or as your project's default error flow (project settings). *Current limitation:* this only delivers when both flows are deployed to the same runtime process (today's control plane doesn't yet support routing one runtime's error to a handler flow running elsewhere).

## 7. A realistic example

`examples/mqtt-sensor-to-postgres.flow.json` is a streaming demo: MQTT In (sensor topic) → Set (shape the record) → SQL Sink (insert into Postgres). `examples/webhook-divide-triggered.flow.json` is a triggered demo showing the whole story above: HTTP In → Script (divides two numbers from the request, deliberately throws on division by zero) → HTTP Response — POST a zero divisor, watch the failed execution and its dead letter appear, fix the script, redeploy, and re-run the failed request from the Executions view to confirm it now succeeds. `examples/edge-sensor-storeforward.flow.json` is an edge demo: MQTT In → HTTP Request to a central API, with `onError: "storeForward"` on the request node and `runtimeAssignment.group: "edge-fab2"` — see §8.

## 8. Fleet management and edge runtimes

Click **Fleet** in the top bar to see every runtime that has ever registered with the control plane: its kind (server/edge), version, live CPU/memory, how many flows it currently has deployed, and whether it's **enrolled** (registered with a per-device credential rather than the open walking-skeleton path). Only a System Admin can manage groups, issue enrollment tokens, or reassign a runtime's group — everyone else sees the inventory read-only.

### 8.1 Runtime groups and deploy targeting

A **runtime group** is a named subset of your fleet (e.g. `edge-fab2` for every device on one production line). Create one from the Fleet page, then use the flow editor's deploy-target dropdown (§4.4) to send a flow only to runtimes in that group — the rest of your fleet never receives it. Leaving the target as "Any runtime" keeps the old behavior (every connected runtime gets it).

### 8.2 Enrolling an edge device

From the Fleet page, **Issue token** creates a one-time-shown enrollment token, optionally pre-assigned to a group. Configure the edge device's runtime process with `RUNTIME_ENROLL_TOKEN=<token>` (alongside a stable `RUNTIME_ID` so it keeps its identity across restarts) and start it — it shows up in the Fleet page as enrolled and grouped as soon as it registers. Once a device has enrolled with a token, it must keep presenting that same token on every future registration; **Revoke** on a token blocks it from being used again but does not un-enroll a device that already used it.

### 8.3 Surviving a network outage (store-and-forward)

A node writing to something outside the local network — an MQTT/SQL sink or HTTP request pointed at a central system — can be configured with `errorPolicy.onError: "storeForward"` (currently a flow-JSON-only setting; a config-panel UI for it is *coming soon*) instead of the usual fail/retry/discard/error-port choices. When that destination is unreachable, datagrams are queued to local disk instead of being dropped, and delivered in order, automatically, as soon as the destination comes back — including surviving the runtime process itself restarting in the meantime. `errorPolicy.storeForward.maxSizeMb`/`maxAgeSec` bound how much can be queued before the oldest entries are dropped.

## 9. Current limitations (honest list)

* No subflows, visual groups, or sticky notes yet.
* Industrial connectors beyond MQTT (OPC-UA, Modbus, Kafka, SECS/GEM) are scheduled for Increments 10–11.
* Only HTTP In and Error Trigger are trigger nodes today; a "Cron Trigger" (one tracked execution per schedule tick, distinct from the always-on Schedule source) is a natural future addition.
* Cancelling a running execution, or an execution hitting its timeout, does not forcibly stop the node goroutine still processing underneath it — it stops being tracked and its concurrency slot frees, but very long-running node work keeps running to completion.
* The Executions and Dead Letters views are per-flow only; there is no project-wide "everything that failed today" view yet.
* `errorPolicy.onError: "storeForward"` (§8.3) has no config-panel UI yet — set it by editing the flow JSON directly.
* A runtime group can only ever run one deployed flow at a time per member runtime, same as an ungrouped one — group targeting controls *which* runtimes a flow reaches, not how many different flows one runtime can run simultaneously.
* There is no way to fully remove a retired edge device from the Fleet inventory yet, only revoke its enrollment token.
