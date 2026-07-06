# DataPipe — User Guide

**Covers:** development state after Increment 4 (editor MVP) · **Audience:** flow authors and viewers
**Status note:** DataPipe is under active development. This guide describes what works today; sections marked *coming soon* point to features from the specification that are not built yet.

## 1. What DataPipe is

DataPipe is a web-based visual data flow platform. You build flows by dragging nodes onto a canvas, wiring them together, and deploying them to a runtime that executes them continuously. Today the built-in node set is the minimal pipeline used to prove the engine (`inject → set → debug-log`); industrial connectors (MQTT, OPC-UA, SECS/GEM, databases) arrive in the next increments.

## 2. Signing in

Open the editor in your browser (in development: `http://localhost:5173`, served by the UI dev server). Sign in with the username and password your administrator gave you. Use the buttons in the top bar to switch language (English/Deutsch), toggle light/dark theme, and log out. Your session persists until you log out or it expires.

What you can do depends on your role per project: **Viewer** (look, don't touch), **Operator** (operate flows, no editing), **Editor** (create, edit, deploy flows), **Project Admin** (also manage the project), **System Admin** (everything, all projects).

## 3. Projects and flows

After login you land on the **Projects** page. Create a project with the create button (name + description), or open an existing one. Inside a project you see its **flows**; create a new flow the same way. Opening a flow takes you to the editor.

## 4. The flow editor

### 4.1 Canvas

The canvas is infinite: pan by dragging empty space, zoom with the mouse wheel or trackpad. Grid snapping and the minimap can be toggled from the toolbar.

### 4.2 Palette and adding nodes

The palette on the left lists all node types known to the connected control plane, grouped by category and color coded (sources green, processors blue, sinks orange). It is searchable, tracks your **recently used** types, and lets you star **favorites**. Drag a type onto the canvas to create a node.

Currently available node types:

| Node | Category | Purpose |
|---|---|---|
| **Inject** | Source | Emits a configurable test datagram, once or on an interval — your data source until real connectors land |
| **Set** | Processor | Sets/changes payload fields declaratively |
| **Debug Log** | Sink | Writes received datagrams to the runtime log |

### 4.3 Wiring

Drag from a node's output port to another node's input port to create a wire. Valid drop targets highlight while you drag. Wires can be detached from a port and reattached elsewhere. Deleting a node heals its wires where possible (upstream reconnects to downstream).

### 4.4 Configuring nodes

Select a node and the **config panel** opens on the right (it never blocks the canvas). The form is generated from the node type's schema, so every node presents consistent controls; required fields show inline errors until filled. The **Description** tab holds your notes for the node. Rename a node via its name field; disable a node (or the whole flow) without deleting it — disabled elements render dimmed.

### 4.5 Editing commands and shortcuts

| Action | Shortcut (macOS / Windows-Linux) |
|---|---|
| Undo / Redo (up to 100 steps) | Cmd+Z / Ctrl+Z · Shift+Cmd+Z / Shift+Ctrl+Z |
| Copy / Paste selection | Cmd+C / Ctrl+C · Cmd+V / Ctrl+V |
| Duplicate selection | Cmd+D / Ctrl+D |
| Delete selection | Delete or Backspace |

Copy/paste works within and across flows.

### 4.6 Saving and deploying

**Save** stores your draft on the server. **Deploy** validates the flow and pushes it to the connected runtime, where it starts (or hot-swaps) immediately. Two errors you may see inline: *invalid flow* (fix the listed validation problems) and *no runtime connected* (ask your administrator — the runtime process isn't registered with the control plane).

Every deploy creates an immutable version. Version history and one-click rollback exist in the REST API today; the editor UI for browsing versions is *coming soon*.

### 4.7 Seeing your flow run

Today, deployed flows prove themselves through the **Debug Log** node: its output appears in the runtime's log (your admin can show you, e.g. `docker compose logs runtime`). The in-editor live inspection — clicking a node to watch real datagrams flow, debug sidebar, wire animation — is the very next increment (*coming soon*).

## 5. Current limitations (honest list)

* Only the three proof nodes exist; no external connectors yet (next increments).
* No live data view in the editor yet (Increment 5).
* No subflows, sticky notes, or visual groups yet.
* Deploy always targets the default runtime; choosing a specific runtime/edge comes with fleet management.
* Expression fields exist in the config forms (literal ⇄ expression toggle), but the full expression function library arrives with the processor library increment.
