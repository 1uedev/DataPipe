# DataPipe — Flow Definition File Format (v1)

**Status:** Draft · **Basis:** Requirements spec Sections 6, 9, 12, 16 (VCS-120/130: deterministic, diff-friendly, secret-free)

This is the core contract between editor, control plane, and runtime. Implement and test this first — everything else consumes it.

## 1. Design rules

1. Pure JSON, UTF-8. One file per flow (`*.flow.json`); a project is a directory (see §6).
2. **Deterministic serialization**: keys sorted, arrays ordered by `id`, 2-space indent → clean git diffs (VCS-120).
3. **No secrets ever** (SEC-120). Flows reference connection ids; connections reference credential ids; credentials live only in the control plane.
4. Forward compatibility: readers ignore unknown fields; `formatVersion` gates breaking changes.
5. Position/visual data is separated from logic (`layout` vs `graph`) so cosmetic moves don't obscure logic diffs.

## 2. Flow file

```jsonc
{
  "formatVersion": 1,
  "kind": "flow",                      // flow | subflow
  "id": "flow_9f2c1a",                 // stable, unique in project, [a-z0-9_]
  "name": "Line 3 temperature to DB",
  "description": "OPC-UA → deadband filter → TimescaleDB",
  "mode": "streaming",                 // streaming | triggered  (ENG-100; derived, stored for validation)
  "disabled": false,
  "runtimeAssignment": { "group": "edge-fab2" },   // UI-220; null = default runtime

  "settings": {
    "errorFlow": "flow_errhandler",    // ERR-120 override, optional
    "guaranteedDelivery": false,       // BUS-150
    "maxConcurrency": null,            // triggered mode only (ENG-130)
    "executionTimeoutMs": null
  },

  "env": [                             // flow-level variables, overridable by environment profiles (VCS-140)
    { "name": "PLC_ENDPOINT", "type": "string", "default": "opc.tcp://plc1:4840" }
  ],

  "graph": {
    "nodes": [
      {
        "id": "n1",
        "type": "opcua-in",            // node type id from palette/SDK manifest
        "typeVersion": 1,              // per-node schema version for migrations
        "name": "PLC Line 3",
        "disabled": false,
        "connection": "conn_plc1",     // reference into connections registry (CON-110)
        "config": {                    // validated against the node type's JSON Schema
          "mode": "subscribe",
          "items": [
            { "nodeId": "ns=2;s=Line3.Temperature", "samplingMs": 250, "deadband": 0.1 }
          ]
        },
        "errorPolicy": {               // ERR-100, uniform on every node
          "onError": "errorPort",      // fail | retry | errorPort | discard
          "retry": { "max": 3, "backoffMs": 1000, "maxBackoffMs": 30000, "jitter": true }
        },
        "overflow": "block"            // BUS-110 per-input policy: block | dropOldest | dropNewest | sample:n
      },
      {
        "id": "n2",
        "type": "filter",
        "typeVersion": 1,
        "name": "Report by exception",
        "config": { "mode": "deadband", "value": "={{payload.value}}", "deadband": 0.5 }
      },
      {
        "id": "n3",
        "type": "sql-out",
        "typeVersion": 1,
        "name": "Timescale insert",
        "connection": "conn_tsdb",
        "config": {
          "operation": "insert",
          "table": "readings",
          "columns": { "ts": "={{header.sourceTimestamp}}", "value": "={{payload.value}}", "line": "3" },
          "batch": { "size": 500, "flushMs": 1000 }
        }
      }
    ],
    "wires": [
      { "id": "w1", "from": { "node": "n1", "port": "out" }, "to": { "node": "n2", "port": "in" } },
      { "id": "w2", "from": { "node": "n2", "port": "pass" }, "to": { "node": "n3", "port": "in" } }
    ]
  },

  "layout": {
    "nodes": { "n1": { "x": 120, "y": 200 }, "n2": { "x": 420, "y": 200 }, "n3": { "x": 720, "y": 200 } },
    "groups": [ { "id": "g1", "label": "Acquisition", "nodes": ["n1"], "color": "green" } ],
    "notes": [ { "id": "c1", "x": 100, "y": 60, "md": "Deadband agreed with process eng., 2026-06." } ]
  }
}
```

### Expressions

Any string config value starting with `={{` and ending with `}}` is an expression (MAP-130); everything else is a literal. Expression context: `payload`, `header`, `tags`, `env`, `flow` (flow context), `global`, plus the standard function library. Mixed templates: `"line-{{tags.line}}"` inside `={{ }}`-marked strings.

### Ports

Port names are defined by the node type manifest (e.g. `filter`: `pass`/`drop`; `switch`: dynamic `out0..outN` + `default`; every node implicitly has `error` when `errorPolicy.onError == "errorPort"`).

## 3. Subflow file (`kind: "subflow"`)

Adds to the flow file:

```jsonc
{
  "interface": {
    "inputs":  [ { "port": "in",  "description": "raw reading" } ],
    "outputs": [ { "port": "out", "description": "normalized reading" } ],
    "params": [                         // instance parameters (UI-140)
      { "name": "scaleFactor", "type": "number", "default": 1.0 },
      { "name": "targetConn", "type": "connection", "connectionType": "sql" }
    ]
  }
}
```

Instances appear in a parent flow as `"type": "subflow:flow_normalize"` with `"params": { ... }` in config. Recursion is rejected at validation (PROC-160).

## 4. Connections registry (`connections.json`, per project)

```jsonc
{
  "formatVersion": 1,
  "connections": [
    {
      "id": "conn_plc1",
      "type": "opcua",
      "name": "PLC Line 3",
      "config": { "endpoint": "={{env.PLC_ENDPOINT}}", "securityPolicy": "Basic256Sha256", "securityMode": "SignAndEncrypt" },
      "credentialRef": "cred_plc1_user"   // resolved by control plane only; never exported with values
    }
  ]
}
```

## 5. Environment profiles (`profiles/<name>.json`) — VCS-140

```jsonc
{ "formatVersion": 1, "name": "prod", "values": { "PLC_ENDPOINT": "opc.tcp://10.1.3.15:4840" } }
```

Deploy = flow + profile; missing variables fail validation before deploy (UI-200).

## 6. Project directory layout (git-syncable, VCS-120)

```
my-project/
├── project.json          # id, name, description, defaults
├── connections.json      # credential refs only, no secrets
├── flows/
│   ├── line3-temp.flow.json
│   └── error-handler.flow.json
├── subflows/
│   └── normalize.flow.json
├── profiles/
│   ├── dev.json
│   └── prod.json
└── schemas/              # schema registry entries used by this project (DGM-150)
    └── temperature-reading.v2.schema.json
```

## 7. Validation rules (enforced by control plane and CLI)

1. All wire endpoints reference existing nodes/ports; port direction respected; no wire into a source-only port.
2. Every `connection` ref resolves; connection `type` matches what the node type declares.
3. `config` validates against the node type's JSON Schema for its `typeVersion`; unknown node types block deploy (with plugin-install hint, UI-110).
4. Streaming flows must contain ≥ 1 source node; triggered flows must start with a trigger node; mixed entry types are rejected (ENG-100).
5. Loops only through the loop node's designated loop port (PROC-340); other cycles are rejected.
6. IDs unique per file; file must round-trip byte-identically through the canonical serializer.

## 8. Versioning and migration

* `formatVersion` bumps only for breaking envelope changes; per-node `typeVersion` migrations are supplied by node authors (SDK manifest) and run at import/deploy.
* The control plane stores every deployed version immutably (VCS-110); export always emits the canonical format.
