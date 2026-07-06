package flow

import (
	"bytes"
	"encoding/json"
	"testing"
)

// exampleFlowJSON is the Flow-File-Format.md §2 sample, deliberately kept in
// its "human reading order" (not alphabetical) to prove the canonical
// serializer imposes its own order regardless of input.
const exampleFlowJSON = `{
  "formatVersion": 1,
  "kind": "flow",
  "id": "flow_9f2c1a",
  "name": "Line 3 temperature to DB",
  "description": "OPC-UA -> deadband filter -> TimescaleDB",
  "mode": "streaming",
  "disabled": false,
  "runtimeAssignment": { "group": "edge-fab2" },
  "settings": {
    "errorFlow": "flow_errhandler",
    "guaranteedDelivery": false,
    "maxConcurrency": null,
    "executionTimeoutMs": null
  },
  "env": [
    { "name": "PLC_ENDPOINT", "type": "string", "default": "opc.tcp://plc1:4840" }
  ],
  "graph": {
    "nodes": [
      {
        "id": "n1",
        "type": "opcua-in",
        "typeVersion": 1,
        "name": "PLC Line 3",
        "connection": "conn_plc1",
        "config": {
          "mode": "subscribe",
          "items": [ { "nodeId": "ns=2;s=Line3.Temperature", "samplingMs": 250, "deadband": 0.1 } ]
        },
        "errorPolicy": {
          "onError": "errorPort",
          "retry": { "max": 3, "backoffMs": 1000, "maxBackoffMs": 30000, "jitter": true }
        },
        "overflow": "block"
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
}`

func TestFLOWFILE_ParseExampleFromSpec(t *testing.T) {
	f, err := Parse([]byte(exampleFlowJSON))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.FormatVersion != 1 {
		t.Errorf("FormatVersion = %d, want 1", f.FormatVersion)
	}
	if f.Kind != KindFlow {
		t.Errorf("Kind = %q, want %q", f.Kind, KindFlow)
	}
	if f.ID != "flow_9f2c1a" {
		t.Errorf("ID = %q", f.ID)
	}
	if f.RuntimeAssignment == nil || f.RuntimeAssignment.Group != "edge-fab2" {
		t.Errorf("RuntimeAssignment = %+v", f.RuntimeAssignment)
	}
	if len(f.Graph.Nodes) != 3 || len(f.Graph.Wires) != 2 {
		t.Fatalf("got %d nodes, %d wires; want 3, 2", len(f.Graph.Nodes), len(f.Graph.Wires))
	}
	if f.Graph.Nodes[0].ErrorPolicy == nil || f.Graph.Nodes[0].ErrorPolicy.OnError != "errorPort" {
		t.Errorf("n1 errorPolicy = %+v", f.Graph.Nodes[0].ErrorPolicy)
	}
	if f.Graph.Nodes[0].ErrorPolicy.Retry.Max != 3 {
		t.Errorf("n1 retry.max = %d, want 3", f.Graph.Nodes[0].ErrorPolicy.Retry.Max)
	}
	if f.Layout == nil || f.Layout.Nodes["n1"].X != 120 {
		t.Errorf("Layout.Nodes[n1] = %+v", f.Layout.Nodes["n1"])
	}
}

func TestFLOWFILE_CanonicalRoundTripIsByteIdentical(t *testing.T) {
	f, err := Parse([]byte(exampleFlowJSON))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}

	canonical1, err := f.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical (1st): %v", err)
	}

	f2, err := Parse(canonical1)
	if err != nil {
		t.Fatalf("Parse(canonical1): %v", err)
	}
	canonical2, err := f2.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical (2nd): %v", err)
	}

	if string(canonical1) != string(canonical2) {
		t.Fatalf("canonical serializer is not a stable fixed point:\n--- 1st ---\n%s\n--- 2nd ---\n%s", canonical1, canonical2)
	}
}

func TestFLOWFILE_CanonicalIsIndependentOfInputArrayOrder(t *testing.T) {
	f, err := Parse([]byte(exampleFlowJSON))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	canonicalOriginal, err := f.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}

	shuffled := *f
	shuffled.Graph.Nodes = []Node{f.Graph.Nodes[2], f.Graph.Nodes[0], f.Graph.Nodes[1]}
	shuffled.Graph.Wires = []Wire{f.Graph.Wires[1], f.Graph.Wires[0]}

	canonicalShuffled, err := shuffled.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical (shuffled): %v", err)
	}

	if string(canonicalOriginal) != string(canonicalShuffled) {
		t.Fatalf("canonical output depends on input array order:\n--- original order ---\n%s\n--- shuffled order ---\n%s", canonicalOriginal, canonicalShuffled)
	}
}

func TestFLOWFILE_CanonicalSortsObjectKeysAlphabetically(t *testing.T) {
	f, err := Parse([]byte(exampleFlowJSON))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	canonical, err := f.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}

	var generic map[string]json.RawMessage
	if err := json.Unmarshal(canonical, &generic); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Spot-check top-level key order in the raw bytes: "description" (index
	// 3 alphabetically among our fields) must appear before "formatVersion".
	descIdx := bytes.Index(canonical, []byte(`"description"`))
	fvIdx := bytes.Index(canonical, []byte(`"formatVersion"`))
	if descIdx < 0 || fvIdx < 0 {
		t.Fatalf("expected fields not found in canonical output:\n%s", canonical)
	}
	if descIdx > fvIdx {
		t.Errorf("expected alphabetical key order (description before formatVersion), got description@%d formatVersion@%d", descIdx, fvIdx)
	}
}

func TestFLOWFILE_CanonicalUsesTwoSpaceIndent(t *testing.T) {
	f, err := Parse([]byte(exampleFlowJSON))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	canonical, err := f.MarshalCanonical()
	if err != nil {
		t.Fatalf("MarshalCanonical: %v", err)
	}
	if !bytes.Contains(canonical, []byte("\n  \"")) {
		t.Errorf("expected 2-space indented lines in canonical output, got:\n%s", canonical)
	}
}
