package set

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func testSource() datagram.Source { return datagram.Source{NodeID: "test"} }

func newSetNode(t *testing.T, ops ...SetOp) flow.Processor {
	t.Helper()
	raw, err := json.Marshal(Config{Sets: ops})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return n.(flow.Processor)
}

func TestSET_SetsNestedPathCreatingIntermediateMaps(t *testing.T) {
	n := newSetNode(t, SetOp{Path: "a.b.c", Value: 42})
	in := datagram.New(testSource(), datagram.Payload{Value: map[string]any{}})

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("results = %+v, want one PortDatagram on port \"out\"", results)
	}

	m, ok := results[0].Datagram.Payload.Value.(map[string]any)
	if !ok {
		t.Fatalf("payload = %T, want map[string]any", results[0].Datagram.Payload.Value)
	}
	a, _ := m["a"].(map[string]any)
	b, _ := a["b"].(map[string]any)
	// Config.Sets[].Value round-trips through JSON in New(), so a literal
	// 42 becomes float64(42) — real behavior, not a test artifact.
	if b["c"] != float64(42) {
		t.Errorf("a.b.c = %v (%T), want 42", b["c"], b["c"])
	}
}

func TestSET_MultipleSetsApplyInOrder(t *testing.T) {
	n := newSetNode(t, SetOp{Path: "x", Value: 1}, SetOp{Path: "x", Value: 2})
	in := datagram.New(testSource(), datagram.Payload{Value: map[string]any{}})

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if m["x"] != float64(2) {
		t.Errorf("x = %v (%T), want 2 (last set wins)", m["x"], m["x"])
	}
}

func TestSET_EmptyPathReplacesWholePayload(t *testing.T) {
	n := newSetNode(t, SetOp{Path: "", Value: "replaced"})
	in := datagram.New(testSource(), datagram.Payload{Value: map[string]any{"old": true}})

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value != "replaced" {
		t.Errorf("payload = %v, want %q", results[0].Datagram.Payload.Value, "replaced")
	}
}

func TestSET_DoesNotMutateInputPayload(t *testing.T) {
	n := newSetNode(t, SetOp{Path: "a", Value: "new"})
	original := map[string]any{"a": "old", "b": "unchanged"}
	in := datagram.New(testSource(), datagram.Payload{Value: original})

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}

	if original["a"] != "old" {
		t.Errorf("input payload was mutated: a = %v, want unchanged %q (BUS-140 independent copies)", original["a"], "old")
	}
	out := results[0].Datagram.Payload.Value.(map[string]any)
	if out["a"] != "new" || out["b"] != "unchanged" {
		t.Errorf("output = %+v, want a=new b=unchanged", out)
	}
}

func TestSET_LineagePreserved(t *testing.T) {
	n := newSetNode(t, SetOp{Path: "a", Value: 1})
	in := datagram.New(testSource(), datagram.Payload{Value: map[string]any{}})

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	out := results[0].Datagram
	if out.Header.CausationID != in.Header.ID {
		t.Errorf("causation id = %q, want %q", out.Header.CausationID, in.Header.ID)
	}
	if out.Header.CorrelationID != in.Header.CorrelationID {
		t.Errorf("correlation id not propagated")
	}
}
