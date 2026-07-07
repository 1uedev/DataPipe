package filter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func newTestNode(t *testing.T, cfg Config) *node {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return instance.(*node)
}

func port(t *testing.T, n *node, value any) string {
	t.Helper()
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: value})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected exactly 1 output, got %+v", results)
	}
	return results[0].Port
}

func TestPROC310_NewRequiresValidMode(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"bogus"}`)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
	if _, err := New(json.RawMessage(`{"mode":"predicate"}`)); err == nil {
		t.Fatal("expected an error: predicate is required")
	}
	if _, err := New(json.RawMessage(`{"mode":"deadband"}`)); err == nil {
		t.Fatal("expected an error: field is required")
	}
}

func TestPROC310_PredicateModePassesAndDrops(t *testing.T) {
	n := newTestNode(t, Config{Mode: "predicate", Predicate: "payload.value > 10"})
	if got := port(t, n, map[string]any{"value": 20.0}); got != "pass" {
		t.Errorf("port = %q, want pass", got)
	}
	if got := port(t, n, map[string]any{"value": 5.0}); got != "drop" {
		t.Errorf("port = %q, want drop", got)
	}
}

func TestPROC310_DeadbandModeFirstValueAlwaysPasses(t *testing.T) {
	n := newTestNode(t, Config{Mode: "deadband", Field: "value", Deadband: 1.0})
	if got := port(t, n, map[string]any{"value": 20.0}); got != "pass" {
		t.Errorf("first value port = %q, want pass", got)
	}
}

func TestPROC310_DeadbandModeDropsWithinBand(t *testing.T) {
	n := newTestNode(t, Config{Mode: "deadband", Field: "value", Deadband: 1.0})
	port(t, n, map[string]any{"value": 20.0})
	if got := port(t, n, map[string]any{"value": 20.5}); got != "drop" {
		t.Errorf("port = %q, want drop (within deadband)", got)
	}
	if got := port(t, n, map[string]any{"value": 21.5}); got != "pass" {
		t.Errorf("port = %q, want pass (beyond deadband)", got)
	}
}

func TestPROC310_DeadbandModeHeartbeatForcesPassAfterInterval(t *testing.T) {
	n := newTestNode(t, Config{Mode: "deadband", Field: "value", Deadband: 100.0, MinIntervalMs: 50})
	port(t, n, map[string]any{"value": 20.0})
	if got := port(t, n, map[string]any{"value": 20.1}); got != "drop" {
		t.Errorf("port = %q, want drop (well within deadband, no interval elapsed)", got)
	}
	time.Sleep(80 * time.Millisecond)
	if got := port(t, n, map[string]any{"value": 20.1}); got != "pass" {
		t.Errorf("port = %q, want pass (heartbeat interval elapsed)", got)
	}
}

func TestPROC310_DeadbandModeGroupsIndependently(t *testing.T) {
	n := newTestNode(t, Config{Mode: "deadband", Field: "value", Deadband: 1.0, GroupBy: "tags.line"})
	inA := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 20.0}})
	inA.Header.Tags = map[string]string{"line": "A"}
	inB := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 20.0}})
	inB.Header.Tags = map[string]string{"line": "B"}

	if r, err := n.Process(context.Background(), inA); err != nil || r[0].Port != "pass" {
		t.Fatalf("line A first value: %+v, %v", r, err)
	}
	// Line B's first value should also pass (independent group state), even
	// though its value is identical to line A's already-seen value.
	if r, err := n.Process(context.Background(), inB); err != nil || r[0].Port != "pass" {
		t.Fatalf("line B first value: %+v, %v", r, err)
	}
}

func TestPROC310_DeadbandModeRejectsNonNumericField(t *testing.T) {
	n := newTestNode(t, Config{Mode: "deadband", Field: "value", Deadband: 1.0})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": "not-a-number"}})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error for a non-numeric field")
	}
}
