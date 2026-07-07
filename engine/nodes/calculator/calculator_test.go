package calculator

import (
	"context"
	"encoding/json"
	"testing"

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

func asFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	default:
		t.Fatalf("value %v (%T) is not numeric", v, v)
		return 0
	}
}

func TestPROC200_NewRequiresFields(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error: fields is required")
	}
}

func TestPROC200_NewRejectsInvalidExpression(t *testing.T) {
	cfg := Config{Fields: []FieldOp{{Path: "x", Expression: "not valid js !!"}}}
	raw, _ := json.Marshal(cfg)
	if _, err := New(raw); err == nil {
		t.Fatal("expected a compile error")
	}
}

func TestPROC200_UnitConversion(t *testing.T) {
	n := newTestNode(t, Config{Fields: []FieldOp{{Path: "fahrenheit", Expression: "payload.celsius * 9/5 + 32"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"celsius": 100.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if asFloat(t, m["fahrenheit"]) != 212 {
		t.Errorf("fahrenheit = %v", m["fahrenheit"])
	}
	if asFloat(t, m["celsius"]) != 100 {
		t.Errorf("original celsius field should survive, got %v", m["celsius"])
	}
}

func TestPROC200_LaterFieldsSeeEarlierResults(t *testing.T) {
	n := newTestNode(t, Config{Fields: []FieldOp{
		{Path: "doubled", Expression: "payload.x * 2"},
		{Path: "quadrupled", Expression: "payload.doubled * 2"},
	}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"x": 5.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if asFloat(t, m["quadrupled"]) != 20 {
		t.Errorf("quadrupled = %v", m["quadrupled"])
	}
}

func TestPROC200_StatisticalHelpersAvailable(t *testing.T) {
	n := newTestNode(t, Config{Fields: []FieldOp{{Path: "avg", Expression: "stats.mean(payload.readings)"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"readings": []any{1.0, 2.0, 3.0}}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if asFloat(t, m["avg"]) != 2 {
		t.Errorf("avg = %v", m["avg"])
	}
}

func TestPROC200_EmptyPathReplacesWholePayload(t *testing.T) {
	n := newTestNode(t, Config{Fields: []FieldOp{{Path: "", Expression: "payload.x + 1"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"x": 41.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if asFloat(t, results[0].Datagram.Payload.Value) != 42 {
		t.Errorf("payload = %v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC200_DoesNotMutateInputPayload(t *testing.T) {
	n := newTestNode(t, Config{Fields: []FieldOp{{Path: "y", Expression: "payload.x + 1"}}})
	original := map[string]any{"x": 1.0}
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: original})
	if _, err := n.Process(context.Background(), in); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if _, exists := original["y"]; exists {
		t.Error("Process must not mutate the caller's input payload map in place (fan-out safety)")
	}
}
