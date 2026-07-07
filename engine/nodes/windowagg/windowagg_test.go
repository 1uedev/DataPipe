package windowagg

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

func send(t *testing.T, n *node, value any) []map[string]any {
	t.Helper()
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: value})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	out := make([]map[string]any, len(results))
	for i, r := range results {
		out[i] = r.Datagram.Payload.Value.(map[string]any)
	}
	return out
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

func TestPROC210_NewValidatesConfig(t *testing.T) {
	cases := []Config{
		{WindowType: "bogus"},
		{WindowType: "tumbling", WindowBy: "count"},           // missing count
		{WindowType: "session"},                               // missing sessionGapMs
		{WindowType: "tumbling", WindowBy: "count", Count: 1}, // missing aggregates
		{WindowType: "tumbling", WindowBy: "count", Count: 1, Aggregates: []AggregateOp{{Op: "bad"}}}, // unknown op
	}
	for i, cfg := range cases {
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := New(raw); err == nil {
			t.Errorf("case %d: expected an error for %+v", i, cfg)
		}
	}
}

func TestPROC210_TumblingByCountEmitsOnceFullAndResets(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "tumbling", WindowBy: "count", Count: 3,
		Aggregates: []AggregateOp{{Field: "value", Op: "avg", As: "avg"}, {Op: "count", As: "n"}},
	})
	for _, v := range []float64{10, 20} {
		out := send(t, n, map[string]any{"value": v})
		if len(out) != 0 {
			t.Fatalf("expected no emit before window fills, got %+v", out)
		}
	}
	out := send(t, n, map[string]any{"value": 30})
	if len(out) != 1 {
		t.Fatalf("expected exactly 1 emit when window fills, got %+v", out)
	}
	if asFloat(t, out[0]["avg"]) != 20 || asFloat(t, out[0]["n"]) != 3 {
		t.Errorf("emitted = %+v", out[0])
	}

	// The window must have reset: 2 more items shouldn't emit yet.
	for _, v := range []float64{1, 2} {
		out := send(t, n, map[string]any{"value": v})
		if len(out) != 0 {
			t.Fatalf("expected no emit in the fresh window, got %+v", out)
		}
	}
}

func TestPROC210_TumblingByTimeEmitsOnNextArrivalAfterBoundary(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "tumbling", WindowBy: "time", SizeMs: 50,
		Aggregates: []AggregateOp{{Field: "value", Op: "sum", As: "sum"}},
	})
	out := send(t, n, map[string]any{"value": 1.0})
	if len(out) != 0 {
		t.Fatalf("expected no emit on the window's first item, got %+v", out)
	}
	time.Sleep(80 * time.Millisecond)
	out = send(t, n, map[string]any{"value": 2.0})
	if len(out) != 1 {
		t.Fatalf("expected the expired window to emit on the next arrival, got %+v", out)
	}
	if asFloat(t, out[0]["sum"]) != 1 {
		t.Errorf("closed window sum = %v, want 1 (the triggering item starts the NEW window)", out[0]["sum"])
	}
}

func TestPROC210_SlidingByCountKeepsLastNItems(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "sliding", WindowBy: "count", Count: 2,
		Aggregates: []AggregateOp{{Field: "value", Op: "collect", As: "items"}},
	})
	send(t, n, map[string]any{"value": 1.0})
	out := send(t, n, map[string]any{"value": 2.0})
	items := out[0]["items"].([]any)
	if len(items) != 2 || asFloat(t, items[0]) != 1 || asFloat(t, items[1]) != 2 {
		t.Fatalf("items = %+v", items)
	}
	out = send(t, n, map[string]any{"value": 3.0})
	items = out[0]["items"].([]any)
	if len(items) != 2 || asFloat(t, items[0]) != 2 || asFloat(t, items[1]) != 3 {
		t.Fatalf("sliding window should have dropped the oldest item, got %+v", items)
	}
}

func TestPROC210_SessionClosesAfterGapAndExcludesTriggeringItem(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "session", SessionGapMs: 50,
		Aggregates: []AggregateOp{{Op: "count", As: "n"}},
	})
	out := send(t, n, map[string]any{"value": 1.0})
	if len(out) != 0 {
		t.Fatalf("expected no emit on the first item of a session, got %+v", out)
	}
	out = send(t, n, map[string]any{"value": 2.0})
	if len(out) != 0 {
		t.Fatalf("expected no emit within the session gap, got %+v", out)
	}
	time.Sleep(80 * time.Millisecond)
	out = send(t, n, map[string]any{"value": 3.0})
	if len(out) != 1 || asFloat(t, out[0]["n"]) != 2 {
		t.Fatalf("expected the closed session (2 items) to emit, got %+v", out)
	}
}

func TestPROC210_GroupByKeepsGroupsIndependent(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "tumbling", WindowBy: "count", Count: 2, GroupBy: "tags.line",
		Aggregates: []AggregateOp{{Field: "value", Op: "sum", As: "sum"}},
	})
	inA1 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 1.0}})
	inA1.Header.Tags = map[string]string{"line": "A"}
	inB1 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 100.0}})
	inB1.Header.Tags = map[string]string{"line": "B"}

	if r, err := n.Process(context.Background(), inA1); err != nil || len(r) != 0 {
		t.Fatalf("A1: results=%+v err=%v", r, err)
	}
	if r, err := n.Process(context.Background(), inB1); err != nil || len(r) != 0 {
		t.Fatalf("B1: results=%+v err=%v", r, err)
	}

	inA2 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 2.0}})
	inA2.Header.Tags = map[string]string{"line": "A"}
	results, err := n.Process(context.Background(), inA2)
	if err != nil {
		t.Fatalf("A2: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected line A's window to close independently of line B, got %+v", results)
	}
	sum := results[0].Datagram.Payload.Value.(map[string]any)["sum"]
	if asFloat(t, sum) != 3 {
		t.Errorf("line A sum = %v, want 3 (1+2, not contaminated by line B's 100)", sum)
	}
}

func TestPROC210_PercentileAggregate(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "tumbling", WindowBy: "count", Count: 5,
		Aggregates: []AggregateOp{{Field: "v", Op: "percentile", Percentile: 50, As: "p50"}},
	})
	var out []map[string]any
	for _, v := range []float64{1, 2, 3, 4, 5} {
		out = send(t, n, map[string]any{"v": v})
	}
	if asFloat(t, out[0]["p50"]) != 3 {
		t.Errorf("p50 = %v, want 3", out[0]["p50"])
	}
}

func TestPROC210_NonNumericFieldErrorsForNumericAggregate(t *testing.T) {
	n := newTestNode(t, Config{
		WindowType: "tumbling", WindowBy: "count", Count: 1,
		Aggregates: []AggregateOp{{Field: "v", Op: "avg", As: "avg"}},
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"v": "not-a-number"}})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error aggregating a non-numeric field")
	}
}
