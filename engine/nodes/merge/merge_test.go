package merge

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

func dgm(value any) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: value})
}

func TestPROC320_NewValidatesConfig(t *testing.T) {
	cases := []Config{
		{Mode: "bogus"},
		{Mode: "combineLatest"},              // missing keys
		{Mode: "join", KeyA: "x", KeyB: "y"}, // missing windowMs
		{Mode: "join", KeyA: "x", KeyB: "y", WindowMs: 100, JoinType: "bogus"},
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

func TestPROC320_ConcatenatePassesThroughBothPorts(t *testing.T) {
	n := newTestNode(t, Config{Mode: "concatenate"})
	results, err := n.ProcessPort(context.Background(), "a", dgm(1))
	if err != nil || len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("a: results=%+v err=%v", results, err)
	}
	results, err = n.ProcessPort(context.Background(), "b", dgm(2))
	if err != nil || len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("b: results=%+v err=%v", results, err)
	}
}

func TestPROC320_CombineLatestWaitsForBothSidesThenEmitsOnEveryArrival(t *testing.T) {
	n := newTestNode(t, Config{Mode: "combineLatest", KeyA: "payload", KeyB: "payload"})
	results, err := n.ProcessPort(context.Background(), "a", dgm(1))
	if err != nil {
		t.Fatalf("a: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no emit before both sides have arrived, got %+v", results)
	}

	results, err = n.ProcessPort(context.Background(), "b", dgm(2))
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected an emit once both sides arrived, got %+v", results)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if m["a"] != 1 || m["b"] != 2 {
		t.Errorf("combined = %+v", m)
	}

	// A new arrival on either side should emit again, combined with the
	// other side's latest value.
	results, err = n.ProcessPort(context.Background(), "a", dgm(3))
	if err != nil {
		t.Fatalf("a2: %v", err)
	}
	m = results[0].Datagram.Payload.Value.(map[string]any)
	if m["a"] != 3 || m["b"] != 2 {
		t.Errorf("combined after a2 = %+v", m)
	}
}

func TestPROC320_JoinInnerMatchesByKeyWithinWindow(t *testing.T) {
	n := newTestNode(t, Config{Mode: "join", KeyA: "payload.id", KeyB: "payload.id", WindowMs: 1000, JoinType: "inner"})
	a := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "x", "temp": 21.5}})
	b := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "x", "pressure": 1.1}})

	results, err := n.ProcessPort(context.Background(), "a", a)
	if err != nil || len(results) != 0 {
		t.Fatalf("a: results=%+v err=%v, want no emit yet", results, err)
	}
	results, err = n.ProcessPort(context.Background(), "b", b)
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected a join match, got %+v", results)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if m["a"].(map[string]any)["temp"] != 21.5 || m["b"].(map[string]any)["pressure"] != 1.1 {
		t.Errorf("joined = %+v", m)
	}
}

func TestPROC320_JoinInnerDropsExpiredUnmatchedSide(t *testing.T) {
	n := newTestNode(t, Config{Mode: "join", KeyA: "payload.id", KeyB: "payload.id", WindowMs: 30, JoinType: "inner"})
	a := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "x"}})
	if _, err := n.ProcessPort(context.Background(), "a", a); err != nil {
		t.Fatalf("a: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	// A late "b" with a different key just triggers the lazy expiry check.
	other := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "y"}})
	results, err := n.ProcessPort(context.Background(), "b", other)
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("inner join must not emit the expired unmatched \"a\", got %+v", results)
	}

	// The stale "a" must actually be gone: a fresh matching "b" for "x"
	// should NOT still find it (it was evicted).
	late := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "x"}})
	results, err = n.ProcessPort(context.Background(), "b", late)
	if err != nil {
		t.Fatalf("late b: %v", err)
	}
	if len(results) != 0 {
		t.Fatalf("expected no match against the already-expired \"a\", got %+v", results)
	}
}

func TestPROC320_JoinLeftEmitsUnmatchedAWithNilBAfterExpiry(t *testing.T) {
	n := newTestNode(t, Config{Mode: "join", KeyA: "payload.id", KeyB: "payload.id", WindowMs: 30, JoinType: "left"})
	a := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "x", "temp": 5.0}})
	if _, err := n.ProcessPort(context.Background(), "a", a); err != nil {
		t.Fatalf("a: %v", err)
	}
	time.Sleep(60 * time.Millisecond)

	other := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "y"}})
	results, err := n.ProcessPort(context.Background(), "b", other)
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("left join must emit the expired unmatched \"a\" with nil b, got %+v", results)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if m["b"] != nil {
		t.Errorf("b = %v, want nil", m["b"])
	}
	if m["a"].(map[string]any)["temp"] != 5.0 {
		t.Errorf("a = %+v", m["a"])
	}
}

func TestPROC320_BatchMergeCombinesByCorrelationID(t *testing.T) {
	n := newTestNode(t, Config{Mode: "batchMerge"})
	a := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "a-value"})
	b := datagram.NewCaused(a, datagram.Source{NodeID: "test"}, datagram.Payload{Value: "b-value"})
	// NewCaused sets CorrelationID to the same lineage as a, per DGM-160.
	if b.Header.CorrelationID != a.Header.CorrelationID {
		t.Fatalf("test setup: expected b to share a's correlation id")
	}

	results, err := n.ProcessPort(context.Background(), "a", a)
	if err != nil || len(results) != 0 {
		t.Fatalf("a: results=%+v err=%v", results, err)
	}
	results, err = n.ProcessPort(context.Background(), "b", b)
	if err != nil {
		t.Fatalf("b: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected a batch-merge emit, got %+v", results)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if m["a"] != "a-value" || m["b"] != "b-value" {
		t.Errorf("merged = %+v", m)
	}
}

func TestPROC320_BatchMergeRequiresCorrelationID(t *testing.T) {
	n := newTestNode(t, Config{Mode: "batchMerge"})
	bare := datagram.Datagram{Header: datagram.Header{ID: "no-correlation"}, Payload: datagram.Payload{Value: 1}}
	if _, err := n.ProcessPort(context.Background(), "a", bare); err == nil {
		t.Fatal("expected an error: no correlationId to merge on")
	}
}
