package splitbatch

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

func TestPROC330_NewRequiresValidMode(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"bogus"}`)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
	if _, err := New(json.RawMessage(`{"mode":"batch"}`)); err == nil {
		t.Fatal("expected an error: batch requires at least one limit")
	}
}

func TestPROC330_SplitWholePayloadArray(t *testing.T) {
	n := newTestNode(t, Config{Mode: "split"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{"a", "b", "c"}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(results))
	}
	for i, want := range []string{"a", "b", "c"} {
		if results[i].Datagram.Payload.Value != want {
			t.Errorf("results[%d] = %v, want %v", i, results[i].Datagram.Payload.Value, want)
		}
	}
}

func TestPROC330_SplitByField(t *testing.T) {
	n := newTestNode(t, Config{Mode: "split", Field: "items"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"items": []any{1.0, 2.0}}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 outputs, got %d", len(results))
	}
}

func TestPROC330_SplitRejectsNonArrayField(t *testing.T) {
	n := newTestNode(t, Config{Mode: "split"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "not an array"})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error for a non-array payload")
	}
}

func TestPROC330_BatchByCount(t *testing.T) {
	n := newTestNode(t, Config{Mode: "batch", MaxCount: 3})
	for i := 0; i < 2; i++ {
		results, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: float64(i)}))
		if err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
		if len(results) != 0 {
			t.Fatalf("expected no emit before the batch fills, got %+v", results)
		}
	}
	results, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 2.0}))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected exactly 1 emit when the batch fills, got %+v", results)
	}
	batch := results[0].Datagram.Payload.Value.([]any)
	if len(batch) != 3 {
		t.Fatalf("batch = %+v, want 3 items", batch)
	}
}

func TestPROC330_BatchResetsAfterFlush(t *testing.T) {
	n := newTestNode(t, Config{Mode: "batch", MaxCount: 2})
	send := func(v float64) []any {
		results, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: v}))
		if err != nil {
			t.Fatalf("Process: %v", err)
		}
		if len(results) == 0 {
			return nil
		}
		return results[0].Datagram.Payload.Value.([]any)
	}
	send(1)
	first := send(2)
	if len(first) != 2 {
		t.Fatalf("first batch = %+v", first)
	}
	send(3)
	second := send(4)
	if len(second) != 2 || second[0] != 3.0 || second[1] != 4.0 {
		t.Fatalf("second batch = %+v, want a fresh [3, 4]", second)
	}
}

func TestPROC330_BatchByIntervalFlushesOnNextArrivalAfterExpiry(t *testing.T) {
	n := newTestNode(t, Config{Mode: "batch", MaxIntervalMs: 50})
	results, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1.0}))
	if err != nil || len(results) != 0 {
		t.Fatalf("first item: results=%+v err=%v", results, err)
	}
	time.Sleep(80 * time.Millisecond)
	results, err = n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 2.0}))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected the expired batch to flush, got %+v", results)
	}
	batch := results[0].Datagram.Payload.Value.([]any)
	if len(batch) != 2 {
		t.Fatalf("batch = %+v, want the expired-and-triggering items together", batch)
	}
}

func TestPROC330_BatchByBytesFlushesBeforeExceedingLimit(t *testing.T) {
	n := newTestNode(t, Config{Mode: "batch", MaxBytes: 10})
	big := "0123456789" // 12 bytes as a JSON string incl. quotes
	results, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: big}))
	if err != nil || len(results) != 0 {
		t.Fatalf("first item: results=%+v err=%v", results, err)
	}
	results, err = n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: big}))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected the first item's batch to flush before adding the second, got %+v", results)
	}
	batch := results[0].Datagram.Payload.Value.([]any)
	if len(batch) != 1 {
		t.Fatalf("flushed batch = %+v, want exactly the first item alone", batch)
	}
}
