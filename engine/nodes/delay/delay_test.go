package delay

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

func TestPROC350_NewRequiresValidMode(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"bogus"}`)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
	if _, err := New(json.RawMessage(`{"mode":"throttle"}`)); err == nil {
		t.Fatal("expected an error: maxPerInterval/intervalMs required")
	}
}

func TestPROC350_FixedDelayBlocksForConfiguredDuration(t *testing.T) {
	n := newTestNode(t, Config{Mode: "delay", DelayMs: 60})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})
	start := time.Now()
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("Process returned after %v, expected to block ~60ms", elapsed)
	}
	if results[0].Datagram.Payload.Value != 1 {
		t.Errorf("payload should pass through unchanged, got %v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC350_ExpressionDelayComputesFromPayload(t *testing.T) {
	n := newTestNode(t, Config{Mode: "delay", DelayExpression: "payload.delayMs"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"delayMs": 50.0}})
	start := time.Now()
	if _, err := n.Process(context.Background(), in); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Errorf("Process returned after %v, expected to block ~50ms", elapsed)
	}
}

func TestPROC350_DelayRespectsContextCancellation(t *testing.T) {
	n := newTestNode(t, Config{Mode: "delay", DelayMs: 5000})
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := n.Process(ctx, datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1}))
	if err == nil {
		t.Fatal("expected a context-cancellation error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Process took %v, expected to stop promptly on cancellation", elapsed)
	}
}

func TestPROC350_ThrottleAllowsUpToLimitImmediately(t *testing.T) {
	n := newTestNode(t, Config{Mode: "throttle", MaxPerInterval: 3, IntervalMs: 1000})
	start := time.Now()
	for i := 0; i < 3; i++ {
		if _, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: i})); err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("first 3 items took %v, expected near-instant (within the limit)", elapsed)
	}
}

func TestPROC350_ThrottleDelaysThePlusOnethItem(t *testing.T) {
	n := newTestNode(t, Config{Mode: "throttle", MaxPerInterval: 2, IntervalMs: 100})
	for i := 0; i < 2; i++ {
		if _, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: i})); err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
	}
	start := time.Now()
	if _, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 2})); err != nil {
		t.Fatalf("Process 2: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Errorf("3rd item within the same window returned after %v, expected to wait for the window to free up", elapsed)
	}
}

func TestPROC350_ThrottleGroupsIndependently(t *testing.T) {
	n := newTestNode(t, Config{Mode: "throttle", MaxPerInterval: 1, IntervalMs: 5000, GroupBy: "tags.line"})
	inA := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})
	inA.Header.Tags = map[string]string{"line": "A"}
	inB := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 2})
	inB.Header.Tags = map[string]string{"line": "B"}

	start := time.Now()
	if _, err := n.Process(context.Background(), inA); err != nil {
		t.Fatalf("A: %v", err)
	}
	if _, err := n.Process(context.Background(), inB); err != nil {
		t.Fatalf("B: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("independent groups took %v, expected both to pass immediately", elapsed)
	}
}
