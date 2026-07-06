package flow

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestMAP110_PreviewSourceStopsAtMaxRecords(t *testing.T) {
	cfg, err := json.Marshal(map[string]int{"intervalMs": 5})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	results, err := PreviewSource(context.Background(), "graph-test-emitter", cfg, 3, 5*time.Second)
	if err != nil {
		t.Fatalf("PreviewSource: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected exactly 3 records, got %d", len(results))
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("PreviewSource took %v, expected to stop promptly once maxRecords was hit, not wait out the 5s timeout", elapsed)
	}
	for _, r := range results {
		if r.Port != "out" {
			t.Errorf("port = %q, want out", r.Port)
		}
	}
}

func TestMAP110_PreviewSourceRespectsTimeoutWhenTooFewRecords(t *testing.T) {
	cfg, err := json.Marshal(map[string]int{"intervalMs": 200})
	if err != nil {
		t.Fatal(err)
	}
	results, err := PreviewSource(context.Background(), "graph-test-emitter", cfg, 100, 300*time.Millisecond)
	if err != nil {
		t.Fatalf("PreviewSource: %v", err)
	}
	if len(results) == 0 || len(results) >= 100 {
		t.Fatalf("expected a partial sample bounded by the timeout, got %d records", len(results))
	}
}

func TestMAP110_PreviewSourceRejectsProcessorNodeType(t *testing.T) {
	cfg, err := json.Marshal(map[string]float64{"addend": 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PreviewSource(context.Background(), "graph-test-transform", cfg, 5, time.Second); err == nil {
		t.Fatal("expected an error: preview only applies to Source node types")
	}
}

func TestMAP110_PreviewSourceUnknownNodeType(t *testing.T) {
	if _, err := PreviewSource(context.Background(), "does-not-exist", nil, 5, time.Second); err == nil {
		t.Fatal("expected an error for an unregistered node type")
	}
}

func TestMAP110_PreviewSourcePanicIsRecovered(t *testing.T) {
	if _, err := PreviewSource(context.Background(), "exec-test-panicky-source", nil, 5, time.Second); err == nil {
		t.Fatal("expected the recovered panic to surface as an error (ARC-150)")
	}
}

func newPanickySource(json.RawMessage) (any, error) { return panickySource{}, nil }

func init() {
	Register("exec-test-panicky-source", NodeTypeInfo{Kind: KindSource, Outputs: []string{"out"}}, newPanickySource)
}
