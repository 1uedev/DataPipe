package flow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func newExecTestPanicky(json.RawMessage) (any, error) { return &panickyProcessor{}, nil }

type execTestSource struct{}

func (execTestSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	return nil
}
func newExecTestSource(json.RawMessage) (any, error) { return execTestSource{}, nil }

func init() {
	Register("exec-test-panicky", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newExecTestPanicky)
	Register("exec-test-source", NodeTypeInfo{Kind: KindSource, Outputs: []string{"out"}}, newExecTestSource)
}

func TestDBG130_ExecuteNodeRunsProcessorAgainstPinnedInput(t *testing.T) {
	cfg, err := json.Marshal(map[string]float64{"addend": 10})
	if err != nil {
		t.Fatal(err)
	}
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: float64(5)})
	results, err := ExecuteNode(context.Background(), "graph-test-transform", cfg, in)
	if err != nil {
		t.Fatalf("ExecuteNode: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 output, got %d", len(results))
	}
	got, _ := results[0].Datagram.Payload.Value.(float64)
	if got != 15 {
		t.Errorf("output value = %v, want 15", got)
	}
	// Design-time execution must not fabricate lineage that didn't happen on
	// a real bus (DGM-160 only makes sense once a datagram truly flowed).
	if results[0].Datagram.Header.CausationID != in.Header.ID {
		t.Errorf("causation id not linked back to the pinned input")
	}
}

func TestDBG130_ExecuteNodeUnknownType(t *testing.T) {
	if _, err := ExecuteNode(context.Background(), "does-not-exist", nil, testDgm(1)); err == nil {
		t.Fatal("expected an error for an unregistered node type")
	}
}

func TestDBG130_ExecuteNodeRejectsSourceNodes(t *testing.T) {
	if _, err := ExecuteNode(context.Background(), "exec-test-source", nil, testDgm(1)); err == nil {
		t.Fatal("expected an error: Source nodes are out of scope for design-time execution")
	}
}

func TestDBG130_ExecuteNodePanicIsRecovered(t *testing.T) {
	_, err := ExecuteNode(context.Background(), "exec-test-panicky", nil, testDgm(1))
	if err == nil {
		t.Fatal("expected the recovered panic to surface as an error (ARC-150)")
	}
	var ne *NodeError
	if !errors.As(err, &ne) {
		t.Fatalf("expected a *NodeError, got %T: %v", err, err)
	}
	if ne.Message != "boom" {
		t.Errorf("NodeError.Message = %q, want %q", ne.Message, "boom")
	}
}
