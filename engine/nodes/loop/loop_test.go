package loop

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
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

func TestPROC340_EmptyArrayGoesStraightToDone(t *testing.T) {
	n := newTestNode(t, Config{})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "done" {
		t.Fatalf("results = %+v", results)
	}
}

func TestPROC340_NonArrayFieldErrors(t *testing.T) {
	n := newTestNode(t, Config{})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "not an array"})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error for a non-array payload")
	}
}

func TestPROC340_IteratesEachItemThenDone(t *testing.T) {
	n := newTestNode(t, Config{})
	trigger := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{"a", "b", "c"}})

	results, err := n.Process(context.Background(), trigger)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if len(results) != 1 || results[0].Port != "loop" || results[0].Datagram.Payload.Value != "a" {
		t.Fatalf("start results = %+v", results)
	}

	// Simulate the loop-back wire: the sub-flow's output is a NewCaused
	// datagram derived from the "loop" emission, so it shares the same
	// correlation id all the way back to this node's "in" port.
	cont1 := datagram.NewCaused(results[0].Datagram, datagram.Source{NodeID: "sub"}, datagram.Payload{Value: "processed-a"})
	results, err = n.Process(context.Background(), cont1)
	if err != nil {
		t.Fatalf("continue 1: %v", err)
	}
	if len(results) != 1 || results[0].Port != "loop" || results[0].Datagram.Payload.Value != "b" {
		t.Fatalf("continue 1 results = %+v", results)
	}

	cont2 := datagram.NewCaused(results[0].Datagram, datagram.Source{NodeID: "sub"}, datagram.Payload{Value: "processed-b"})
	results, err = n.Process(context.Background(), cont2)
	if err != nil {
		t.Fatalf("continue 2: %v", err)
	}
	if len(results) != 1 || results[0].Port != "loop" || results[0].Datagram.Payload.Value != "c" {
		t.Fatalf("continue 2 results = %+v", results)
	}

	cont3 := datagram.NewCaused(results[0].Datagram, datagram.Source{NodeID: "sub"}, datagram.Payload{Value: "processed-c"})
	results, err = n.Process(context.Background(), cont3)
	if err != nil {
		t.Fatalf("continue 3: %v", err)
	}
	if len(results) != 1 || results[0].Port != "done" {
		t.Fatalf("final results = %+v, want done", results)
	}
	if results[0].Datagram.Payload.Value.(map[string]any)["count"] != float64(3) {
		t.Errorf("done payload = %+v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC340_MaxIterationsGuaranteesTermination(t *testing.T) {
	n := newTestNode(t, Config{MaxIterations: 2})
	trigger := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{"a", "b", "c", "d", "e"}})

	results, err := n.Process(context.Background(), trigger)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	// maxIterations=2 allows indices 0 and 1 (item "a" then "b") before the
	// NEXT continuation is forced to done, well short of all 5 items.
	cont1 := datagram.NewCaused(results[0].Datagram, datagram.Source{NodeID: "sub"}, datagram.Payload{Value: nil})
	results, err = n.Process(context.Background(), cont1)
	if err != nil {
		t.Fatalf("continue 1: %v", err)
	}
	if len(results) != 1 || results[0].Port != "loop" || results[0].Datagram.Payload.Value != "b" {
		t.Fatalf("continue 1 results = %+v, want item \"b\"", results)
	}

	cont2 := datagram.NewCaused(results[0].Datagram, datagram.Source{NodeID: "sub"}, datagram.Payload{Value: nil})
	results, err = n.Process(context.Background(), cont2)
	if err != nil {
		t.Fatalf("continue 2: %v", err)
	}
	if len(results) != 1 || results[0].Port != "done" {
		t.Fatalf("expected maxIterations=2 to force done well before all 5 items, got %+v", results)
	}
}

func TestPROC340_UnrelatedCorrelationIDDoesNotCollideWithAnOpenSession(t *testing.T) {
	n := newTestNode(t, Config{})
	trigger := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{"a", "b"}})
	if _, err := n.Process(context.Background(), trigger); err != nil {
		t.Fatalf("start: %v", err)
	}

	unrelated := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{"x"}})
	results, err := n.Process(context.Background(), unrelated)
	if err != nil {
		t.Fatalf("unrelated: %v", err)
	}
	if len(results) != 1 || results[0].Port != "loop" || results[0].Datagram.Payload.Value != "x" {
		t.Fatalf("unrelated trigger should start its own fresh session, got %+v", results)
	}
}

// --- live-Deployment test proving the loop-back wire genuinely works
// through the engine (not just by manually constructing NewCaused in a
// unit test). ---

type loopTestTrigger struct{ items []any }

func (s *loopTestTrigger) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	d := datagram.New(datagram.Source{NodeID: "trigger"}, datagram.Payload{Value: s.items})
	return emit("out", d)
}

func newLoopTestTrigger(json.RawMessage) (any, error) {
	return &loopTestTrigger{items: []any{1.0, 2.0, 3.0}}, nil
}

type loopTestDouble struct{}

func (loopTestDouble) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	v, _ := in.Payload.Value.(float64)
	out := datagram.NewCaused(in, datagram.Source{NodeID: "double"}, datagram.Payload{Value: v * 2})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func newLoopTestDouble(json.RawMessage) (any, error) { return loopTestDouble{}, nil }

var loopTestDoneChan = make(chan map[string]any, 10)

type loopTestDoneSink struct{}

func (loopTestDoneSink) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	select {
	case loopTestDoneChan <- in.Payload.Value.(map[string]any):
	case <-ctx.Done():
	}
	return nil, nil
}

func newLoopTestDoneSink(json.RawMessage) (any, error) { return loopTestDoneSink{}, nil }

func init() {
	flow.Register("loop-test-trigger", flow.NodeTypeInfo{Kind: flow.KindSource, Outputs: []string{"out"}}, newLoopTestTrigger)
	flow.Register("loop-test-double", flow.NodeTypeInfo{Kind: flow.KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, newLoopTestDouble)
	flow.Register("loop-test-done-sink", flow.NodeTypeInfo{Kind: flow.KindProcessor, Inputs: []string{"in"}}, newLoopTestDoneSink)
}

func TestPROC340_LoopBackWiringWorksUnderLiveDeployment(t *testing.T) {
	f := &flow.FlowFile{
		FormatVersion: 1, Kind: flow.KindFlow, ID: "loop-test-flow", Name: "t", Mode: flow.ModeStreaming,
		Graph: flow.Graph{
			Nodes: []flow.Node{
				{ID: "trigger", Type: "loop-test-trigger"},
				{ID: "lp", Type: "loop"},
				{ID: "dbl", Type: "loop-test-double"},
				{ID: "sink", Type: "loop-test-done-sink"},
			},
			Wires: []flow.Wire{
				{ID: "w1", From: flow.Endpoint{Node: "trigger", Port: "out"}, To: flow.Endpoint{Node: "lp", Port: "in"}},
				{ID: "w2", From: flow.Endpoint{Node: "lp", Port: "loop"}, To: flow.Endpoint{Node: "dbl", Port: "in"}},
				{ID: "w3", From: flow.Endpoint{Node: "dbl", Port: "out"}, To: flow.Endpoint{Node: "lp", Port: "in"}},
				{ID: "w4", From: flow.Endpoint{Node: "lp", Port: "done"}, To: flow.Endpoint{Node: "sink", Port: "in"}},
			},
		},
	}
	dep := flow.NewDeployment(nil)
	defer dep.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dep.Deploy(ctx, f); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	select {
	case done := <-loopTestDoneChan:
		if done["count"] != float64(3) {
			t.Errorf("done = %+v, want count=3", done)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("loop did not complete within timeout")
	}
}
