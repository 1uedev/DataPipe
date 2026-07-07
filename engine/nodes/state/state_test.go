package state

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/ctxstore"
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

func TestPROC410_NewValidatesConfig(t *testing.T) {
	cases := []Config{
		{Scope: "bogus", Operations: []Operation{{Op: "get", Key: "x"}}},
		{Scope: "global"}, // no operations
		{Scope: "global", Operations: []Operation{{Op: "bogus", Key: "x"}}},
		{Scope: "global", Operations: []Operation{{Op: "get"}}}, // no key
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

func TestPROC410_ProcessRequiresContextStore(t *testing.T) {
	n := newTestNode(t, Config{Scope: "global", Operations: []Operation{{Op: "get", Key: "x", As: "x"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{}})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error: no context store attached")
	}
}

func TestPROC410_GlobalScopeSetThenGet(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)

	setNode := newTestNode(t, Config{Scope: "global", Operations: []Operation{
		{Op: "set", Key: "greeting", Value: json.RawMessage(`"hello"`)},
	}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{}})
	if _, err := setNode.Process(ctx, in); err != nil {
		t.Fatalf("set: %v", err)
	}

	getNode := newTestNode(t, Config{Scope: "global", Operations: []Operation{
		{Op: "get", Key: "greeting", As: "greeting"},
	}})
	results, err := getNode.Process(ctx, in)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if results[0].Datagram.Payload.Value.(map[string]any)["greeting"] != "hello" {
		t.Errorf("payload = %+v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC410_SetValueSupportsExpression(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)

	setNode := newTestNode(t, Config{Scope: "global", Operations: []Operation{
		{Op: "set", Key: "doubled", Value: json.RawMessage(`"={{payload.x * 2}}"`)},
	}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"x": 21.0}})
	if _, err := setNode.Process(ctx, in); err != nil {
		t.Fatalf("set: %v", err)
	}

	v, found, err := store.Get(context.Background(), ctxstore.Key{Scope: ctxstore.ScopeGlobal, Name: "doubled"})
	if err != nil || !found {
		t.Fatalf("Get: found=%v err=%v", found, err)
	}
	if asFloat(t, v) != 42 {
		t.Errorf("stored value = %v, want 42", v)
	}
}

func TestPROC410_IncrementDefaultsToOne(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)
	n := newTestNode(t, Config{Scope: "global", Operations: []Operation{{Op: "increment", Key: "count", As: "count"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{}})

	for i := 1; i <= 3; i++ {
		results, err := n.Process(ctx, in)
		if err != nil {
			t.Fatalf("increment %d: %v", i, err)
		}
		if got := asFloat(t, results[0].Datagram.Payload.Value.(map[string]any)["count"]); got != float64(i) {
			t.Errorf("increment %d: count = %v, want %d", i, got, i)
		}
	}
}

func TestPROC410_IncrementWithExplicitDelta(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)
	n := newTestNode(t, Config{Scope: "global", Operations: []Operation{
		{Op: "increment", Key: "total", Value: json.RawMessage(`"={{payload.amount}}"`), As: "total"},
	}})

	in1 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"amount": 5.0}})
	results, err := n.Process(ctx, in1)
	if err != nil {
		t.Fatalf("increment 1: %v", err)
	}
	if asFloat(t, results[0].Datagram.Payload.Value.(map[string]any)["total"]) != 5 {
		t.Fatalf("total after +5 = %v", results[0].Datagram.Payload.Value)
	}

	in2 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"amount": 3.0}})
	results, err = n.Process(ctx, in2)
	if err != nil {
		t.Fatalf("increment 2: %v", err)
	}
	if asFloat(t, results[0].Datagram.Payload.Value.(map[string]any)["total"]) != 8 {
		t.Fatalf("total after +5+3 = %v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC410_DeleteIsHarmlessWhenKeyMissing(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)
	n := newTestNode(t, Config{Scope: "global", Operations: []Operation{{Op: "delete", Key: "never-set"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{}})
	if _, err := n.Process(ctx, in); err != nil {
		t.Fatalf("delete of a missing key should not error: %v", err)
	}
}

func TestPROC410_MultipleOperationsAppliedInOrder(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)
	n := newTestNode(t, Config{Scope: "global", Operations: []Operation{
		{Op: "set", Key: "a", Value: json.RawMessage(`1`)},
		{Op: "increment", Key: "a", As: "afterIncrement"},
		{Op: "get", Key: "a", As: "final"},
	}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{}})
	results, err := n.Process(ctx, in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	if asFloat(t, m["afterIncrement"]) != 2 || asFloat(t, m["final"]) != 2 {
		t.Errorf("payload = %+v", m)
	}
}

// --- live-Deployment test proving node-scoped state is correctly keyed per
// (flowID, nodeID) and persists across invocations. ---

type stateTestTickSource struct{ n int }

func (s *stateTestTickSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	for i := 0; i < s.n; i++ {
		if err := emit("out", datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{Value: map[string]any{}})); err != nil {
			return err
		}
	}
	return nil
}

func newStateTestTickSource(json.RawMessage) (any, error) { return &stateTestTickSource{n: 3}, nil }

var stateTestSinkChan = make(chan float64, 10)

type stateTestSink struct{}

func (stateTestSink) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	v := asFloatNoTest(in.Payload.Value.(map[string]any)["count"])
	select {
	case stateTestSinkChan <- v:
	case <-ctx.Done():
	}
	return nil, nil
}

func newStateTestSink(json.RawMessage) (any, error) { return stateTestSink{}, nil }

func asFloatNoTest(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	default:
		return -1
	}
}

func init() {
	flow.Register("state-test-tick-source", flow.NodeTypeInfo{Kind: flow.KindSource, Outputs: []string{"out"}}, newStateTestTickSource)
	flow.Register("state-test-sink", flow.NodeTypeInfo{Kind: flow.KindProcessor, Inputs: []string{"in"}}, newStateTestSink)
}

func TestPROC410_NodeScopedStatePersistsAcrossInvocationsUnderLiveDeployment(t *testing.T) {
	cfg, err := json.Marshal(Config{Scope: "node", Operations: []Operation{{Op: "increment", Key: "count", As: "count"}}})
	if err != nil {
		t.Fatal(err)
	}
	f := &flow.FlowFile{
		FormatVersion: 1, Kind: flow.KindFlow, ID: "state-test-flow", Name: "t", Mode: flow.ModeStreaming,
		Graph: flow.Graph{
			Nodes: []flow.Node{
				{ID: "src", Type: "state-test-tick-source"},
				{ID: "st", Type: "state", Config: cfg},
				{ID: "snk", Type: "state-test-sink"},
			},
			Wires: []flow.Wire{
				{ID: "w1", From: flow.Endpoint{Node: "src", Port: "out"}, To: flow.Endpoint{Node: "st", Port: "in"}},
				{ID: "w2", From: flow.Endpoint{Node: "st", Port: "out"}, To: flow.Endpoint{Node: "snk", Port: "in"}},
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

	var last float64
	for i := 0; i < 3; i++ {
		select {
		case v := <-stateTestSinkChan:
			last = v
		case <-time.After(2 * time.Second):
			t.Fatalf("did not receive tick %d in time", i)
		}
	}
	if last != 3 {
		t.Errorf("final count = %v, want 3 (node-scoped state must persist across invocations)", last)
	}
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
