package script

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

// asFloat tolerates goja's Export() returning int64 for whole-number
// results and float64 otherwise.
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

func TestPROC100_NewRequiresCode(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error: code is required")
	}
}

func TestPROC100_NewRejectsSyntaxError(t *testing.T) {
	if _, err := New(json.RawMessage(`{"code": "this is not }{ valid js"}`)); err == nil {
		t.Fatal("expected a compile error")
	}
}

func TestPROC100_ImplicitReturnEmitsOnFirstOutputPort(t *testing.T) {
	n := newTestNode(t, Config{Code: "payload.value * 2"})
	in := datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{Value: map[string]any{"value": 21.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("results = %+v", results)
	}
	if asFloat(t, results[0].Datagram.Payload.Value) != 42 {
		t.Errorf("payload = %v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC100_EmitProducesMultipleOutputPorts(t *testing.T) {
	n := newTestNode(t, Config{
		Code:    "emit('a', 1); emit('b', 2); emit('a', 3);",
		Outputs: []string{"a", "b"},
	})
	in := datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{Value: nil})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 emitted results, got %d: %+v", len(results), results)
	}
	if results[0].Port != "a" || results[1].Port != "b" || results[2].Port != "a" {
		t.Errorf("ports = %v, %v, %v", results[0].Port, results[1].Port, results[2].Port)
	}
}

func TestPROC300_OutputPortsReflectsConfig(t *testing.T) {
	n := newTestNode(t, Config{Code: "1", Outputs: []string{"x", "y", "z"}})
	got := n.OutputPorts()
	if len(got) != 3 || got[0] != "x" || got[1] != "y" || got[2] != "z" {
		t.Errorf("OutputPorts() = %v", got)
	}
}

func TestSEC150_NoFilesystemOrNetworkAccess(t *testing.T) {
	n := newTestNode(t, Config{Code: "typeof require === 'undefined' && typeof fetch === 'undefined' && typeof process === 'undefined'"})
	in := datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{Value: nil})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value != true {
		t.Errorf("expected no fs/network globals bound, got %v", results[0].Datagram.Payload.Value)
	}
}

func TestENG150_InfiniteLoopIsInterruptedByTimeout(t *testing.T) {
	n := newTestNode(t, Config{Code: "while(true) {}", TimeoutMs: 100})
	start := time.Now()
	_, err := n.Process(context.Background(), datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{}))
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Process took %v, expected to be interrupted promptly", elapsed)
	}
}

func TestPROC410_NodeStateGetSetRoundTrips(t *testing.T) {
	n := newTestNode(t, Config{Code: "state.set('seen', (state.get('seen') || 0) + 1); state.get('seen')"})
	in := datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{})

	// nodeStateObject requires a context store to actually persist;
	// without one, get() returns undefined and set() throws, so this test
	// documents (and asserts) the graceful degradation instead of silently
	// no-op'ing when the node runs outside a live Deployment.
	_, err := n.Process(context.Background(), in)
	if err == nil {
		t.Fatal("expected state.set to throw without a context store attached")
	}
	if !strings.Contains(err.Error(), "context store") {
		t.Errorf("error = %v, want it to mention the missing context store", err)
	}
}

// --- live-Deployment test proving node state persists across invocations
// and console output reaches the debug sidebar (mirrors debuglog's own
// SidebarEvent-under-live-Deployment pattern). ---

type scriptTestTickSource struct{ n int }

func (s *scriptTestTickSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	for i := 0; i < s.n; i++ {
		d := datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{Value: float64(i)})
		if err := emit("out", d); err != nil {
			return err
		}
	}
	return nil
}

func newScriptTestTickSource(json.RawMessage) (any, error) { return &scriptTestTickSource{n: 5}, nil }

type scriptTestSink struct{ ch chan any }

func (s *scriptTestSink) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	select {
	case s.ch <- in.Payload.Value:
	case <-ctx.Done():
	}
	return nil, nil
}

var scriptTestSinkChan = make(chan any, 100)

func newScriptTestSink(json.RawMessage) (any, error) {
	return &scriptTestSink{ch: scriptTestSinkChan}, nil
}

type sidebarCollector struct {
	mu     sync.Mutex
	events []flow.DebugEvent
}

func (c *sidebarCollector) Capture(e flow.DebugEvent) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}
func (c *sidebarCollector) WireMetrics(flow.WireMetricsSample) {}
func (c *sidebarCollector) snapshot() []flow.DebugEvent {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]flow.DebugEvent(nil), c.events...)
}

func init() {
	flow.Register("script-test-tick-source", flow.NodeTypeInfo{Kind: flow.KindSource, Outputs: []string{"out"}}, newScriptTestTickSource)
	flow.Register("script-test-sink", flow.NodeTypeInfo{Kind: flow.KindProcessor, Inputs: []string{"in"}}, newScriptTestSink)
}

func TestPROC410_NodeStatePersistsAcrossInvocationsUnderLiveDeployment(t *testing.T) {
	cfg, err := json.Marshal(Config{Code: "console.log('tick', payload); state.set('count', (state.get('count') || 0) + 1); state.get('count')"})
	if err != nil {
		t.Fatal(err)
	}
	f := &flow.FlowFile{
		FormatVersion: 1, Kind: flow.KindFlow, ID: "script-test-flow", Name: "t", Mode: flow.ModeStreaming,
		Graph: flow.Graph{
			Nodes: []flow.Node{
				{ID: "src", Type: "script-test-tick-source"},
				{ID: "scr", Type: "script", Config: cfg},
				{ID: "snk", Type: "script-test-sink"},
			},
			Wires: []flow.Wire{
				{ID: "w1", From: flow.Endpoint{Node: "src", Port: "out"}, To: flow.Endpoint{Node: "scr", Port: "in"}},
				{ID: "w2", From: flow.Endpoint{Node: "scr", Port: "out"}, To: flow.Endpoint{Node: "snk", Port: "in"}},
			},
		},
	}

	dep := flow.NewDeployment(nil)
	defer dep.Stop()
	collector := &sidebarCollector{}
	dep.SetDebugSink(collector)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dep.Deploy(ctx, f); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	var last float64
	for i := 0; i < 5; i++ {
		select {
		case v := <-scriptTestSinkChan:
			last = asFloat(t, v)
		case <-time.After(2 * time.Second):
			t.Fatalf("did not receive tick %d in time", i)
		}
	}
	if last != 5 {
		t.Errorf("final state.get('count') = %v, want 5 (node state must persist across invocations)", last)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		found := false
		for _, e := range collector.snapshot() {
			if e.Direction == flow.DirSidebar && e.Label == "console.log" {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("expected at least one console.log sidebar event")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
