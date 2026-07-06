package debuglog

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestDEBUGLOG_LogsLabelAndPayloadWithoutError(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	raw, err := json.Marshal(Config{Label: "my-debug", Console: true})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"temp": 42.5}})
	results, err := proc.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("debug-log is a sink, got %d output datagrams, want 0", len(results))
	}

	out := buf.String()
	if !strings.Contains(out, "my-debug") {
		t.Errorf("log output missing label:\n%s", out)
	}
	if !strings.Contains(out, in.Header.CorrelationID) {
		t.Errorf("log output missing correlation id:\n%s", out)
	}
}

func TestDEBUGLOG_ConsoleOffByDefaultProducesNoLogOutput(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	n, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})
	if _, err := proc.Process(context.Background(), in); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if buf.Len() != 0 {
		t.Errorf("expected no log output with console unset (sidebar-only by default, DBG-110), got:\n%s", buf.String())
	}
}

func TestDEBUGLOG110_ExpressionSelectsSubPathForSidebar(t *testing.T) {
	raw, err := json.Marshal(Config{Expression: "reading.temp"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{
		Value: map[string]any{"reading": map[string]any{"temp": 42.5, "unit": "C"}},
	})
	// SidebarEvent is a no-op without a live Deployment's debug context, but
	// Process must still succeed and produce no output datagrams (sink).
	results, err := proc.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("debug-log is a sink, got %d output datagrams, want 0", len(results))
	}
	if got := evalExpression(in.Payload.Value, "reading.temp"); got != 42.5 {
		t.Errorf("evalExpression(%q) = %v, want 42.5", "reading.temp", got)
	}
	if got := evalExpression(in.Payload.Value, ""); got == nil {
		t.Error("evalExpression with empty path should return the whole payload, got nil")
	}
	if got := evalExpression(in.Payload.Value, "reading.missing"); got != nil {
		t.Errorf("evalExpression for a missing key should return nil, got %v", got)
	}
}

// --- a minimal one-shot test source, used only to feed a real Deployment
// so SidebarEvent's context plumbing can be proven end to end. ---

type debuglogTestOnceSource struct{ value any }

func (s *debuglogTestOnceSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	d := datagram.New(datagram.Source{NodeID: "src"}, datagram.Payload{Value: s.value})
	return emit("out", d)
}

func newDebuglogTestOnceSource(json.RawMessage) (any, error) {
	return &debuglogTestOnceSource{value: map[string]any{"temp": 99.0}}, nil
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
	flow.Register("debuglog-test-once-source", flow.NodeTypeInfo{Kind: flow.KindSource, Outputs: []string{"out"}}, newDebuglogTestOnceSource)
}

// TestDEBUGLOG110_SidebarEventReachesDebugSinkUnderLiveDeployment proves the
// context-injected DebugSink plumbing (flow.SidebarEvent) actually delivers
// a DirSidebar event carrying the node's label and evaluated expression when
// the node runs inside a real Deployment — not just that it's a harmless
// no-op in isolation (already covered above).
func TestDEBUGLOG110_SidebarEventReachesDebugSinkUnderLiveDeployment(t *testing.T) {
	cfg, err := json.Marshal(Config{Label: "boiler-temp", Expression: "temp"})
	if err != nil {
		t.Fatal(err)
	}
	f := &flow.FlowFile{
		FormatVersion: 1, Kind: flow.KindFlow, ID: "sidebar-test-flow", Name: "t", Mode: flow.ModeStreaming,
		Graph: flow.Graph{
			Nodes: []flow.Node{
				{ID: "src", Type: "debuglog-test-once-source"},
				{ID: "dbg", Type: "debug-log", Config: cfg},
			},
			Wires: []flow.Wire{
				{ID: "w1", From: flow.Endpoint{Node: "src", Port: "out"}, To: flow.Endpoint{Node: "dbg", Port: "in"}},
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
		t.Fatalf("deploy failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var sidebarEvents []flow.DebugEvent
	for time.Now().Before(deadline) {
		for _, e := range collector.snapshot() {
			if e.Direction == flow.DirSidebar {
				sidebarEvents = append(sidebarEvents, e)
			}
		}
		if len(sidebarEvents) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(sidebarEvents) == 0 {
		t.Fatal("no sidebar debug event was captured within the deadline")
	}
	e := sidebarEvents[0]
	if e.Label != "boiler-temp" {
		t.Errorf("sidebar event label = %q, want %q", e.Label, "boiler-temp")
	}
	if e.NodeID != "dbg" {
		t.Errorf("sidebar event nodeId = %q, want %q", e.NodeID, "dbg")
	}
	if e.Value != 99.0 {
		t.Errorf("sidebar event value = %v, want 99.0 (expression should have selected the sub-path)", e.Value)
	}
}
