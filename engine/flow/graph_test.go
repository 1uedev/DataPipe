package flow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// --- test-only node types for graph/hot-deploy tests ---

type graphTestEmitter struct{ intervalMs int }

func (s *graphTestEmitter) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	ticker := time.NewTicker(time.Duration(s.intervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			d := datagram.New(datagram.Source{NodeID: "emitter"}, datagram.Payload{Value: float64(1)})
			if err := emit("out", d); err != nil {
				return err
			}
		}
	}
}

func newGraphTestEmitter(raw json.RawMessage) (any, error) {
	var cfg struct {
		IntervalMs int `json:"intervalMs"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
	}
	if cfg.IntervalMs <= 0 {
		cfg.IntervalMs = 5
	}
	return &graphTestEmitter{intervalMs: cfg.IntervalMs}, nil
}

type graphTestTransform struct{ addend float64 }

func (p *graphTestTransform) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	v, _ := in.Payload.Value.(float64)
	out := datagram.NewCaused(in, datagram.Source{NodeID: "transform"}, datagram.Payload{Value: v + p.addend})
	return []PortDatagram{{Port: "out", Datagram: out}}, nil
}

func newGraphTestTransform(raw json.RawMessage) (any, error) {
	var cfg struct {
		Addend float64 `json:"addend"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &graphTestTransform{addend: cfg.Addend}, nil
}

var (
	graphTestSinkMu       sync.Mutex
	graphTestSinkChannels = map[string]chan float64{}
)

func graphTestSinkChannel(key string) chan float64 {
	graphTestSinkMu.Lock()
	defer graphTestSinkMu.Unlock()
	if ch, ok := graphTestSinkChannels[key]; ok {
		return ch
	}
	ch := make(chan float64, 100)
	graphTestSinkChannels[key] = ch
	return ch
}

type graphTestSink struct{ ch chan float64 }

func (s *graphTestSink) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	v, _ := in.Payload.Value.(float64)
	select {
	case s.ch <- v:
	case <-ctx.Done():
	}
	return nil, nil
}

func newGraphTestSink(raw json.RawMessage) (any, error) {
	var cfg struct {
		SinkKey string `json:"sinkKey"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &graphTestSink{ch: graphTestSinkChannel(cfg.SinkKey)}, nil
}

func init() {
	Register("graph-test-emitter", NodeTypeInfo{Kind: KindSource, Outputs: []string{"out"}}, newGraphTestEmitter)
	Register("graph-test-transform", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, newGraphTestTransform)
	Register("graph-test-sink", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newGraphTestSink)
}

func rawConfig(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	return raw
}

func threeNodePipeline(t *testing.T, addend float64, sinkKey string) *FlowFile {
	return &FlowFile{
		FormatVersion: 1,
		Kind:          KindFlow,
		ID:            "flow_test",
		Name:          "test",
		Mode:          ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "n1", Type: "graph-test-emitter", TypeVersion: 1, Config: rawConfig(t, map[string]any{"intervalMs": 5})},
				{ID: "n2", Type: "graph-test-transform", TypeVersion: 1, Config: rawConfig(t, map[string]any{"addend": addend})},
				{ID: "n3", Type: "graph-test-sink", TypeVersion: 1, Config: rawConfig(t, map[string]any{"sinkKey": sinkKey})},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "n1", Port: "out"}, To: Endpoint{Node: "n2", Port: "in"}},
				{ID: "w2", From: Endpoint{Node: "n2", Port: "out"}, To: Endpoint{Node: "n3", Port: "in"}},
			},
		},
	}
}

func drainUntil(t *testing.T, ch chan float64, want float64, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case v := <-ch:
			if v == want {
				return
			}
		case <-deadline:
			t.Fatalf("did not see value %v on sink channel within %v", want, timeout)
		}
	}
}

func TestENG140_DeployRunsInjectSetDebugStylePipeline(t *testing.T) {
	sinkKey := "pipeline-basic"
	f := threeNodePipeline(t, 10, sinkKey)

	d := NewDeployment(testLogger())
	defer d.Stop()
	if err := d.Deploy(context.Background(), f); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	// emitter emits 1, transform adds 10 -> 11 at the sink.
	drainUntil(t, graphTestSinkChannel(sinkKey), 11, 2*time.Second)
}

func TestENG140_HotDeployRestartsOnlyModifiedNodes(t *testing.T) {
	sinkKey := "pipeline-hotdeploy"
	v1 := threeNodePipeline(t, 10, sinkKey)

	d := NewDeployment(testLogger())
	defer d.Stop()
	ctx := context.Background()

	if err := d.Deploy(ctx, v1); err != nil {
		t.Fatalf("Deploy v1: %v", err)
	}
	drainUntil(t, graphTestSinkChannel(sinkKey), 11, 2*time.Second)

	n1Before, _ := d.NodeStats("n1")
	n2Before, _ := d.NodeStats("n2")
	n3Before, _ := d.NodeStats("n3")
	if n1Before.StartCount != 1 || n2Before.StartCount != 1 || n3Before.StartCount != 1 {
		t.Fatalf("initial start counts = n1:%d n2:%d n3:%d, want 1/1/1", n1Before.StartCount, n2Before.StartCount, n3Before.StartCount)
	}

	// v2 changes only n2's config (addend 10 -> 20); wiring is identical.
	v2 := threeNodePipeline(t, 20, sinkKey)
	if err := d.Deploy(ctx, v2); err != nil {
		t.Fatalf("Deploy v2: %v", err)
	}

	// The pipeline must keep delivering, now with the new addend applied.
	drainUntil(t, graphTestSinkChannel(sinkKey), 21, 2*time.Second)

	n1After, _ := d.NodeStats("n1")
	n2After, _ := d.NodeStats("n2")
	n3After, _ := d.NodeStats("n3")

	if n1After.StartCount != 1 {
		t.Errorf("n1 (untouched) StartCount = %d, want 1 (it must keep running, not restart)", n1After.StartCount)
	}
	if n3After.StartCount != 1 {
		t.Errorf("n3 (untouched) StartCount = %d, want 1 (it must keep running, not restart)", n3After.StartCount)
	}
	if n2After.StartCount != 2 {
		t.Errorf("n2 (modified) StartCount = %d, want 2 (it must have restarted)", n2After.StartCount)
	}
}

func TestENG140_RemovedNodeIsStopped(t *testing.T) {
	sinkKey := "pipeline-remove"
	v1 := threeNodePipeline(t, 10, sinkKey)

	d := NewDeployment(testLogger())
	defer d.Stop()
	ctx := context.Background()
	if err := d.Deploy(ctx, v1); err != nil {
		t.Fatalf("Deploy v1: %v", err)
	}
	drainUntil(t, graphTestSinkChannel(sinkKey), 11, 2*time.Second)

	// v2 drops n3 (and its wire) entirely: just n1 -> n2.
	v2 := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_test", Name: "test", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{v1.Graph.Nodes[0], v1.Graph.Nodes[1]},
			Wires: []Wire{v1.Graph.Wires[0]},
		},
	}
	if err := d.Deploy(ctx, v2); err != nil {
		t.Fatalf("Deploy v2: %v", err)
	}

	if _, running := d.NodeStats("n3"); running {
		t.Error("n3 should have been stopped after being removed from the flow")
	}
	if _, running := d.NodeStats("n1"); !running {
		t.Error("n1 should still be running")
	}
}
