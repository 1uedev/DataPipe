package flow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// --- test-only DynamicOutputs node: routes to whichever port its config
// names, proving Validate/Deploy use the instance's real ports rather than
// a static registered list. ---

type dynRouterNode struct{ port string }

func (n *dynRouterNode) OutputPorts() []string { return []string{n.port} }

func (n *dynRouterNode) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	return []PortDatagram{{Port: n.port, Datagram: in}}, nil
}

func newDynRouterNode(raw json.RawMessage) (any, error) {
	var cfg struct {
		Port string `json:"port"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Port == "" {
		return nil, errors.New("flow-test-dyn-router: port is required")
	}
	return &dynRouterNode{port: cfg.Port}, nil
}

func init() {
	// Registered with NO static Outputs — every port comes from
	// DynamicOutputs, mirroring switch/route's out0..outN+default.
	Register("flow-test-dyn-router", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newDynRouterNode)
}

func TestPROC300_ValidateAcceptsWireToADynamicOutputPort(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_dyn", Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "n1", Type: "graph-test-emitter", TypeVersion: 1, Config: rawConfig(t, map[string]any{"intervalMs": 5})},
				{ID: "n2", Type: "flow-test-dyn-router", TypeVersion: 1, Config: rawConfig(t, map[string]any{"port": "outQuux"})},
				{ID: "n3", Type: "graph-test-sink", TypeVersion: 1, Config: rawConfig(t, map[string]any{"sinkKey": "dyn-validate"})},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "n1", Port: "out"}, To: Endpoint{Node: "n2", Port: "in"}},
				{ID: "w2", From: Endpoint{Node: "n2", Port: "outQuux"}, To: Endpoint{Node: "n3", Port: "in"}},
			},
		},
	}
	if err := Validate(f); err != nil {
		t.Fatalf("Validate: %v, want the dynamic port \"outQuux\" to be accepted", err)
	}
}

func TestPROC300_ValidateRejectsUnknownPortEvenForDynamicNode(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_dyn2", Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "n1", Type: "graph-test-emitter", TypeVersion: 1, Config: rawConfig(t, map[string]any{"intervalMs": 5})},
				{ID: "n2", Type: "flow-test-dyn-router", TypeVersion: 1, Config: rawConfig(t, map[string]any{"port": "outQuux"})},
				{ID: "n3", Type: "graph-test-sink", TypeVersion: 1, Config: rawConfig(t, map[string]any{"sinkKey": "dyn-validate-reject"})},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "n1", Port: "out"}, To: Endpoint{Node: "n2", Port: "in"}},
				{ID: "w2", From: Endpoint{Node: "n2", Port: "notTheConfiguredPort"}, To: Endpoint{Node: "n3", Port: "in"}},
			},
		},
	}
	if err := Validate(f); err == nil {
		t.Fatal("expected Validate to reject a wire from a port the instance didn't declare")
	}
}

func TestPROC300_DeployRoutesToDynamicOutputPort(t *testing.T) {
	sinkKey := "dyn-deploy"
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_dyn3", Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "n1", Type: "graph-test-emitter", TypeVersion: 1, Config: rawConfig(t, map[string]any{"intervalMs": 5})},
				{ID: "n2", Type: "flow-test-dyn-router", TypeVersion: 1, Config: rawConfig(t, map[string]any{"port": "outQuux"})},
				{ID: "n3", Type: "graph-test-sink", TypeVersion: 1, Config: rawConfig(t, map[string]any{"sinkKey": sinkKey})},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "n1", Port: "out"}, To: Endpoint{Node: "n2", Port: "in"}},
				{ID: "w2", From: Endpoint{Node: "n2", Port: "outQuux"}, To: Endpoint{Node: "n3", Port: "in"}},
			},
		},
	}
	d := NewDeployment(testLogger())
	defer d.Stop()
	if err := d.Deploy(context.Background(), f); err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	drainUntil(t, graphTestSinkChannel(sinkKey), 1, 2*time.Second)
}

// --- test-only MultiInputProcessor node: tags every datagram with which
// port it arrived on, proving Deployment starts one receive-loop per
// declared input and dispatches through ProcessPort correctly. ---

type multiTagNode struct{}

func (n *multiTagNode) ProcessPort(ctx context.Context, port string, in datagram.Datagram) ([]PortDatagram, error) {
	v, _ := in.Payload.Value.(float64)
	out := datagram.NewCaused(in, datagram.Source{NodeID: "multi"}, datagram.Payload{Value: map[string]any{"port": port, "value": v}})
	return []PortDatagram{{Port: "out", Datagram: out}}, nil
}

func newMultiTagNode(json.RawMessage) (any, error) { return &multiTagNode{}, nil }

func init() {
	Register("flow-test-multi-tag", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"a", "b"}, Outputs: []string{"out"}}, newMultiTagNode)
}

type multiTagSink struct{ ch chan map[string]any }

func (s *multiTagSink) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	v, _ := in.Payload.Value.(map[string]any)
	select {
	case s.ch <- v:
	case <-ctx.Done():
	}
	return nil, nil
}

var multiTagSinkChan = make(chan map[string]any, 100)

func newMultiTagSink(json.RawMessage) (any, error) { return &multiTagSink{ch: multiTagSinkChan}, nil }

func init() {
	Register("flow-test-multi-tag-sink", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newMultiTagSink)
}

func TestPROC320_MultiInputProcessorReceivesFromEachNamedPort(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_multi", Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "e1", Type: "graph-test-emitter", TypeVersion: 1, Config: rawConfig(t, map[string]any{"intervalMs": 5})},
				{ID: "e2", Type: "graph-test-emitter", TypeVersion: 1, Config: rawConfig(t, map[string]any{"intervalMs": 5})},
				{ID: "m1", Type: "flow-test-multi-tag", TypeVersion: 1, Config: rawConfig(t, map[string]any{})},
				{ID: "s1", Type: "flow-test-multi-tag-sink", TypeVersion: 1, Config: rawConfig(t, map[string]any{})},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "e1", Port: "out"}, To: Endpoint{Node: "m1", Port: "a"}},
				{ID: "w2", From: Endpoint{Node: "e2", Port: "out"}, To: Endpoint{Node: "m1", Port: "b"}},
				{ID: "w3", From: Endpoint{Node: "m1", Port: "out"}, To: Endpoint{Node: "s1", Port: "in"}},
			},
		},
	}
	d := NewDeployment(testLogger())
	defer d.Stop()
	if err := d.Deploy(context.Background(), f); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	seenA, seenB := false, false
	deadline := time.After(2 * time.Second)
	for !seenA || !seenB {
		select {
		case v := <-multiTagSinkChan:
			switch v["port"] {
			case "a":
				seenA = true
			case "b":
				seenB = true
			}
		case <-deadline:
			t.Fatalf("did not observe datagrams tagged with both ports within timeout (seenA=%v seenB=%v)", seenA, seenB)
		}
	}
}
