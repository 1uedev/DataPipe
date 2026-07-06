package flow

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestCON110_ResolveConnectionWithNoContextErrors(t *testing.T) {
	if _, err := ResolveConnection(context.Background()); err == nil {
		t.Fatal("expected an error when no connection context is present")
	}
}

func TestCON110_ResolveConnectionWithNoConnectionConfiguredErrors(t *testing.T) {
	ctx := WithConnection(context.Background(), NoopConnectionResolver, "")
	if _, err := ResolveConnection(ctx); err == nil {
		t.Fatal("expected an error when the node has no connection configured")
	}
}

func TestCON110_NoopConnectionResolverErrorsClearly(t *testing.T) {
	ctx := WithConnection(context.Background(), NoopConnectionResolver, "conn-1")
	if _, err := ResolveConnection(ctx); err == nil {
		t.Fatal("expected NoopConnectionResolver to error")
	}
}

// fakeConnectionResolver records every connection id it was asked to
// resolve and returns a canned ConnectionInfo.
type fakeConnectionResolver struct {
	info ConnectionInfo
	err  error
	got  []string
}

func (f *fakeConnectionResolver) ResolveConnection(_ context.Context, connectionID string) (ConnectionInfo, error) {
	f.got = append(f.got, connectionID)
	return f.info, f.err
}

// connTestNode is a Processor that resolves its own connection and emits
// the result (or the error message) as its output payload, proving the
// context-injection wiring end to end under a real Deployment.
type connTestNode struct{}

func (connTestNode) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	info, err := ResolveConnection(ctx)
	var value map[string]any
	if err != nil {
		value = map[string]any{"error": err.Error()}
	} else {
		value = map[string]any{
			"type":       info.Type,
			"config":     string(info.Config),
			"credential": string(info.CredentialJSON),
		}
	}
	out := datagram.NewCaused(in, datagram.Source{NodeID: "conn-test"}, datagram.Payload{Value: value})
	return []PortDatagram{{Port: "out", Datagram: out}}, nil
}

func newConnTestNode(json.RawMessage) (any, error) { return connTestNode{}, nil }

func init() {
	Register("conn-test-node", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, newConnTestNode)
}

func TestCON110_NodeResolvesItsConfiguredConnectionUnderLiveDeployment(t *testing.T) {
	resolver := &fakeConnectionResolver{info: ConnectionInfo{
		Type:           "mqtt",
		Config:         json.RawMessage(`{"broker":"tcp://localhost:1883"}`),
		CredentialJSON: json.RawMessage(`{"username":"u","password":"p"}`),
	}}

	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "conn-test-flow", Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "src", Type: "debug-test-fast-source", Config: json.RawMessage(`{"n":1}`)},
				{ID: "ct", Type: "conn-test-node", Connection: "conn-42"},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "src", Port: "out"}, To: Endpoint{Node: "ct", Port: "in"}},
			},
		},
	}

	dep := NewDeployment(nil)
	defer dep.Stop()
	dep.SetConnectionResolver(resolver)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := dep.Deploy(ctx, f); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if stats, ok := dep.NodeStats("ct"); ok && stats.Metrics.Processed >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stats, ok := dep.NodeStats("ct"); !ok || stats.Metrics.Processed < 1 {
		t.Fatalf("conn-test-node never processed a datagram (stats=%+v, ok=%v)", stats, ok)
	}

	if len(resolver.got) == 0 || resolver.got[0] != "conn-42" {
		t.Fatalf("resolver.got = %+v, want [conn-42]", resolver.got)
	}
}
