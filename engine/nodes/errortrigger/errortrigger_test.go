package errortrigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/topics"
)

func TestERR120_RunEmitsFreshRootFromPublishedError(t *testing.T) {
	instance, err := New(json.RawMessage(`{"flowId":"flow_target"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := instance.(*node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	emitted := make(chan datagram.Datagram, 1)
	go func() {
		_ = n.Run(ctx, func(port string, d datagram.Datagram) error {
			if port != "out" {
				t.Errorf("port = %q, want \"out\"", port)
			}
			emitted <- d
			return nil
		})
	}()

	// Give Run a moment to subscribe before publishing.
	time.Sleep(20 * time.Millisecond)

	original := datagram.New(datagram.Source{NodeID: "failing-node"}, datagram.Payload{Value: map[string]any{"x": 1}})
	errDgm := flow.BuildErrorDatagram(original, &flow.NodeError{Message: "boom", Node: "failing-node"})
	topics.DefaultBroker.Publish(context.Background(), flow.ErrorFlowTopic("flow_target"), nil, errDgm)

	select {
	case got := <-emitted:
		if got.Header.CorrelationID != got.Header.ID {
			t.Fatal("expected error-trigger to emit a fresh root datagram (new correlation chain), not a continuation of the original")
		}
		payload, _ := got.Payload.Value.(map[string]any)
		if payload == nil || payload["error"] == nil {
			t.Fatalf("payload = %+v, want the ERR-100 {original, error} shape", got.Payload.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("error-trigger never emitted the published error")
	}
}

func TestERR120_ProjectWideWildcardSubscribesToEveryFlow(t *testing.T) {
	instance, err := New(json.RawMessage(`{"flowId":"*"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := instance.(*node)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	emitted := make(chan datagram.Datagram, 1)
	go func() {
		_ = n.Run(ctx, func(port string, d datagram.Datagram) error {
			emitted <- d
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond)

	errDgm := flow.BuildErrorDatagram(datagram.New(datagram.Source{}, datagram.Payload{}), &flow.NodeError{Message: "boom"})
	topics.DefaultBroker.Publish(context.Background(), flow.ErrorFlowTopic("some_unrelated_flow"), nil, errDgm)

	select {
	case <-emitted:
	case <-time.After(2 * time.Second):
		t.Fatal("the \"*\" project-wide handler never received an error published for an unrelated flow id")
	}
}

func TestERR120_FlowIDIsRequired(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected New to reject a config with no flowId")
	}
}

func TestERR120_TriggerKindIsError(t *testing.T) {
	instance, err := New(json.RawMessage(`{"flowId":"x"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := instance.(*node)
	if got := n.TriggerKind(); got != "error" {
		t.Fatalf("TriggerKind() = %q, want \"error\"", got)
	}
}
