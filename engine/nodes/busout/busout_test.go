package busout

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/topics"
)

func TestSNK220_NewRequiresTopic(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error when topic is missing")
	}
}

func TestSNK220_ProcessPublishesAndPassesThrough(t *testing.T) {
	raw, err := json.Marshal(Config{Topic: "test/busout/publish", Tags: map[string]string{"site": "A"}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	wire, cancel := topics.DefaultBroker.Subscribe("test/busout/+", map[string]string{"site": "A"}, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer cancel()

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 42})
	results, err := proc.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("expected a single passthrough output on 'out', got %+v", results)
	}
	if v, _ := results[0].Datagram.Payload.Value.(int); v != 42 {
		t.Errorf("passthrough payload = %v, want 42 unchanged", results[0].Datagram.Payload.Value)
	}

	ctx, done := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer done()
	published, err := wire.Receive(ctx)
	if err != nil {
		t.Fatal("expected the subscriber to receive the published datagram")
	}
	if v, _ := published.Payload.Value.(int); v != 42 {
		t.Errorf("published payload = %v, want 42", published.Payload.Value)
	}
}

func TestSNK220_ProcessDoesNotPublishToNonMatchingSubscriber(t *testing.T) {
	raw, err := json.Marshal(Config{Topic: "test/busout/other"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	wire, cancel := topics.DefaultBroker.Subscribe("test/busout/unrelated", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer cancel()

	if _, err := proc.Process(context.Background(), datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})); err != nil {
		t.Fatalf("Process: %v", err)
	}

	ctx, done := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer done()
	if _, err := wire.Receive(ctx); err == nil {
		t.Fatal("expected no delivery to a non-matching topic subscriber")
	}
}
