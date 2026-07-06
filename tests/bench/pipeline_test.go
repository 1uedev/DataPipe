package bench

import (
	"context"
	"testing"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
)

// TestThreeNodePipelineCorrectness proves the pipeline BenchmarkThreeNodePipeline
// measures actually delivers every datagram, in order, with lineage intact,
// before we trust its throughput number.
func TestThreeNodePipelineCorrectness(t *testing.T) {
	ctx := context.Background()
	sourceToProc := bus.NewWire(bus.WireConfig{Capacity: 16, Overflow: bus.OverflowBlock})
	procToSink := bus.NewWire(bus.WireConfig{Capacity: 16, Overflow: bus.OverflowBlock})

	go func() {
		for {
			in, err := sourceToProc.Receive(ctx)
			if err != nil {
				return
			}
			out := datagram.NewCaused(in, datagram.Source{NodeID: "processor"}, in.Payload)
			if _, err := procToSink.Send(ctx, out); err != nil {
				return
			}
		}
	}()

	const n = 1000
	source := datagram.Source{NodeID: "source"}
	sent := make([]datagram.Datagram, n)

	// The source feed must run concurrently with draining the sink: with
	// bounded, blocking wires a "send everything, then receive everything"
	// sequence deadlocks as soon as the pipeline's combined capacity fills
	// up (BUS-110 backpressure working as intended).
	go func() {
		for i := 0; i < n; i++ {
			d := datagram.New(source, datagram.Payload{Value: i})
			sent[i] = d
			if _, err := sourceToProc.Send(ctx, d); err != nil {
				t.Errorf("Send(%d): %v", i, err)
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		out, err := procToSink.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive(%d): %v", i, err)
		}
		if v, ok := out.Payload.Value.(int); !ok || v != i {
			t.Fatalf("out of order or wrong value at %d: got %v", i, out.Payload.Value)
		}
		if out.Header.CausationID != sent[i].Header.ID {
			t.Errorf("lineage broken at %d: causationId=%q want %q", i, out.Header.CausationID, sent[i].Header.ID)
		}
		if out.Header.CorrelationID != sent[i].Header.CorrelationID {
			t.Errorf("correlation id not propagated at %d", i)
		}
	}
}
