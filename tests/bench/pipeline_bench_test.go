// Package bench holds the cross-package benchmark suite (NFR-100). This
// file establishes the Increment 1 baseline: a 3-node in-process pipeline
// (source -> processor -> sink) wired together purely with engine/bus.Wire,
// targeting >= 50k datagrams/s end to end (Development-Plan Increment 1
// "Done when"). The regression gate (fail on >10% drop) activates from
// Increment 6 per the working agreements; for now this just reports ns/op.
package bench

import (
	"context"
	"testing"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
)

func BenchmarkThreeNodePipeline(b *testing.B) {
	ctx := context.Background()
	sourceToProc := bus.NewWire(bus.WireConfig{Capacity: 1024, Overflow: bus.OverflowBlock})
	procToSink := bus.NewWire(bus.WireConfig{Capacity: 1024, Overflow: bus.OverflowBlock})

	processorDone := make(chan struct{})
	go func() {
		defer close(processorDone)
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

	sinkDone := make(chan struct{})
	go func() {
		defer close(sinkDone)
		for i := 0; i < b.N; i++ {
			if _, err := procToSink.Receive(ctx); err != nil {
				return
			}
		}
	}()

	source := datagram.Source{NodeID: "source"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d := datagram.New(source, datagram.Payload{Value: i})
		if _, err := sourceToProc.Send(ctx, d); err != nil {
			b.Fatalf("Send: %v", err)
		}
	}
	<-sinkDone
	b.StopTimer()

	sourceToProc.Close()
	<-processorDone

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "dgm/s")
}
