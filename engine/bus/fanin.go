package bus

import (
	"context"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// FanIn merges n output wires into a single input (BUS-140: "fan-in
// interleaves in arrival order"): one goroutine per source wire forwards
// into a shared buffered channel, so datagrams surface in the order they
// actually become ready rather than in strict round-robin.
type FanIn struct {
	out    chan datagram.Datagram
	cancel context.CancelFunc
}

// NewFanIn starts merging the given wires. Call Close when done to stop the
// forwarding goroutines.
func NewFanIn(ctx context.Context, bufferSize int, wires ...*Wire) *FanIn {
	ctx, cancel := context.WithCancel(ctx)
	f := &FanIn{out: make(chan datagram.Datagram, bufferSize), cancel: cancel}

	for _, w := range wires {
		go func(w *Wire) {
			for {
				dgm, err := w.Receive(ctx)
				if err != nil {
					return
				}
				select {
				case f.out <- dgm:
				case <-ctx.Done():
					return
				}
			}
		}(w)
	}

	return f
}

// Receive returns the next datagram from any source wire, in arrival order.
func (f *FanIn) Receive(ctx context.Context) (datagram.Datagram, error) {
	select {
	case dgm := <-f.out:
		return dgm, nil
	case <-ctx.Done():
		return datagram.Datagram{}, ctx.Err()
	}
}

// Close stops all forwarding goroutines.
func (f *FanIn) Close() {
	f.cancel()
}
