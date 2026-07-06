package bus

import (
	"context"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// FanOut delivers an independent copy of each datagram to every wired
// output (BUS-140: "one output wired to n inputs delivers an independent
// copy").
type FanOut struct {
	wires     []*Wire
	threshold int
}

// NewFanOut wires the given outputs together. threshold is the DGM-120
// binary-by-reference size threshold applied when cloning per destination;
// use datagram.DefaultBinaryRefThreshold unless a flow overrides it.
func NewFanOut(threshold int, wires ...*Wire) *FanOut {
	return &FanOut{wires: wires, threshold: threshold}
}

// Send clones dgm once per wire and delivers each copy according to that
// wire's own overflow policy. It returns on the first hard error (context
// cancellation or a closed wire); drops are recorded in each wire's metrics
// rather than surfaced as errors.
func (f *FanOut) Send(ctx context.Context, dgm datagram.Datagram) error {
	for _, w := range f.wires {
		if _, err := w.Send(ctx, dgm.Clone(f.threshold)); err != nil {
			return err
		}
	}
	return nil
}
