package flow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// ExecuteNode runs one Processor node type in isolation against a single
// input datagram, for design-time "run once" execution (DBG-130). It is a
// pure, in-process call — no Deployment, no goroutine, no wires — so
// callers outside the runtime process (e.g. the control plane) can use it
// directly as long as the node package is linked in. Panics inside the node
// are recovered (ARC-150), same as the live runner.
//
// Source nodes are out of scope: a Source drives its own loop and produces
// its own data, so "pin a sample input and run once" doesn't apply to it
// the way it does to a Processor; DBG-130 is scoped accordingly.
func ExecuteNode(ctx context.Context, nodeType string, config json.RawMessage, input datagram.Datagram) ([]PortDatagram, error) {
	info, factory, ok := Lookup(nodeType)
	if !ok {
		return nil, fmt.Errorf("flow: unknown node type %q", nodeType)
	}
	if info.Kind != KindProcessor {
		return nil, fmt.Errorf("flow: node type %q is a Source; design-time execution only supports Processor nodes (DBG-130)", nodeType)
	}
	instance, err := factory(config)
	if err != nil {
		return nil, fmt.Errorf("flow: configuring node %q: %w", nodeType, err)
	}
	proc, ok := instance.(Processor)
	if !ok {
		return nil, fmt.Errorf("flow: node type %q factory did not return a Processor", nodeType)
	}
	return invokeWithRecover(ctx, proc, input)
}
