package flow

import (
	"context"
	"fmt"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// PortDatagram pairs a datagram with the output port it should be sent on,
// letting a Processor address multiple output ports (e.g. switch/filter).
type PortDatagram struct {
	Port     string
	Datagram datagram.Datagram
}

// Source is implemented by node types with no input port: they drive their
// own loop and emit datagrams until ctx is cancelled (e.g. inject, and later
// the real connectors of Increment 6). emit delivers to the named output
// port; a Source with a single output conventionally uses "out".
type Source interface {
	Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error
}

// Processor is implemented by node types with exactly one input port: one
// datagram in, zero or more (port, datagram) results out. The engine (not
// the node) is responsible for panic recovery (ARC-150) and error-policy
// application (ERR-100), so Process can simply return an error.
type Processor interface {
	Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error)
}

// NodeError is the ERR-100 error object: "message, code, node, stack,
// attempt" carried alongside the original datagram in error routing.
type NodeError struct {
	Message string
	Code    string
	Node    string
	Stack   string
	Attempt int
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("node %s: %s (attempt %d)", e.Node, e.Message, e.Attempt)
}
