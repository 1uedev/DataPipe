// Package trycatch implements the "try-catch" node (PROC-370/ERR-110): a
// visual error scope that routes a wrapped node type's errors to a "catch"
// port instead of following that node's own error policy.
//
// This is a deliberate scope reduction from the spec's "visual error scope"
// (a UI grouping of arbitrarily many nodes sharing one catch port): flow
// visual grouping (UI-150) isn't built yet, so there is no editor concept
// of "these N nodes are one scope" to hang a shared catch port off of.
// Instead, try-catch wraps exactly ONE node type/config per instance,
// invoked in-process via engine/flow.ExecuteNode (the same panic-safe,
// no-live-Deployment call DBG-130's design-time execution already uses) —
// chain multiple try-catch nodes to cover multiple steps. Documented in
// TODO.md as a scope reduction, not a defect.
package trycatch

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"tryType": { "type": "string", "description": "The wrapped node type name (must be a registered Processor)." },
		"tryConfig": { "type": "object", "description": "The wrapped node type's own config object." }
	},
	"required": ["tryType", "tryConfig"]
}`

func init() {
	flow.Register("try-catch", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out", "catch"},
		DisplayName:  "Try/Catch",
		Category:     flow.CategoryControl,
		Description:  "Wraps one node type, routing its errors/panics to a \"catch\" port instead of its own error policy (PROC-370/ERR-110).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "try-catch" node's "config" object.
type Config struct {
	TryType   string          `json:"tryType"`
	TryConfig json.RawMessage `json:"tryConfig"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "try-catch" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.TryType == "" {
		return nil, fmt.Errorf("try-catch: tryType is required")
	}
	info, _, ok := flow.Lookup(cfg.TryType)
	if !ok {
		return nil, fmt.Errorf("try-catch: unknown node type %q (is the plugin installed?)", cfg.TryType)
	}
	if info.Kind != flow.KindProcessor {
		return nil, fmt.Errorf("try-catch: node type %q is a Source; only Processor node types can be wrapped", cfg.TryType)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	results, err := flow.ExecuteNode(ctx, n.cfg.TryType, n.cfg.TryConfig, in)
	if err != nil {
		nodeErr := flow.AsNodeError(err, n.cfg.TryType, 1)
		errDgm := flow.BuildErrorDatagram(in, nodeErr)
		return []flow.PortDatagram{{Port: "catch", Datagram: errDgm}}, nil
	}
	// Every result is remapped onto this node's own "out" port regardless of
	// which port the wrapped node emitted to — a wrapped multi-output node
	// type (e.g. "switch") collapses its port distinctions here; wrap
	// single-output node types for full fidelity.
	out := make([]flow.PortDatagram, len(results))
	for i, r := range results {
		out[i] = flow.PortDatagram{Port: "out", Datagram: r.Datagram}
	}
	return out, nil
}
