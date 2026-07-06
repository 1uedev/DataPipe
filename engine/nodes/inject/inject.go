// Package inject implements the "inject" node (CON-600 Manual Inject): it
// emits a configured datagram once, and optionally repeats on an interval —
// the editor's "inject button" itself is a later (Increment 4+) UI concern,
// this is the runtime behavior behind it.
package inject

import (
	"context"
	"encoding/json"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

// configSchema is a JSON Schema (draft 2020-12) for Config, letting the
// editor generate this node's config form (UI-170) rather than hand-build
// one.
const configSchema = `{
	"type": "object",
	"properties": {
		"payload": { "description": "The datagram payload to emit (any JSON value)." },
		"initialDelayMs": { "type": "integer", "minimum": 0, "default": 0, "description": "Delay before the first emit, in milliseconds." },
		"repeatMs": { "type": "integer", "minimum": 0, "default": 0, "description": "Re-fire on this interval; 0 fires once and stops." }
	}
}`

func init() {
	flow.Register("inject", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Inject",
		Category:     flow.CategorySource,
		Description:  "Emits a configured datagram once, and optionally repeats on an interval.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the inject node's "config" object (Flow-File-Format.md §2).
type Config struct {
	Payload        any `json:"payload"`
	InitialDelayMs int `json:"initialDelayMs,omitempty"`
	// RepeatMs re-fires on this interval; 0 means fire once and stop.
	RepeatMs int `json:"repeatMs,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "inject" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	if n.cfg.InitialDelayMs > 0 {
		select {
		case <-time.After(time.Duration(n.cfg.InitialDelayMs) * time.Millisecond):
		case <-ctx.Done():
			return nil
		}
	}

	if err := n.fire(ctx, emit); err != nil {
		return err
	}
	if n.cfg.RepeatMs <= 0 {
		return nil
	}

	ticker := time.NewTicker(time.Duration(n.cfg.RepeatMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := n.fire(ctx, emit); err != nil {
				return err
			}
		}
	}
}

func (n *node) fire(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	d := datagram.New(datagram.Source{Connector: "inject"}, datagram.Payload{Value: n.cfg.Payload})
	return emit("out", d)
}
