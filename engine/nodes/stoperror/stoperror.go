// Package stoperror implements the "stop-and-error" node (ERR-140):
// deliberately fails an execution with a structured error, routed through
// the node's own ERR-100 error policy like any other node error (the
// default "fail" policy is what actually stops the execution; a flow
// author who instead wires "errorPort" or "discard" onto this node has
// explicitly chosen to handle or acknowledge the deliberate stop, which is
// respected the same as for any other node).
package stoperror

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/nodeutil"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"message": { "type": "string", "description": "Structured error message (ERR-140); a literal string or a \"{{ expr }}\" mixed template." },
		"code": { "type": "string", "description": "Optional structured error code; a literal string or a \"{{ expr }}\" mixed template." },
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["message"]
}`

func init() {
	flow.Register("stop-and-error", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		DisplayName:  "Stop and Error",
		Category:     flow.CategoryControl,
		Description:  "Deliberately fails the execution with a structured error (ERR-140).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "stop-and-error" node's "config" object.
type Config struct {
	Message   string `json:"message"`
	Code      string `json:"code,omitempty"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "stop-and-error" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Message == "" {
		return nil, fmt.Errorf("stop-and-error: message is required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data := nodeutil.ExprData(ctx, in)
	timeout := time.Duration(n.cfg.TimeoutMs) * time.Millisecond

	message, err := expr.RenderTemplate(ctx, n.cfg.Message, data, timeout)
	if err != nil {
		return nil, fmt.Errorf("stop-and-error: rendering message: %w", err)
	}
	code := n.cfg.Code
	if code != "" {
		code, err = expr.RenderTemplate(ctx, code, data, timeout)
		if err != nil {
			return nil, fmt.Errorf("stop-and-error: rendering code: %w", err)
		}
	}

	// Returned as a *flow.NodeError directly (rather than a plain error) so
	// AsNodeError preserves Message/Code as-is instead of only wrapping a
	// generic message — the "structured error" ERR-140 asks for.
	return nil, &flow.NodeError{Message: message, Code: code}
}
