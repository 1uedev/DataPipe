// Package debuglog implements the "debug-log" node (DBG-110): an explicit
// node that pushes a selected expression from each datagram it sees to the
// global debug sidebar, and optionally also logs it to the console.
package debuglog

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"label": { "type": "string", "description": "Identifies this debug node's output among several in one flow." },
		"expression": { "type": "string", "description": "\".\"-separated key path into the payload to capture; empty captures the whole payload." },
		"console": { "type": "boolean", "description": "Also log to the console/structured log, in addition to the debug sidebar (off by default)." }
	}
}`

func init() {
	flow.Register("debug-log", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		DisplayName:  "Debug",
		Category:     flow.CategorySink,
		Description:  "Sends a selected expression to the global debug sidebar (DBG-110); optionally also logs to the console.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "debug-log" node's "config" object.
type Config struct {
	// Label identifies this debug node's output among several in one flow.
	Label string `json:"label,omitempty"`
	// Expression is a "."-separated key path into the payload; empty
	// captures the whole payload.
	Expression string `json:"expression,omitempty"`
	// Console also logs to slog.Default(), in addition to the sidebar.
	Console bool `json:"console,omitempty"`
}

type node struct {
	cfg    Config
	logger *slog.Logger
}

// New is the flow.Factory for the "debug-log" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
	}
	return &node{cfg: cfg, logger: slog.Default()}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	value := evalExpression(in.Payload.Value, n.cfg.Expression)

	flow.SidebarEvent(ctx, n.cfg.Label, in, value)

	if n.cfg.Console {
		n.logger.Info("debug",
			"label", n.cfg.Label,
			"correlationId", in.Header.CorrelationID,
			"quality", in.Header.Quality,
			"payload", value,
		)
	}
	return nil, nil
}

// evalExpression reads a "."-separated key path out of a JSON-shaped value
// (the minimal read-side counterpart to the "set" node's applySet); an
// empty path returns the whole value unchanged. Full expression support
// (MAP-130) is a later increment.
func evalExpression(root any, path string) any {
	if path == "" {
		return root
	}
	cur := root
	for _, k := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[k]
	}
	return cur
}
