// Package debuglog implements the "debug-log" node (DBG-110 minimal
// slice): prints selected datagrams. The live debug sidebar/ring buffer
// (DBG-100/110 in full) is an Increment 5 concern; this only proves the
// wiring works end to end via structured log output.
package debuglog

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"label": { "type": "string", "description": "Identifies this debug node's output among several in one flow." }
	}
}`

func init() {
	flow.Register("debug-log", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		DisplayName:  "Debug Log",
		Category:     flow.CategorySink,
		Description:  "Logs the datagrams that pass through it (structured log output).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "debug-log" node's "config" object.
type Config struct {
	// Label identifies this debug node's output among several in one flow.
	Label string `json:"label,omitempty"`
}

type node struct {
	cfg    Config
	logger *slog.Logger
}

// New is the flow.Factory for the "debug-log" node type. It logs via
// slog.Default() so CLI runs show output on stdout/stderr without any
// special wiring.
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
	n.logger.Info("debug",
		"label", n.cfg.Label,
		"correlationId", in.Header.CorrelationID,
		"quality", in.Header.Quality,
		"payload", in.Payload.Value,
	)
	return nil, nil
}
