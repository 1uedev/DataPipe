// Package busout implements the "bus-out" node (SNK-220 "Bus Out",
// BUS-120): publishes the datagram to a named internal bus topic and
// passes it through unchanged, so a flow can hand off to other flows
// listening on the bus while still continuing its own branch.
package busout

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/topics"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"topic": { "type": "string", "description": "Topic name to publish to." },
		"tags": { "type": "object", "description": "Optional tags carried alongside the published datagram, matched by bus-in's tag filter." }
	},
	"required": ["topic"]
}`

func init() {
	flow.Register("bus-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Bus Out",
		Category:     flow.CategoryProcessor,
		Description:  "Publishes to a named internal bus topic (BUS-120) and passes the datagram through unchanged.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "bus-out" node's "config" object.
type Config struct {
	Topic string            `json:"topic"`
	Tags  map[string]string `json:"tags,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "bus-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("bus-out: topic is required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	topics.DefaultBroker.Publish(ctx, n.cfg.Topic, n.cfg.Tags, in)
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}
