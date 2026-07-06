// Package busin implements the "bus-in" node (CON-600 "Bus In", BUS-120):
// subscribes to a named internal bus topic, decoupled from any single
// flow's own wiring, enabling flow-to-flow communication within a runtime.
package busin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/topics"
)

// DefaultSubscriberCapacity bounds this node's own subscription queue
// (BUS-110: every queue has a bound and overflow policy).
const DefaultSubscriberCapacity = 256

const configSchema = `{
	"type": "object",
	"properties": {
		"topic": { "type": "string", "description": "MQTT-style topic pattern to subscribe to (\"+\" matches one level, \"#\" matches the rest)." },
		"tags": { "type": "object", "description": "Optional exact-match tag filter; a published datagram must carry at least these tags." },
		"overflow": { "type": "string", "enum": ["block", "dropOldest", "dropNewest"], "description": "Overflow policy for this subscription's own queue (default dropOldest)." }
	},
	"required": ["topic"]
}`

func init() {
	flow.Register("bus-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Bus In",
		Category:     flow.CategorySource,
		Description:  "Subscribes to a named internal bus topic (BUS-120), decoupled from any single flow's wiring.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "bus-in" node's "config" object.
type Config struct {
	Topic    string            `json:"topic"`
	Tags     map[string]string `json:"tags,omitempty"`
	Overflow string            `json:"overflow,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "bus-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("bus-in: topic is required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	wire, cancel := topics.DefaultBroker.Subscribe(n.cfg.Topic, n.cfg.Tags, bus.WireConfig{
		Capacity: DefaultSubscriberCapacity,
		Overflow: parseOverflow(n.cfg.Overflow),
	})
	defer cancel()

	for {
		d, err := wire.Receive(ctx)
		if err != nil {
			return nil
		}
		if err := emit("out", d); err != nil {
			return err
		}
	}
}

func parseOverflow(spec string) bus.OverflowPolicy {
	switch spec {
	case "block":
		return bus.OverflowBlock
	case "dropNewest":
		return bus.OverflowDropNewest
	default:
		return bus.OverflowDropOldest
	}
}
