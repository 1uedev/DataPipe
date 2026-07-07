// Package errortrigger implements the "error-trigger" node (ERR-120): the
// n8n Error-Trigger equivalent, and a triggered flow's entry node — each
// unhandled node error published for the configured target flow becomes a
// fresh, independently tracked execution (ENG-130) of this error-handler
// flow, carrying the original datagram plus the ERR-100 error object.
package errortrigger

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
		"flowId": {
			"type": "string",
			"description": "The flow id (settings.errorFlow override target) whose unhandled errors this handles, or \"*\" for the project-wide default error handler."
		}
	},
	"required": ["flowId"]
}`

func init() {
	flow.Register("error-trigger", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Trigger:      true, // each unhandled error becomes a tracked execution (ENG-100/ENG-130/ERR-120)
		Outputs:      []string{"out"},
		DisplayName:  "Error Trigger",
		Category:     flow.CategoryControl,
		Description:  "Flow-level error handler entry point (ERR-120): starts an execution for every unhandled error routed to this flow.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "error-trigger" node's "config" object.
type Config struct {
	FlowID string `json:"flowId"`
}

type node struct{ cfg Config }

// TriggerKind reports error-trigger's ENG-130 trigger-kind label ("error")
// for execution-history display (flow.TriggerKindProvider).
func (n *node) TriggerKind() string { return "error" }

// New is the flow.Factory for the "error-trigger" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.FlowID == "" {
		return nil, fmt.Errorf("error-trigger: flowId is required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	wire, cancel := topics.DefaultBroker.Subscribe(flow.ErrorFlowTopic(n.cfg.FlowID), nil, bus.WireConfig{
		Capacity: DefaultSubscriberCapacity,
		Overflow: bus.OverflowDropOldest,
	})
	defer cancel()

	for {
		d, err := wire.Receive(ctx)
		if err != nil {
			return nil
		}
		// A fresh correlation chain (datagram.New), not NewCaused: this is
		// the start of a brand-new tracked execution of the error-handler
		// flow, independent of whatever execution the original error
		// belonged to (its datagram/error object is preserved as d.Payload,
		// built by flow.BuildErrorDatagram).
		fresh := datagram.New(datagram.Source{NodeID: "error-trigger", Origin: "error-flow"}, d.Payload)
		if err := emit("out", fresh); err != nil {
			return err
		}
	}
}
