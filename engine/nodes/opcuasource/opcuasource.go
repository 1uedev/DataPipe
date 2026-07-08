// Package opcuasource implements the "opcua-source" node (CON-210): mode
// "subscribe" creates monitored items (sampling/publishing intervals,
// deadband filters) and emits one datagram per value-changed notification;
// mode "polled" periodically reads a fixed list of nodes and emits one
// datagram per node per poll. Event/alarm subscription and history read are
// P2, not implemented — see TODO.md.
package opcuasource

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/opcuashared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["subscribe", "polled"] },
		"nodes": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"name": { "type": "string" },
					"nodeId": { "type": "string", "description": "e.g. \"ns=2;s=Temperature\"." },
					"deadband": { "type": "number", "description": "Absolute deadband filter, mode \"subscribe\" (0 = report every sample)." }
				},
				"required": ["name", "nodeId"]
			}
		},
		"publishingIntervalMs": { "type": "integer", "minimum": 1, "description": "Mode \"subscribe\"." },
		"samplingIntervalMs": { "type": "integer", "minimum": 1, "description": "Mode \"subscribe\"." },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "Mode \"polled\"." }
	},
	"required": ["mode", "nodes"]
}`

func init() {
	flow.Register("opcua-source", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "OPC-UA Source",
		Category:     flow.CategorySource,
		Description:  "OPC-UA subscription (monitored items) or polled reads, one datagram per value (CON-210).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// NodeRef is one entry in the "nodes" config array.
type NodeRef struct {
	Name     string  `json:"name"`
	NodeID   string  `json:"nodeId"`
	Deadband float64 `json:"deadband,omitempty"`
}

// Config is the "opcua-source" node's "config" object.
type Config struct {
	Mode                 string    `json:"mode"`
	Nodes                []NodeRef `json:"nodes"`
	PublishingIntervalMs int       `json:"publishingIntervalMs,omitempty"`
	SamplingIntervalMs   int       `json:"samplingIntervalMs,omitempty"`
	IntervalMs           int       `json:"intervalMs,omitempty"`
}

type node struct {
	cfg    Config
	parsed []parsedNode
}

type parsedNode struct {
	NodeRef
	id *ua.NodeID
}

// New is the flow.Factory for the "opcua-source" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.Nodes) == 0 {
		return nil, fmt.Errorf("opcua-source: at least one node is required")
	}
	parsed := make([]parsedNode, len(cfg.Nodes))
	for i, nr := range cfg.Nodes {
		if nr.Name == "" || nr.NodeID == "" {
			return nil, fmt.Errorf("opcua-source: node[%d]: name and nodeId are required", i)
		}
		id, err := ua.ParseNodeID(nr.NodeID)
		if err != nil {
			return nil, fmt.Errorf("opcua-source: node[%d]: invalid nodeId %q: %w", i, nr.NodeID, err)
		}
		parsed[i] = parsedNode{NodeRef: nr, id: id}
	}
	switch cfg.Mode {
	case "subscribe":
	case "polled":
		if cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("opcua-source: intervalMs must be positive for mode \"polled\"")
		}
	default:
		return nil, fmt.Errorf("opcua-source: mode must be \"subscribe\" or \"polled\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg, parsed: parsed}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	client, err := opcuashared.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close(ctx) }()

	if n.cfg.Mode == "subscribe" {
		return n.runSubscribe(ctx, client, emit)
	}
	return n.runPolled(ctx, client, emit)
}

func (n *node) runPolled(ctx context.Context, client *opcua.Client, emit func(string, datagram.Datagram) error) error {
	poll := func() error {
		ids := make([]*ua.ReadValueID, len(n.parsed))
		for i, p := range n.parsed {
			ids[i] = &ua.ReadValueID{NodeID: p.id, AttributeID: ua.AttributeIDValue}
		}
		resp, err := client.Read(ctx, &ua.ReadRequest{NodesToRead: ids, TimestampsToReturn: ua.TimestampsToReturnBoth})
		if err != nil {
			return fmt.Errorf("opcua-source: read: %w", err)
		}
		for i, dv := range resp.Results {
			if err := emit("out", valueDatagram(n.parsed[i].Name, dv)); err != nil {
				return err
			}
		}
		return nil
	}
	if err := poll(); err != nil {
		return err
	}
	ticker := time.NewTicker(time.Duration(n.cfg.IntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := poll(); err != nil {
				return err
			}
		}
	}
}

func (n *node) runSubscribe(ctx context.Context, client *opcua.Client, emit func(string, datagram.Datagram) error) error {
	publishing := time.Duration(n.cfg.PublishingIntervalMs) * time.Millisecond
	if publishing <= 0 {
		publishing = time.Second
	}
	notifyCh := make(chan *opcua.PublishNotificationData, 16)
	sub, err := client.Subscribe(ctx, &opcua.SubscriptionParameters{Interval: publishing}, notifyCh)
	if err != nil {
		return fmt.Errorf("opcua-source: subscribing: %w", err)
	}
	defer func() { _ = sub.Cancel(ctx) }()

	sampling := time.Duration(n.cfg.SamplingIntervalMs) * time.Millisecond
	handleToName := make(map[uint32]string, len(n.parsed))
	reqs := make([]*ua.MonitoredItemCreateRequest, len(n.parsed))
	for i, p := range n.parsed {
		handle := uint32(i + 1)
		handleToName[handle] = p.Name
		req := opcua.NewMonitoredItemCreateRequestWithDefaults(p.id, ua.AttributeIDValue, handle)
		if sampling > 0 {
			req.RequestedParameters.SamplingInterval = float64(sampling.Milliseconds())
		}
		if p.Deadband > 0 {
			req.RequestedParameters.Filter = ua.NewExtensionObject(&ua.DataChangeFilter{
				Trigger:       ua.DataChangeTriggerStatusValue,
				DeadbandType:  uint32(ua.DeadbandTypeAbsolute),
				DeadbandValue: p.Deadband,
			})
		}
		reqs[i] = req
	}
	if _, err := sub.Monitor(ctx, ua.TimestampsToReturnBoth, reqs...); err != nil {
		return fmt.Errorf("opcua-source: creating monitored items: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case data, ok := <-notifyCh:
			if !ok {
				return nil
			}
			if data.Error != nil {
				continue // a single publish error doesn't end the subscription
			}
			change, ok := data.Value.(*ua.DataChangeNotification)
			if !ok {
				continue
			}
			for _, item := range change.MonitoredItems {
				name, ok := handleToName[item.ClientHandle]
				if !ok {
					continue
				}
				if err := emit("out", valueDatagram(name, item.Value)); err != nil {
					return err
				}
			}
		}
	}
}

func valueDatagram(name string, dv *ua.DataValue) datagram.Datagram {
	var value any
	if dv.Value != nil {
		value = dv.Value.Value()
	}
	d := datagram.New(datagram.Source{NodeID: "opcua-source"}, datagram.Payload{Value: value})
	d.Header.Tags = map[string]string{"opcua.node": name}
	d.Header.Quality = opcuashared.QualityOf(dv.Status)
	return d
}
