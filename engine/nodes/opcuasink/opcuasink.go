// Package opcuasink implements the "opcua-sink" node (CON-210 write side):
// mode "write" writes a value to a node; mode "call" invokes a method.
package opcuasink

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/opcuashared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["write", "call"] },
		"nodeId": { "type": "string", "description": "Target node, mode \"write\"." },
		"valueField": { "type": "string", "description": "Payload field carrying the value to write (default: the whole payload), mode \"write\"." },
		"objectNodeId": { "type": "string", "description": "Owning object node, mode \"call\"." },
		"methodNodeId": { "type": "string", "description": "Method node, mode \"call\"." },
		"argsField": { "type": "string", "description": "Payload field carrying an array of input arguments, mode \"call\" (default: the whole payload if it is an array)." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("opcua-sink", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "OPC-UA Sink",
		Category:     flow.CategoryProcessor,
		Description:  "Write a node value, or call a method (CON-210).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "opcua-sink" node's "config" object.
type Config struct {
	Mode         string `json:"mode"`
	NodeID       string `json:"nodeId,omitempty"`
	ValueField   string `json:"valueField,omitempty"`
	ObjectNodeID string `json:"objectNodeId,omitempty"`
	MethodNodeID string `json:"methodNodeId,omitempty"`
	ArgsField    string `json:"argsField,omitempty"`
}

type node struct {
	cfg Config

	nodeID   *ua.NodeID
	objectID *ua.NodeID
	methodID *ua.NodeID

	connectOnce sync.Once
	client      *opcua.Client
	connectErr  error
}

// New is the flow.Factory for the "opcua-sink" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	n := &node{cfg: cfg}
	switch cfg.Mode {
	case "write":
		if cfg.NodeID == "" {
			return nil, fmt.Errorf("opcua-sink: nodeId is required for mode \"write\"")
		}
		id, err := ua.ParseNodeID(cfg.NodeID)
		if err != nil {
			return nil, fmt.Errorf("opcua-sink: invalid nodeId %q: %w", cfg.NodeID, err)
		}
		n.nodeID = id
	case "call":
		if cfg.ObjectNodeID == "" || cfg.MethodNodeID == "" {
			return nil, fmt.Errorf("opcua-sink: objectNodeId and methodNodeId are required for mode \"call\"")
		}
		objID, err := ua.ParseNodeID(cfg.ObjectNodeID)
		if err != nil {
			return nil, fmt.Errorf("opcua-sink: invalid objectNodeId %q: %w", cfg.ObjectNodeID, err)
		}
		methID, err := ua.ParseNodeID(cfg.MethodNodeID)
		if err != nil {
			return nil, fmt.Errorf("opcua-sink: invalid methodNodeId %q: %w", cfg.MethodNodeID, err)
		}
		n.objectID, n.methodID = objID, methID
	default:
		return nil, fmt.Errorf("opcua-sink: mode must be \"write\" or \"call\", got %q", cfg.Mode)
	}
	return n, nil
}

func (n *node) connect(ctx context.Context) (*opcua.Client, error) {
	n.connectOnce.Do(func() { n.client, n.connectErr = opcuashared.Connect(ctx) })
	return n.client, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	client, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("opcua-sink: %w", err)
	}
	if n.cfg.Mode == "write" {
		return n.processWrite(ctx, client, in)
	}
	return n.processCall(ctx, client, in)
}

func (n *node) processWrite(ctx context.Context, client *opcua.Client, in datagram.Datagram) ([]flow.PortDatagram, error) {
	value := fieldOrWhole(in.Payload.Value, n.cfg.ValueField)
	v, err := ua.NewVariant(value)
	if err != nil {
		return nil, fmt.Errorf("opcua-sink: encoding value %v: %w", value, err)
	}
	req := &ua.WriteRequest{
		NodesToWrite: []*ua.WriteValue{{
			NodeID:      n.nodeID,
			AttributeID: ua.AttributeIDValue,
			Value:       &ua.DataValue{EncodingMask: ua.DataValueValue, Value: v},
		}},
	}
	resp, err := client.Write(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("opcua-sink: write: %w", err)
	}
	if len(resp.Results) > 0 && resp.Results[0] != ua.StatusOK {
		return nil, fmt.Errorf("opcua-sink: write failed: %s", resp.Results[0])
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) processCall(ctx context.Context, client *opcua.Client, in datagram.Datagram) ([]flow.PortDatagram, error) {
	var argValues []any
	raw := fieldOrWhole(in.Payload.Value, n.cfg.ArgsField)
	if arr, ok := raw.([]any); ok {
		argValues = arr
	}
	args := make([]*ua.Variant, len(argValues))
	for i, v := range argValues {
		vv, err := ua.NewVariant(v)
		if err != nil {
			return nil, fmt.Errorf("opcua-sink: encoding arg[%d] %v: %w", i, v, err)
		}
		args[i] = vv
	}

	result, err := client.Call(ctx, &ua.CallMethodRequest{
		ObjectID:       n.objectID,
		MethodID:       n.methodID,
		InputArguments: args,
	})
	if err != nil {
		return nil, fmt.Errorf("opcua-sink: call: %w", err)
	}
	if result.StatusCode != ua.StatusOK {
		return nil, fmt.Errorf("opcua-sink: call failed: %s", result.StatusCode)
	}

	outputs := make([]any, len(result.OutputArguments))
	for i, v := range result.OutputArguments {
		outputs[i] = v.Value()
	}
	out := datagram.NewCaused(in, datagram.Source{NodeID: "opcua-sink"}, datagram.Payload{Value: outputs})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func fieldOrWhole(payload any, field string) any {
	if field == "" {
		return payload
	}
	if m, ok := payload.(map[string]any); ok {
		return m[field]
	}
	return payload
}
