// Package state implements the "state" node (PROC-410): read/write named
// node-, flow-, or global-scoped key/value state via engine/ctxstore
// (pluggable persistence — memory today, a durable backend can be added
// later without changing this node), including the atomic increment
// operation.
package state

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/1uedev/DataPipe/engine/ctxstore"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/nodeutil"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"scope": { "type": "string", "enum": ["node", "flow", "global"] },
		"operations": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"op": { "type": "string", "enum": ["get", "set", "delete", "increment"] },
					"key": { "type": "string" },
					"value": { "description": "Literal or \"={{ expr }}\" (MAP-130); used by \"set\" (the value) and \"increment\" (the delta, default 1)." },
					"as": { "type": "string", "description": "\".\"-separated path where \"get\"/\"increment\"'s result is written into the payload." }
				},
				"required": ["op", "key"]
			}
		}
	},
	"required": ["scope", "operations"]
}`

func init() {
	flow.Register("state", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "State/Context",
		Category:     flow.CategoryProcessor,
		Description:  "Read/write node-, flow-, or global-scoped state, including atomic increment (PROC-410).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Operation is one state read/write applied in order.
type Operation struct {
	Op    string          `json:"op"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
	As    string          `json:"as,omitempty"`
}

// Config is the "state" node's "config" object.
type Config struct {
	Scope      string      `json:"scope"`
	Operations []Operation `json:"operations"`
}

var validOps = map[string]bool{"get": true, "set": true, "delete": true, "increment": true}

type node struct{ cfg Config }

// New is the flow.Factory for the "state" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Scope != "node" && cfg.Scope != "flow" && cfg.Scope != "global" {
		return nil, fmt.Errorf("state: scope must be \"node\", \"flow\", or \"global\"")
	}
	if len(cfg.Operations) == 0 {
		return nil, fmt.Errorf("state: operations is required")
	}
	for i, op := range cfg.Operations {
		if !validOps[op.Op] {
			return nil, fmt.Errorf("state: operation %d: unknown op %q", i, op.Op)
		}
		if op.Key == "" {
			return nil, fmt.Errorf("state: operation %d: key is required", i)
		}
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	store, ok := flow.ContextStore(ctx)
	if !ok {
		return nil, fmt.Errorf("state: no context store attached")
	}
	flowID, nodeID := flow.CurrentIDs(ctx)
	scope := ctxstore.Scope(n.cfg.Scope)

	value := deepCopy(in.Payload.Value)
	for _, op := range n.cfg.Operations {
		key := ctxstore.Key{Scope: scope, FlowID: flowID, NodeID: nodeID, Name: op.Key}
		var err error
		value, err = n.applyOp(ctx, store, key, op, value, in)
		if err != nil {
			return nil, fmt.Errorf("state: operation %q on key %q: %w", op.Op, op.Key, err)
		}
	}

	out := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: value})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func (n *node) applyOp(ctx context.Context, store ctxstore.Store, key ctxstore.Key, op Operation, payload any, in datagram.Datagram) (any, error) {
	switch op.Op {
	case "get":
		v, _, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if op.As == "" {
			return payload, nil
		}
		return applyPath(payload, op.As, v), nil
	case "set":
		v, err := resolveValue(ctx, op.Value, in, payload)
		if err != nil {
			return nil, err
		}
		if err := store.Set(ctx, key, v); err != nil {
			return nil, err
		}
		return payload, nil
	case "delete":
		if err := store.Delete(ctx, key); err != nil && err != ctxstore.ErrNotFound {
			return nil, err
		}
		return payload, nil
	case "increment":
		delta := 1.0
		if len(op.Value) > 0 {
			v, err := resolveValue(ctx, op.Value, in, payload)
			if err != nil {
				return nil, err
			}
			f, ok := toFloat(v)
			if !ok {
				return nil, fmt.Errorf("increment delta did not evaluate to a number (got %T)", v)
			}
			delta = f
		}
		next, err := store.Increment(ctx, key, delta)
		if err != nil {
			return nil, err
		}
		if op.As == "" {
			return payload, nil
		}
		return applyPath(payload, op.As, next), nil
	default:
		return nil, fmt.Errorf("unhandled op %q", op.Op)
	}
}

func resolveValue(ctx context.Context, raw json.RawMessage, in datagram.Datagram, currentPayload any) (any, error) {
	data := nodeutil.ExprData(ctx, in)
	data.Payload = currentPayload
	return expr.ResolveValue(ctx, raw, data, 0)
}

func applyPath(root any, path string, value any) any {
	if path == "" {
		return value
	}
	keys := strings.Split(path, ".")
	m, ok := root.(map[string]any)
	if !ok {
		m = map[string]any{}
	}
	cur := m
	for _, k := range keys[:len(keys)-1] {
		next, ok := cur[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[k] = next
		}
		cur = next
	}
	cur[keys[len(keys)-1]] = value
	return m
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopy(val)
		}
		return out
	default:
		return v
	}
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int64:
		return float64(t), true
	case int:
		return float64(t), true
	default:
		return 0, false
	}
}
