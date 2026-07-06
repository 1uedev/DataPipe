// Package set implements the "set" node (PROC-110 Set/Change, minimal
// slice): declarative field operations without code. Expression support
// (MAP-130) lands in Increment 7; this only applies literal values.
package set

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"sets": {
			"type": "array",
			"description": "Literal field assignments applied in order.",
			"items": {
				"type": "object",
				"properties": {
					"path": { "type": "string", "description": "\".\"-separated key path; empty replaces the whole payload." },
					"value": { "description": "The literal value to assign." }
				},
				"required": ["path"]
			}
		}
	},
	"required": ["sets"]
}`

func init() {
	flow.Register("set", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Set",
		Category:     flow.CategoryProcessor,
		Description:  "Declarative field operations without code: set literal values at a payload path.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// SetOp assigns a literal Value at Path, a "."-separated key path into the
// payload (an empty Path replaces the whole payload).
type SetOp struct {
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// Config is the "set" node's "config" object.
type Config struct {
	Sets []SetOp `json:"sets"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "set" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	// deepCopy first: fan-out only deep-copies Tags/binary payloads
	// (DGM-120/BUS-140), not generic map/slice payload values, so mutating
	// in place could otherwise leak across branches sharing the same map.
	value := deepCopy(in.Payload.Value)

	for _, op := range n.cfg.Sets {
		value = applySet(value, op.Path, op.Value)
	}

	out := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: value})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func applySet(root any, path string, value any) any {
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
		return v // scalars (string/number/bool/nil) are immutable by value
	}
}
