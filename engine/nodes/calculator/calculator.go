// Package calculator implements the "calculator" node (PROC-200): derived
// values via engine/expr expressions — unit conversion, scaling, polynomial
// linearization, arbitrary math including the stats.* statistical helpers.
package calculator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/nodeutil"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"fields": {
			"type": "array",
			"description": "Derived field assignments applied in order (so a later field's expression can see an earlier one's result).",
			"items": {
				"type": "object",
				"properties": {
					"path": { "type": "string", "description": "\".\"-separated key path; empty replaces the whole payload." },
					"expression": { "type": "string", "description": "JavaScript expression (see engine/expr): payload/header/tags/env/flow/global, plus dt/stats/conv/hash helpers." }
				},
				"required": ["path", "expression"]
			}
		},
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["fields"]
}`

func init() {
	flow.Register("calculator", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Calculator",
		Category:     flow.CategoryProcessor,
		Description:  "Derived values via expressions: unit conversion, scaling, polynomial linearization, statistics (PROC-200).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// FieldOp assigns Expression's evaluated result at Path.
type FieldOp struct {
	Path       string `json:"path"`
	Expression string `json:"expression"`
}

// Config is the "calculator" node's "config" object.
type Config struct {
	Fields    []FieldOp `json:"fields"`
	TimeoutMs int       `json:"timeoutMs,omitempty"`
}

type compiledField struct {
	path string
	prog *expr.Program
}

type node struct {
	fields  []compiledField
	timeout time.Duration
	rt      *expr.Runtime
}

// New is the flow.Factory for the "calculator" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.Fields) == 0 {
		return nil, fmt.Errorf("calculator: fields is required")
	}
	fields := make([]compiledField, len(cfg.Fields))
	for i, f := range cfg.Fields {
		prog, err := expr.Compile(f.Expression)
		if err != nil {
			return nil, fmt.Errorf("calculator: field %q: %w", f.Path, err)
		}
		fields[i] = compiledField{path: f.Path, prog: prog}
	}
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	return &node{fields: fields, timeout: timeout, rt: expr.New()}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	value := deepCopy(in.Payload.Value)
	data := nodeutil.ExprData(ctx, in)

	for _, f := range n.fields {
		data.Payload = value // later fields can see earlier ones' results
		result, err := n.rt.Run(ctx, f.prog, data, n.timeout)
		if err != nil {
			return nil, fmt.Errorf("calculator: field %q: %w", f.path, err)
		}
		value = applyPath(value, f.path, result)
	}

	out := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: value})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
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
