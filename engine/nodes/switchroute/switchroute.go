// Package switchroute implements the "switch" node (PROC-300): rule-based
// routing to N output ports. Rules are JavaScript boolean expressions (see
// engine/expr) — comparisons, regex (JS regex literals/.test()), type
// checks (typeof), quality checks (header.quality), tag matches
// (tags.foo), and arbitrary predicates are all just expressions, rather
// than a separate structured-comparison mini-language. Output ports follow
// Flow-File-Format.md §2's documented convention: dynamic "out0".."outN"
// plus "default".
package switchroute

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/nodeutil"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"rules": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"expression": { "type": "string", "description": "JavaScript boolean expression (see engine/expr)." }
				},
				"required": ["expression"]
			}
		},
		"mode": { "type": "string", "enum": ["firstMatch", "allMatches"], "default": "firstMatch" },
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["rules"]
}`

func init() {
	flow.Register("switch", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out0", "default"}, // static fallback; DynamicOutputs reflects the real per-config rule count
		DisplayName:  "Switch/Route",
		Category:     flow.CategoryControl,
		Description:  "Rule-based routing to N output ports: comparisons, regex, type/quality/tag checks, arbitrary predicates (PROC-300).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Rule is one routing predicate.
type Rule struct {
	Expression string `json:"expression"`
}

// Config is the "switch" node's "config" object.
type Config struct {
	Rules     []Rule `json:"rules"`
	Mode      string `json:"mode,omitempty"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
}

type compiledRule struct {
	port string
	prog *expr.Program
}

type node struct {
	rules      []compiledRule
	allMatches bool
	timeout    time.Duration
	outputs    []string
	rt         *expr.Runtime
}

// New is the flow.Factory for the "switch" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if len(cfg.Rules) == 0 {
		return nil, fmt.Errorf("switch: rules is required")
	}
	if cfg.Mode == "" {
		cfg.Mode = "firstMatch"
	}
	if cfg.Mode != "firstMatch" && cfg.Mode != "allMatches" {
		return nil, fmt.Errorf("switch: unknown mode %q", cfg.Mode)
	}

	rules := make([]compiledRule, len(cfg.Rules))
	outputs := make([]string, 0, len(cfg.Rules)+1)
	for i, r := range cfg.Rules {
		prog, err := expr.Compile(r.Expression)
		if err != nil {
			return nil, fmt.Errorf("switch: rule %d: %w", i, err)
		}
		port := fmt.Sprintf("out%d", i)
		rules[i] = compiledRule{port: port, prog: prog}
		outputs = append(outputs, port)
	}
	outputs = append(outputs, "default")

	return &node{
		rules:      rules,
		allMatches: cfg.Mode == "allMatches",
		timeout:    time.Duration(cfg.TimeoutMs) * time.Millisecond,
		outputs:    outputs,
		rt:         expr.New(),
	}, nil
}

// OutputPorts implements flow.DynamicOutputs.
func (n *node) OutputPorts() []string { return n.outputs }

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data := nodeutil.ExprData(ctx, in)

	var results []flow.PortDatagram
	for _, r := range n.rules {
		v, err := n.rt.Run(ctx, r.prog, data, n.timeout)
		if err != nil {
			return nil, fmt.Errorf("switch: rule %q: %w", r.port, err)
		}
		matched, _ := v.(bool)
		if !matched {
			continue
		}
		results = append(results, flow.PortDatagram{Port: r.port, Datagram: in})
		if !n.allMatches {
			break
		}
	}

	if len(results) == 0 {
		return []flow.PortDatagram{{Port: "default", Datagram: in}}, nil
	}
	return results, nil
}
