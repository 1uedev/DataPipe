// Package template implements the "template" node (PROC-130): renders text
// (strings, reports, SQL, markup) from a datagram using engine/expr's
// "{{ expr }}" mixed-template mode — logic-capable because each placeholder
// is a full JavaScript expression (ternaries, .map/.join, etc.), the same
// "one documented expression syntax platform-wide" MAP-130 requires
// elsewhere, rather than a second, template-specific control-flow syntax.
package template

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
		"template": {
			"type": "string",
			"description": "Literal text with \"{{ expr }}\" placeholders; each placeholder is a JavaScript expression evaluated against payload/header/tags/env/flow/global."
		},
		"parseJSON": {
			"type": "boolean",
			"default": false,
			"description": "Parse the rendered text as JSON before emitting, instead of emitting the raw string."
		},
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["template"]
}`

func init() {
	flow.Register("template", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Template",
		Category:     flow.CategoryProcessor,
		Description:  "Renders text (strings, reports, SQL, markup) from a datagram with a logic-capable template syntax (PROC-130).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "template" node's "config" object.
type Config struct {
	Template  string `json:"template"`
	ParseJSON bool   `json:"parseJSON,omitempty"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "template" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Template == "" {
		return nil, fmt.Errorf("template: template is required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data := nodeutil.ExprData(ctx, in)
	rendered, err := expr.RenderTemplate(ctx, n.cfg.Template, data, time.Duration(n.cfg.TimeoutMs)*time.Millisecond)
	if err != nil {
		return nil, fmt.Errorf("template: %w", err)
	}

	var value any = rendered
	if n.cfg.ParseJSON {
		if err := json.Unmarshal([]byte(rendered), &value); err != nil {
			return nil, fmt.Errorf("template: rendered text is not valid JSON: %w", err)
		}
	}

	out := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: value})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}
