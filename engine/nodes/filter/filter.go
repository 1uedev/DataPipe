// Package filter implements the "filter" node (PROC-310): pass/drop by
// predicate, or "report by exception" — only forward when a numeric field
// changed beyond a deadband, or a maximum interval has elapsed since the
// last forward (a heartbeat so a genuinely idle signal still gets reported
// occasionally).
package filter

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
		"mode": { "type": "string", "enum": ["predicate", "deadband"] },
		"predicate": { "type": "string", "description": "JavaScript boolean expression (mode \"predicate\"): true passes, false drops." },
		"field": { "type": "string", "description": "\".\"-separated numeric field path to watch (mode \"deadband\")." },
		"deadband": { "type": "number", "minimum": 0, "description": "Forward when the field changes by at least this much (mode \"deadband\")." },
		"minIntervalMs": { "type": "integer", "minimum": 0, "description": "Also forward if at least this long has passed since the last forward, even without enough change (mode \"deadband\")." },
		"groupBy": { "type": "string", "description": "JavaScript expression keying independent deadband state per group; empty tracks one shared state." },
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("filter", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"pass", "drop"},
		DisplayName:  "Filter",
		Category:     flow.CategoryControl,
		Description:  "Pass/drop by predicate, or report-by-exception via deadband/interval (PROC-310).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "filter" node's "config" object.
type Config struct {
	Mode          string  `json:"mode"`
	Predicate     string  `json:"predicate,omitempty"`
	Field         string  `json:"field,omitempty"`
	Deadband      float64 `json:"deadband,omitempty"`
	MinIntervalMs int     `json:"minIntervalMs,omitempty"`
	GroupBy       string  `json:"groupBy,omitempty"`
	TimeoutMs     int     `json:"timeoutMs,omitempty"`
}

type deadbandState struct {
	hasValue bool
	last     float64
	lastTime time.Time
}

type node struct {
	cfg          Config
	predicate    *expr.Program
	groupProg    *expr.Program
	rt           *expr.Runtime
	timeout      time.Duration
	deadbandByID map[string]*deadbandState
}

// New is the flow.Factory for the "filter" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	n := &node{cfg: cfg, rt: expr.New(), timeout: time.Duration(cfg.TimeoutMs) * time.Millisecond, deadbandByID: map[string]*deadbandState{}}

	switch cfg.Mode {
	case "predicate":
		if cfg.Predicate == "" {
			return nil, fmt.Errorf("filter: predicate is required for mode \"predicate\"")
		}
		prog, err := expr.Compile(cfg.Predicate)
		if err != nil {
			return nil, fmt.Errorf("filter: predicate: %w", err)
		}
		n.predicate = prog
	case "deadband":
		if cfg.Field == "" {
			return nil, fmt.Errorf("filter: field is required for mode \"deadband\"")
		}
		if cfg.Deadband < 0 {
			return nil, fmt.Errorf("filter: deadband must be >= 0")
		}
	default:
		return nil, fmt.Errorf("filter: unknown mode %q", cfg.Mode)
	}

	if strings.TrimSpace(cfg.GroupBy) != "" {
		prog, err := expr.Compile(cfg.GroupBy)
		if err != nil {
			return nil, fmt.Errorf("filter: groupBy: %w", err)
		}
		n.groupProg = prog
	}
	return n, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data := nodeutil.ExprData(ctx, in)

	var pass bool
	var err error
	switch n.cfg.Mode {
	case "predicate":
		pass, err = n.evalPredicate(ctx, data)
	case "deadband":
		pass, err = n.evalDeadband(ctx, in, data)
	}
	if err != nil {
		return nil, fmt.Errorf("filter: %w", err)
	}

	port := "drop"
	if pass {
		port = "pass"
	}
	return []flow.PortDatagram{{Port: port, Datagram: in}}, nil
}

func (n *node) evalPredicate(ctx context.Context, data expr.Data) (bool, error) {
	v, err := n.rt.Run(ctx, n.predicate, data, n.timeout)
	if err != nil {
		return false, err
	}
	b, _ := v.(bool)
	return b, nil
}

func (n *node) evalDeadband(ctx context.Context, in datagram.Datagram, data expr.Data) (bool, error) {
	value := fieldValue(in.Payload.Value, n.cfg.Field)
	f, ok := toFloat(value)
	if !ok {
		return false, fmt.Errorf("field %q is not numeric (got %T)", n.cfg.Field, value)
	}

	key := "_"
	if n.groupProg != nil {
		v, err := n.rt.Run(ctx, n.groupProg, data, n.timeout)
		if err != nil {
			return false, fmt.Errorf("groupBy: %w", err)
		}
		key = fmt.Sprint(v)
	}
	st, ok := n.deadbandByID[key]
	if !ok {
		st = &deadbandState{}
		n.deadbandByID[key] = st
	}

	now := time.Now()
	changed := !st.hasValue || abs(f-st.last) >= n.cfg.Deadband
	heartbeat := n.cfg.MinIntervalMs > 0 && st.hasValue && now.Sub(st.lastTime) >= time.Duration(n.cfg.MinIntervalMs)*time.Millisecond
	pass := changed || heartbeat

	if pass {
		st.hasValue = true
		st.last = f
		st.lastTime = now
	}
	return pass, nil
}

func fieldValue(item any, path string) any {
	if path == "" {
		return item
	}
	cur := item
	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[key]
	}
	return cur
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

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
