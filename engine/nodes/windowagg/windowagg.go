// Package windowagg implements the "window-aggregate" node (PROC-210):
// tumbling, sliding, and session windows by time or count, grouped by a key
// expression, computing min/max/avg/sum/count/first/last/stddev/
// percentile/collect-to-list aggregates.
//
// A window closes and emits on the NEXT incoming datagram after its
// boundary passes, not proactively on a background timer: the engine's
// Processor contract (engine/flow.Processor) is purely reactive — one
// datagram in, zero or more out — and has no lifecycle hook a node could
// use to run or clean up a background goroutine tied to its own teardown.
// A genuinely idle group (no further datagrams) holds its last partial
// window unemitted until either new data arrives or the flow is
// redeployed. Watermark/lateness handling for out-of-order source
// timestamps (SHOULD, P2) is not implemented. Both are documented in
// TODO.md as scope reductions forced by the current single-input,
// call-and-return node model.
package windowagg

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
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
		"windowType": { "type": "string", "enum": ["tumbling", "sliding", "session"] },
		"windowBy": { "type": "string", "enum": ["time", "count"], "description": "Ignored for windowType \"session\", which is always time-gap-based." },
		"sizeMs": { "type": "integer", "minimum": 1 },
		"count": { "type": "integer", "minimum": 1 },
		"sessionGapMs": { "type": "integer", "minimum": 1 },
		"groupBy": { "type": "string", "description": "JavaScript expression (see engine/expr); empty groups everything together." },
		"aggregates": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"field": { "type": "string", "description": "\".\"-separated path into the payload; empty aggregates the whole payload (collect/first/last/count only)." },
					"op": { "type": "string", "enum": ["min", "max", "avg", "sum", "count", "first", "last", "stddev", "percentile", "collect"] },
					"percentile": { "type": "number", "minimum": 0, "maximum": 100, "description": "Required when op is \"percentile\"." },
					"as": { "type": "string", "description": "Output field name." }
				},
				"required": ["op", "as"]
			}
		}
	},
	"required": ["windowType", "aggregates"]
}`

func init() {
	flow.Register("window-aggregate", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Window/Aggregate",
		Category:     flow.CategoryProcessor,
		Description:  "Tumbling/sliding/session windows grouped by key, with min/max/avg/sum/count/first/last/stddev/percentile/collect aggregates (PROC-210).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// AggregateOp is one configured output field derived from the window's
// buffered items.
type AggregateOp struct {
	Field      string  `json:"field,omitempty"`
	Op         string  `json:"op"`
	Percentile float64 `json:"percentile,omitempty"`
	As         string  `json:"as"`
}

// Config is the "window-aggregate" node's "config" object.
type Config struct {
	WindowType   string        `json:"windowType"`
	WindowBy     string        `json:"windowBy,omitempty"`
	SizeMs       int           `json:"sizeMs,omitempty"`
	Count        int           `json:"count,omitempty"`
	SessionGapMs int           `json:"sessionGapMs,omitempty"`
	GroupBy      string        `json:"groupBy,omitempty"`
	Aggregates   []AggregateOp `json:"aggregates"`
}

var validOps = map[string]bool{
	"min": true, "max": true, "avg": true, "sum": true, "count": true,
	"first": true, "last": true, "stddev": true, "percentile": true, "collect": true,
}

// groupState is one group's window buffer. The engine calls Process
// serially for a given node instance (one input port, one receive loop),
// so this needs no locking.
type groupState struct {
	items       []any
	itemTimes   []time.Time // parallel to items, only populated for windowBy "time"
	windowStart time.Time
	lastItem    time.Time
}

type node struct {
	cfg       Config
	groupProg *expr.Program // nil if GroupBy is empty
	rt        *expr.Runtime
	groups    map[string]*groupState
}

// New is the flow.Factory for the "window-aggregate" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.WindowType {
	case "tumbling", "sliding":
		switch cfg.WindowBy {
		case "time":
			if cfg.SizeMs <= 0 {
				return nil, fmt.Errorf("window-aggregate: sizeMs is required for windowBy \"time\"")
			}
		case "count":
			if cfg.Count <= 0 {
				return nil, fmt.Errorf("window-aggregate: count is required for windowBy \"count\"")
			}
		default:
			return nil, fmt.Errorf("window-aggregate: windowBy must be \"time\" or \"count\" for windowType %q", cfg.WindowType)
		}
	case "session":
		if cfg.SessionGapMs <= 0 {
			return nil, fmt.Errorf("window-aggregate: sessionGapMs is required for windowType \"session\"")
		}
	default:
		return nil, fmt.Errorf("window-aggregate: unknown windowType %q", cfg.WindowType)
	}
	if len(cfg.Aggregates) == 0 {
		return nil, fmt.Errorf("window-aggregate: aggregates is required")
	}
	for _, a := range cfg.Aggregates {
		if !validOps[a.Op] {
			return nil, fmt.Errorf("window-aggregate: unknown aggregate op %q", a.Op)
		}
		if a.As == "" {
			return nil, fmt.Errorf("window-aggregate: aggregate \"as\" is required")
		}
		if a.Op == "percentile" && (a.Percentile < 0 || a.Percentile > 100) {
			return nil, fmt.Errorf("window-aggregate: aggregate %q: percentile must be 0..100", a.As)
		}
	}

	var groupProg *expr.Program
	if strings.TrimSpace(cfg.GroupBy) != "" {
		prog, err := expr.Compile(cfg.GroupBy)
		if err != nil {
			return nil, fmt.Errorf("window-aggregate: groupBy: %w", err)
		}
		groupProg = prog
	}

	return &node{cfg: cfg, groupProg: groupProg, rt: expr.New(), groups: map[string]*groupState{}}, nil
}

func (n *node) groupKey(ctx context.Context, in datagram.Datagram) (string, error) {
	if n.groupProg == nil {
		return "_", nil
	}
	data := nodeutil.ExprData(ctx, in)
	v, err := n.rt.Run(ctx, n.groupProg, data, 0)
	if err != nil {
		return "", fmt.Errorf("groupBy: %w", err)
	}
	return fmt.Sprint(v), nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	key, err := n.groupKey(ctx, in)
	if err != nil {
		return nil, err
	}
	g, ok := n.groups[key]
	if !ok {
		g = &groupState{}
		n.groups[key] = g
	}

	now := time.Now()
	var closed *groupState

	switch n.cfg.WindowType {
	case "tumbling":
		if n.cfg.WindowBy == "time" {
			if g.windowStart.IsZero() {
				g.windowStart = now
			}
			if now.Sub(g.windowStart) >= time.Duration(n.cfg.SizeMs)*time.Millisecond && len(g.items) > 0 {
				closed = &groupState{items: g.items}
				g.items = nil
				g.windowStart = now
			}
			g.items = append(g.items, in.Payload.Value)
		} else {
			g.items = append(g.items, in.Payload.Value)
			if len(g.items) >= n.cfg.Count {
				closed = &groupState{items: g.items}
				g.items = nil
			}
		}
	case "sliding":
		if n.cfg.WindowBy == "time" {
			g.items = append(g.items, in.Payload.Value)
			g.itemTimes = append(g.itemTimes, now)
			cutoff := now.Add(-time.Duration(n.cfg.SizeMs) * time.Millisecond)
			i := 0
			for i < len(g.itemTimes) && g.itemTimes[i].Before(cutoff) {
				i++
			}
			g.items = g.items[i:]
			g.itemTimes = g.itemTimes[i:]
			closed = &groupState{items: g.items}
		} else {
			g.items = append(g.items, in.Payload.Value)
			if len(g.items) > n.cfg.Count {
				g.items = g.items[len(g.items)-n.cfg.Count:]
			}
			closed = &groupState{items: g.items}
		}
	case "session":
		if !g.lastItem.IsZero() && now.Sub(g.lastItem) > time.Duration(n.cfg.SessionGapMs)*time.Millisecond && len(g.items) > 0 {
			closed = &groupState{items: g.items}
			g.items = nil
		}
		g.items = append(g.items, in.Payload.Value)
		g.lastItem = now
	}

	if closed == nil {
		return nil, nil
	}
	fields, err := n.computeAggregates(closed.items)
	if err != nil {
		return nil, err
	}
	out := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: fields})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func (n *node) computeAggregates(items []any) (map[string]any, error) {
	out := make(map[string]any, len(n.cfg.Aggregates))
	for _, a := range n.cfg.Aggregates {
		v, err := computeOne(items, a)
		if err != nil {
			return nil, fmt.Errorf("aggregate %q: %w", a.As, err)
		}
		out[a.As] = v
	}
	return out, nil
}

func computeOne(items []any, a AggregateOp) (any, error) {
	switch a.Op {
	case "count":
		return float64(len(items)), nil
	case "first":
		if len(items) == 0 {
			return nil, nil
		}
		return fieldValue(items[0], a.Field), nil
	case "last":
		if len(items) == 0 {
			return nil, nil
		}
		return fieldValue(items[len(items)-1], a.Field), nil
	case "collect":
		out := make([]any, len(items))
		for i, it := range items {
			out[i] = fieldValue(it, a.Field)
		}
		return out, nil
	}

	xs := make([]float64, 0, len(items))
	for _, it := range items {
		f, ok := toFloat(fieldValue(it, a.Field))
		if !ok {
			return nil, fmt.Errorf("field %q is not numeric in every item", a.Field)
		}
		xs = append(xs, f)
	}
	if len(xs) == 0 {
		return nil, fmt.Errorf("no items to aggregate")
	}

	switch a.Op {
	case "sum":
		s := 0.0
		for _, x := range xs {
			s += x
		}
		return s, nil
	case "avg":
		s := 0.0
		for _, x := range xs {
			s += x
		}
		return s / float64(len(xs)), nil
	case "min":
		m := xs[0]
		for _, x := range xs[1:] {
			if x < m {
				m = x
			}
		}
		return m, nil
	case "max":
		m := xs[0]
		for _, x := range xs[1:] {
			if x > m {
				m = x
			}
		}
		return m, nil
	case "stddev":
		if len(xs) < 2 {
			return 0.0, nil
		}
		mean := 0.0
		for _, x := range xs {
			mean += x
		}
		mean /= float64(len(xs))
		sq := 0.0
		for _, x := range xs {
			sq += (x - mean) * (x - mean)
		}
		return math.Sqrt(sq / float64(len(xs)-1)), nil
	case "percentile":
		sorted := append([]float64(nil), xs...)
		sort.Float64s(sorted)
		if len(sorted) == 1 {
			return sorted[0], nil
		}
		rank := (a.Percentile / 100) * float64(len(sorted)-1)
		lo := int(math.Floor(rank))
		hi := int(math.Ceil(rank))
		if lo == hi {
			return sorted[lo], nil
		}
		frac := rank - float64(lo)
		return sorted[lo]*(1-frac) + sorted[hi]*frac, nil
	default:
		return nil, fmt.Errorf("unhandled op %q", a.Op)
	}
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
