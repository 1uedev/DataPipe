// Package merge implements the "merge" node (PROC-320): two named input
// ports ("a", "b") combined per mode — concatenate (pass every arrival
// straight through), combine-latest (emit the newest pair once both sides
// have been seen at least once), join on a key within a time window
// (inner/left), or batch-merge parallel branches sharing one correlationId.
//
// Like PROC-210, matching is checked reactively (on the next arrival on
// either port), not proactively on a background timer, because
// engine/flow.MultiInputProcessor is as purely reactive as Processor: there
// is no lifecycle hook for a node to run or clean up its own background
// goroutine. A "left" join's unmatched side is only emitted once another
// arrival on either port triggers the lazy expiry check — a genuinely idle
// pending item can sit unmatched-and-unemitted indefinitely. Documented in
// TODO.md.
package merge

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
		"mode": { "type": "string", "enum": ["concatenate", "combineLatest", "join", "batchMerge"] },
		"keyA": { "type": "string", "description": "JavaScript expression extracting the join key from an \"a\"-port datagram (modes combineLatest/join)." },
		"keyB": { "type": "string", "description": "JavaScript expression extracting the join key from a \"b\"-port datagram (modes combineLatest/join)." },
		"windowMs": { "type": "integer", "minimum": 1, "description": "Maximum time between matching a/b arrivals (mode \"join\")." },
		"joinType": { "type": "string", "enum": ["inner", "left"], "default": "inner" },
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("merge", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"a", "b"},
		Outputs:      []string{"out"},
		DisplayName:  "Merge/Join",
		Category:     flow.CategoryControl,
		Description:  "Concatenate, combine-latest, windowed join (inner/left), or correlationId batch-merge of two branches (PROC-320).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "merge" node's "config" object.
type Config struct {
	Mode      string `json:"mode"`
	KeyA      string `json:"keyA,omitempty"`
	KeyB      string `json:"keyB,omitempty"`
	WindowMs  int    `json:"windowMs,omitempty"`
	JoinType  string `json:"joinType,omitempty"`
	TimeoutMs int    `json:"timeoutMs,omitempty"`
}

type pendingItem struct {
	datagram datagram.Datagram
	arrived  time.Time
}

type node struct {
	cfg     Config
	keyA    *expr.Program
	keyB    *expr.Program
	rt      *expr.Runtime
	timeout time.Duration
	window  time.Duration

	// combineLatest
	lastA, lastB *datagram.Datagram

	// join: keyed pending items waiting for a match from the other side.
	pendingA, pendingB map[string]pendingItem

	// batchMerge: pending item per correlationId, one slot per side.
	batchByCorrelation map[string]*batchPair
}

type batchPair struct {
	a, b *datagram.Datagram
}

// New is the flow.Factory for the "merge" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	n := &node{
		cfg:                cfg,
		rt:                 expr.New(),
		timeout:            time.Duration(cfg.TimeoutMs) * time.Millisecond,
		pendingA:           map[string]pendingItem{},
		pendingB:           map[string]pendingItem{},
		batchByCorrelation: map[string]*batchPair{},
	}
	if cfg.JoinType == "" {
		n.cfg.JoinType = "inner"
	}

	switch cfg.Mode {
	case "concatenate", "batchMerge":
		// no extra config needed
	case "combineLatest":
		if cfg.KeyA == "" || cfg.KeyB == "" {
			return nil, fmt.Errorf("merge: keyA and keyB are required for mode %q", cfg.Mode)
		}
	case "join":
		if cfg.KeyA == "" || cfg.KeyB == "" {
			return nil, fmt.Errorf("merge: keyA and keyB are required for mode \"join\"")
		}
		if cfg.WindowMs <= 0 {
			return nil, fmt.Errorf("merge: windowMs is required for mode \"join\"")
		}
		if n.cfg.JoinType != "inner" && n.cfg.JoinType != "left" {
			return nil, fmt.Errorf("merge: unknown joinType %q", n.cfg.JoinType)
		}
		n.window = time.Duration(cfg.WindowMs) * time.Millisecond
	default:
		return nil, fmt.Errorf("merge: unknown mode %q", cfg.Mode)
	}

	if cfg.KeyA != "" {
		prog, err := expr.Compile(cfg.KeyA)
		if err != nil {
			return nil, fmt.Errorf("merge: keyA: %w", err)
		}
		n.keyA = prog
	}
	if cfg.KeyB != "" {
		prog, err := expr.Compile(cfg.KeyB)
		if err != nil {
			return nil, fmt.Errorf("merge: keyB: %w", err)
		}
		n.keyB = prog
	}
	return n, nil
}

func (n *node) ProcessPort(ctx context.Context, port string, in datagram.Datagram) ([]flow.PortDatagram, error) {
	switch n.cfg.Mode {
	case "concatenate":
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	case "combineLatest":
		return n.processCombineLatest(ctx, port, in)
	case "join":
		return n.processJoin(ctx, port, in)
	case "batchMerge":
		return n.processBatchMerge(port, in)
	default:
		return nil, fmt.Errorf("merge: unhandled mode %q", n.cfg.Mode)
	}
}

func (n *node) processCombineLatest(ctx context.Context, port string, in datagram.Datagram) ([]flow.PortDatagram, error) {
	if port == "a" {
		n.lastA = &in
	} else {
		n.lastB = &in
	}
	if n.lastA == nil || n.lastB == nil {
		return nil, nil
	}
	out := combinedDatagram(*n.lastA, *n.lastB)
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func (n *node) processJoin(ctx context.Context, port string, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data := nodeutil.ExprData(ctx, in)
	prog := n.keyA
	other := n.pendingB
	if port == "b" {
		prog = n.keyB
		other = n.pendingA
	}
	keyName := "keyA"
	if port == "b" {
		keyName = "keyB"
	}
	v, err := n.rt.Run(ctx, prog, data, n.timeout)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", keyName, err)
	}
	key := fmt.Sprint(v)
	now := time.Now()

	var results []flow.PortDatagram
	if match, ok := other[key]; ok && now.Sub(match.arrived) <= n.window {
		delete(other, key)
		if port == "a" {
			results = append(results, flow.PortDatagram{Port: "out", Datagram: combinedDatagram(in, match.datagram)})
		} else {
			results = append(results, flow.PortDatagram{Port: "out", Datagram: combinedDatagram(match.datagram, in)})
		}
	} else {
		if ok {
			delete(other, key) // stale match, evict
		}
		mine := n.pendingA
		if port == "b" {
			mine = n.pendingB
		}
		mine[key] = pendingItem{datagram: in, arrived: now}
	}

	results = append(results, n.expireStale(now)...)
	return results, nil
}

// expireStale evicts pending items older than the join window; a "left"
// join emits the expired side paired with a nil counterpart instead of
// silently dropping it.
func (n *node) expireStale(now time.Time) []flow.PortDatagram {
	var out []flow.PortDatagram
	for key, item := range n.pendingA {
		if now.Sub(item.arrived) > n.window {
			delete(n.pendingA, key)
			if n.cfg.JoinType == "left" {
				out = append(out, flow.PortDatagram{Port: "out", Datagram: combinedDatagramNilB(item.datagram)})
			}
		}
	}
	for key, item := range n.pendingB {
		if now.Sub(item.arrived) > n.window {
			delete(n.pendingB, key)
			// Only the "a" side is left-preserved, matching SQL LEFT JOIN
			// semantics (left = "a"); an unmatched "b" is simply dropped.
		}
	}
	return out
}

func (n *node) processBatchMerge(port string, in datagram.Datagram) ([]flow.PortDatagram, error) {
	corrID := in.Header.CorrelationID
	if corrID == "" {
		return nil, fmt.Errorf("batchMerge: datagram has no correlationId to merge on")
	}
	pair, ok := n.batchByCorrelation[corrID]
	if !ok {
		pair = &batchPair{}
		n.batchByCorrelation[corrID] = pair
	}
	if port == "a" {
		pair.a = &in
	} else {
		pair.b = &in
	}
	if pair.a == nil || pair.b == nil {
		return nil, nil
	}
	delete(n.batchByCorrelation, corrID)
	out := combinedDatagram(*pair.a, *pair.b)
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func combinedDatagram(a, b datagram.Datagram) datagram.Datagram {
	return datagram.NewCaused(a, a.Header.Source, datagram.Payload{Value: map[string]any{
		"a": a.Payload.Value,
		"b": b.Payload.Value,
	}})
}

func combinedDatagramNilB(a datagram.Datagram) datagram.Datagram {
	return datagram.NewCaused(a, a.Header.Source, datagram.Payload{Value: map[string]any{
		"a": a.Payload.Value,
		"b": nil,
	}})
}
