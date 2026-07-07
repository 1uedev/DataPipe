// Package delay implements the "delay" node (PROC-350): a fixed or
// expression-computed delay, or rate limiting (n per interval, optionally
// per key) with scheduled release rather than dropping. Both modes work by
// blocking this node's own Process call for as long as needed — safe
// because each node instance already runs its receive-process loop on its
// own dedicated goroutine (engine/flow.nodeRunner), so holding one up only
// paces that node's own throughput, exactly the point of a delay/throttle.
package delay

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
		"mode": { "type": "string", "enum": ["delay", "throttle"] },
		"delayMs": { "type": "integer", "minimum": 0, "description": "mode \"delay\": fixed delay in milliseconds." },
		"delayExpression": { "type": "string", "description": "mode \"delay\": JavaScript expression computing the delay in milliseconds; overrides delayMs if set." },
		"maxPerInterval": { "type": "integer", "minimum": 1, "description": "mode \"throttle\": at most this many datagrams pass per intervalMs." },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "mode \"throttle\": the rate-limit window." },
		"groupBy": { "type": "string", "description": "mode \"throttle\": JavaScript expression keying independent rate limits per group; empty shares one limit." },
		"timeoutMs": { "type": "integer", "minimum": 1, "default": 2000 }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("delay", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Delay/Throttle",
		Category:     flow.CategoryControl,
		Description:  "Fixed/expression delay, or rate limiting (n per interval, optionally per key) with scheduled release (PROC-350).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "delay" node's "config" object.
type Config struct {
	Mode            string `json:"mode"`
	DelayMs         int    `json:"delayMs,omitempty"`
	DelayExpression string `json:"delayExpression,omitempty"`
	MaxPerInterval  int    `json:"maxPerInterval,omitempty"`
	IntervalMs      int    `json:"intervalMs,omitempty"`
	GroupBy         string `json:"groupBy,omitempty"`
	TimeoutMs       int    `json:"timeoutMs,omitempty"`
}

type node struct {
	cfg         Config
	delayProg   *expr.Program
	groupProg   *expr.Program
	rt          *expr.Runtime
	timeout     time.Duration
	windowByKey map[string][]time.Time
}

// New is the flow.Factory for the "delay" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	n := &node{cfg: cfg, rt: expr.New(), timeout: time.Duration(cfg.TimeoutMs) * time.Millisecond, windowByKey: map[string][]time.Time{}}

	switch cfg.Mode {
	case "delay":
		if cfg.DelayExpression != "" {
			prog, err := expr.Compile(cfg.DelayExpression)
			if err != nil {
				return nil, fmt.Errorf("delay: delayExpression: %w", err)
			}
			n.delayProg = prog
		}
	case "throttle":
		if cfg.MaxPerInterval <= 0 || cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("delay: maxPerInterval and intervalMs are required for mode \"throttle\"")
		}
		if cfg.GroupBy != "" {
			prog, err := expr.Compile(cfg.GroupBy)
			if err != nil {
				return nil, fmt.Errorf("delay: groupBy: %w", err)
			}
			n.groupProg = prog
		}
	default:
		return nil, fmt.Errorf("delay: unknown mode %q", cfg.Mode)
	}
	return n, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	if n.cfg.Mode == "delay" {
		if err := n.applyDelay(ctx, in); err != nil {
			return nil, err
		}
	} else {
		if err := n.applyThrottle(ctx, in); err != nil {
			return nil, err
		}
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) applyDelay(ctx context.Context, in datagram.Datagram) error {
	ms := n.cfg.DelayMs
	if n.delayProg != nil {
		v, err := n.rt.Run(ctx, n.delayProg, nodeutil.ExprData(ctx, in), n.timeout)
		if err != nil {
			return fmt.Errorf("delay: delayExpression: %w", err)
		}
		f, ok := toFloat(v)
		if !ok {
			return fmt.Errorf("delay: delayExpression did not evaluate to a number (got %T)", v)
		}
		ms = int(f)
	}
	if ms <= 0 {
		return nil
	}
	return sleep(ctx, time.Duration(ms)*time.Millisecond)
}

func (n *node) applyThrottle(ctx context.Context, in datagram.Datagram) error {
	key := "_"
	if n.groupProg != nil {
		v, err := n.rt.Run(ctx, n.groupProg, nodeutil.ExprData(ctx, in), n.timeout)
		if err != nil {
			return fmt.Errorf("delay: groupBy: %w", err)
		}
		key = fmt.Sprint(v)
	}

	interval := time.Duration(n.cfg.IntervalMs) * time.Millisecond
	for {
		now := time.Now()
		window := pruneWindow(n.windowByKey[key], now, interval)
		if len(window) < n.cfg.MaxPerInterval {
			n.windowByKey[key] = append(window, now)
			return nil
		}
		wait := interval - now.Sub(window[0])
		if wait < 0 {
			wait = 0
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
		// loop again: after sleeping, re-check with a fresh now/prune —
		// under concurrent load elsewhere this could require more than one
		// pass, though in practice one is enough since only this goroutine
		// ever writes to windowByKey[key].
	}
}

func pruneWindow(timestamps []time.Time, now time.Time, interval time.Duration) []time.Time {
	cutoff := now.Add(-interval)
	i := 0
	for i < len(timestamps) && timestamps[i].Before(cutoff) {
		i++
	}
	return timestamps[i:]
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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
