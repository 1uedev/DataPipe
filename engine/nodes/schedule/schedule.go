// Package schedule implements the "schedule" node (CON-330): time triggers
// via cron expressions (with optional per-expression timezone, "calendar
// rules incl. time zones") or a fixed interval, emitting a trigger
// datagram on each firing.
package schedule

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["cron", "interval"], "description": "\"cron\" for a cron expression, \"interval\" for a fixed period." },
		"cron": { "type": "string", "description": "6-field cron expression (second minute hour day-of-month month day-of-week). Prefix with \"CRON_TZ=Region/City \" for a specific timezone." },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "Fixed firing period in milliseconds (mode \"interval\")." },
		"payload": { "description": "Optional literal payload for the emitted trigger datagram; defaults to the firing time." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("schedule", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Schedule",
		Category:     flow.CategorySource,
		Description:  "Time triggers: cron expressions (with timezone support) or fixed intervals (CON-330).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "schedule" node's "config" object.
type Config struct {
	Mode       string `json:"mode"`
	Cron       string `json:"cron,omitempty"`
	IntervalMs int    `json:"intervalMs,omitempty"`
	Payload    any    `json:"payload,omitempty"`
}

type node struct {
	cfg      Config
	schedule cron.Schedule // parsed once at construction time for mode "cron"
}

var cronParser = cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

// New is the flow.Factory for the "schedule" node type. A cron expression
// is parsed (and a bad one rejected) at construction time rather than only
// discovered when the node first runs.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}

	n := &node{cfg: cfg}
	switch cfg.Mode {
	case "cron":
		if cfg.Cron == "" {
			return nil, fmt.Errorf("schedule: cron expression is required in mode \"cron\"")
		}
		schedule, err := cronParser.Parse(cfg.Cron)
		if err != nil {
			return nil, fmt.Errorf("schedule: invalid cron expression %q: %w", cfg.Cron, err)
		}
		n.schedule = schedule
	case "interval":
		if cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("schedule: intervalMs must be positive in mode \"interval\"")
		}
	default:
		return nil, fmt.Errorf("schedule: mode must be \"cron\" or \"interval\", got %q", cfg.Mode)
	}
	return n, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	if n.cfg.Mode == "interval" {
		return n.runInterval(ctx, emit)
	}
	return n.runCron(ctx, emit)
}

func (n *node) runInterval(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	ticker := time.NewTicker(time.Duration(n.cfg.IntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-ticker.C:
			if err := emit("out", n.trigger(t)); err != nil {
				return err
			}
		}
	}
}

func (n *node) runCron(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	for {
		next := n.schedule.Next(time.Now())
		timer := time.NewTimer(time.Until(next))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
			if err := emit("out", n.trigger(next)); err != nil {
				return err
			}
		}
	}
}

func (n *node) trigger(firedAt time.Time) datagram.Datagram {
	value := n.cfg.Payload
	if value == nil {
		value = map[string]any{"firedAt": firedAt.UTC().Format(time.RFC3339)}
	}
	return datagram.New(datagram.Source{NodeID: "schedule", Origin: "schedule"}, datagram.Payload{Value: value})
}
