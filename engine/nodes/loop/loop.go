// Package loop implements the "loop" node (PROC-340): iterate an array
// payload one item at a time over loop-back wiring — this node's "loop"
// output is wired to a sub-processing chain whose own output wires back to
// this node's "in" port, and the "done" output fires once every item has
// been through (or, defensively, once maxIterations is hit) — with
// guaranteed termination even if the flow is misconfigured.
//
// A continuation is recognized by DGM-160's CorrelationID, which every
// well-behaved node propagates unchanged through datagram.NewCaused: the
// session state (item array + current index) lives in this node instance's
// own in-process map, keyed by the triggering datagram's correlation id, so
// nothing needs to round-trip through the loop-back wire itself. This
// assumes the loop-back sub-flow is a simple chain (no fan-out/fan-in that
// would create multiple concurrent continuations for one correlation id).
//
// Flow-File-Format.md §7 rule 5 ("loops only through the loop node's
// designated loop port; other cycles are rejected") is a general
// graph-cycle-validation feature not yet implemented in engine/flow.Validate
// — see TODO.md.
package loop

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"itemsField": { "type": "string", "description": "\".\"-separated path to the array to iterate; empty iterates the whole trigger payload." },
		"maxIterations": { "type": "integer", "minimum": 1, "default": 100000, "description": "Guaranteed termination cap, independent of the array's own length." }
	}
}`

func init() {
	flow.Register("loop", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"loop", "done"},
		DisplayName:  "Loop",
		Category:     flow.CategoryControl,
		Description:  "Iterate array items one at a time over loop-back wiring, with a guaranteed-termination max-iterations cap (PROC-340).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// DefaultMaxIterations is used when Config.MaxIterations is unset.
const DefaultMaxIterations = 100000

// Config is the "loop" node's "config" object.
type Config struct {
	ItemsField    string `json:"itemsField,omitempty"`
	MaxIterations int    `json:"maxIterations,omitempty"`
}

type session struct {
	items []any
	index int
}

type node struct {
	cfg      Config
	sessions map[string]*session
}

// New is the flow.Factory for the "loop" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
	}
	if cfg.MaxIterations <= 0 {
		cfg.MaxIterations = DefaultMaxIterations
	}
	return &node{cfg: cfg, sessions: map[string]*session{}}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	corrID := in.Header.CorrelationID
	if sess, ok := n.sessions[corrID]; ok {
		return n.continueSession(in, corrID, sess)
	}
	return n.startSession(in, corrID)
}

func (n *node) startSession(in datagram.Datagram, corrID string) ([]flow.PortDatagram, error) {
	value := fieldValue(in.Payload.Value, n.cfg.ItemsField)
	items, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("loop: field %q is not an array (got %T)", n.cfg.ItemsField, value)
	}
	if len(items) == 0 {
		return []flow.PortDatagram{doneResult(in, items)}, nil
	}
	n.sessions[corrID] = &session{items: items, index: 0}
	return []flow.PortDatagram{loopResult(in, items[0])}, nil
}

func (n *node) continueSession(in datagram.Datagram, corrID string, sess *session) ([]flow.PortDatagram, error) {
	sess.index++
	if sess.index >= len(sess.items) || sess.index >= n.cfg.MaxIterations {
		delete(n.sessions, corrID)
		return []flow.PortDatagram{doneResult(in, sess.items)}, nil
	}
	return []flow.PortDatagram{loopResult(in, sess.items[sess.index])}, nil
}

func loopResult(cause datagram.Datagram, item any) flow.PortDatagram {
	d := datagram.NewCaused(cause, cause.Header.Source, datagram.Payload{Value: item})
	return flow.PortDatagram{Port: "loop", Datagram: d}
}

func doneResult(cause datagram.Datagram, items []any) flow.PortDatagram {
	d := datagram.NewCaused(cause, cause.Header.Source, datagram.Payload{Value: map[string]any{
		"count": float64(len(items)),
	}})
	return flow.PortDatagram{Port: "done", Datagram: d}
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
