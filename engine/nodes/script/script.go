// Package script implements the "script" node (PROC-100): inline
// JavaScript with full read/write access to the current datagram
// (payload/header/tags), node/flow/global state, and multiple addressable
// output ports. Sandboxed via goja — no filesystem or network globals are
// ever bound in (SEC-150), and execution is bounded by a CPU/time limit
// (ENG-150) enforced through goja's Interrupt mechanism. Console output is
// forwarded to the debug sidebar (DBG-110) rather than the process's
// stdout.
package script

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dop251/goja"

	"github.com/1uedev/DataPipe/engine/ctxstore"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/nodeutil"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"code": {
			"type": "string",
			"description": "JavaScript source. Call emit(port, value) to produce one or more outputs; a plain trailing expression/return value is emitted on the first declared output port if emit was never called. console.log(...) is forwarded to the debug sidebar."
		},
		"outputs": {
			"type": "array",
			"items": { "type": "string" },
			"default": ["out"],
			"description": "Declared output port names (multiple output ports addressable from code, PROC-100)."
		},
		"timeoutMs": {
			"type": "integer",
			"minimum": 1,
			"default": 2000,
			"description": "CPU/time sandbox limit (ENG-150); the script is interrupted if it runs longer than this."
		}
	},
	"required": ["code"]
}`

func init() {
	flow.Register("script", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"}, // static fallback; DynamicOutputs below reflects the real, per-config ports
		DisplayName:  "Script",
		Category:     flow.CategoryProcessor,
		Description:  "Inline JavaScript with full read/write access to the datagram and node/flow/global state; sandboxed (PROC-100).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// DefaultTimeout is used when Config.TimeoutMs is unset.
const DefaultTimeout = 2 * time.Second

// Config is the "script" node's "config" object.
type Config struct {
	Code      string   `json:"code"`
	Outputs   []string `json:"outputs,omitempty"`
	TimeoutMs int      `json:"timeoutMs,omitempty"`
}

type node struct {
	cfg     Config
	prog    *goja.Program
	outputs []string
	timeout time.Duration
}

// New is the flow.Factory for the "script" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Code == "" {
		return nil, fmt.Errorf("script: code is required")
	}
	outputs := cfg.Outputs
	if len(outputs) == 0 {
		outputs = []string{"out"}
	}
	prog, err := goja.Compile("script", cfg.Code, false)
	if err != nil {
		return nil, fmt.Errorf("script: compiling: %w", err)
	}
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &node{cfg: cfg, prog: prog, outputs: outputs, timeout: timeout}, nil
}

// OutputPorts implements flow.DynamicOutputs: the real ports come from
// config, not a fixed type-registration-time list.
func (n *node) OutputPorts() []string { return n.outputs }

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	vm := goja.New()
	if err := expr.BindGlobals(vm, nodeutil.ExprData(ctx, in)); err != nil {
		return nil, fmt.Errorf("script: %w", err)
	}

	_, nodeID := flow.CurrentIDs(ctx)
	var results []flow.PortDatagram
	emit := func(port string, value any) {
		d := datagram.NewCaused(in, datagram.Source{NodeID: nodeID}, datagram.Payload{Value: value})
		results = append(results, flow.PortDatagram{Port: port, Datagram: d})
	}
	if err := vm.Set("emit", emit); err != nil {
		return nil, fmt.Errorf("script: %w", err)
	}
	if err := vm.Set("console", consoleObject(ctx, in)); err != nil {
		return nil, fmt.Errorf("script: %w", err)
	}
	if err := vm.Set("state", nodeStateObject(ctx)); err != nil {
		return nil, fmt.Errorf("script: %w", err)
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(n.timeout):
			vm.Interrupt("script: execution timed out")
		case <-ctx.Done():
			vm.Interrupt("script: context cancelled")
		case <-done:
		}
	}()
	v, err := vm.RunProgram(n.prog)
	close(done)
	if err != nil {
		return nil, fmt.Errorf("script: %w", err)
	}

	if len(results) == 0 && v != nil && !goja.IsUndefined(v) && !goja.IsNull(v) {
		emit(n.outputs[0], v.Export())
	}
	return results, nil
}

// consoleObject forwards console.log/warn/error to the debug sidebar
// (DBG-110) instead of the runtime process's own stdout/stderr.
func consoleObject(ctx context.Context, in datagram.Datagram) map[string]any {
	log := func(level string) func(args ...any) {
		return func(args ...any) {
			flow.SidebarEvent(ctx, "console."+level, in, fmt.Sprint(args...))
		}
	}
	return map[string]any{
		"log":   log("log"),
		"warn":  log("warn"),
		"error": log("error"),
	}
}

// nodeStateObject binds node-scoped state (PROC-410's node scope, on top of
// the flow/global scope engine/expr already provides via nodeutil.ExprData)
// — the Script node's own extra capability beyond a plain expression field.
func nodeStateObject(ctx context.Context) map[string]any {
	store, ok := flow.ContextStore(ctx)
	flowID, nodeID := flow.CurrentIDs(ctx)
	return map[string]any{
		"get": func(key string) any {
			if !ok {
				return goja.Undefined()
			}
			v, found, err := store.Get(ctx, ctxstore.Key{Scope: ctxstore.ScopeNode, FlowID: flowID, NodeID: nodeID, Name: key})
			if err != nil || !found {
				return goja.Undefined()
			}
			return v
		},
		"set": func(key string, value any) error {
			if !ok {
				return fmt.Errorf("script: no context store attached")
			}
			return store.Set(ctx, ctxstore.Key{Scope: ctxstore.ScopeNode, FlowID: flowID, NodeID: nodeID, Name: key}, value)
		},
	}
}
