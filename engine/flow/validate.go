// Validation implements Flow-File-Format.md §7: id uniqueness, wire
// endpoint/port/direction checks, registered node types, the ENG-100
// streaming/triggered mode check, and rule 3 (node config validates
// against its type's JSON Schema). Rule 2 (connection-ref resolution) is
// moot for control-plane-issued deploys, which reference control-plane
// connection ids already validated to exist there.
package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
)

// ValidationError collects every problem found so callers can show a
// navigable error list (UI-200) instead of failing on the first issue.
type ValidationError struct {
	Problems []string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("flow validation failed: %s", strings.Join(e.Problems, "; "))
}

// Validate checks f against the Flow-File-Format §7 rules this increment
// implements. It returns *ValidationError (with every problem found) or nil.
func Validate(f *FlowFile) error {
	var problems []string
	add := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }

	nodeByID := make(map[string]*Node, len(f.Graph.Nodes))
	for i := range f.Graph.Nodes {
		n := &f.Graph.Nodes[i]
		if _, dup := nodeByID[n.ID]; dup {
			add("duplicate node id %q", n.ID)
			continue
		}
		nodeByID[n.ID] = n
	}

	wireIDs := make(map[string]bool, len(f.Graph.Wires))
	hasSource := false

	nodeInfo := make(map[string]NodeTypeInfo, len(f.Graph.Nodes))
	nodeOutputs := make(map[string][]string, len(f.Graph.Nodes))
	for _, n := range f.Graph.Nodes {
		info, factory, ok := Lookup(n.Type)
		if !ok {
			add("node %q: unknown node type %q (is the plugin installed?)", n.ID, n.Type)
			continue
		}
		nodeInfo[n.ID] = info
		if info.Kind == KindSource {
			hasSource = true
		}

		// Rule 3: config must validate against the node type's JSON Schema
		// before even trying to construct it — a clearer error than
		// whatever Go-level failure an out-of-schema config happens to
		// produce, and skips the redundant second error below.
		if err := validateConfigSchema(info.ConfigSchema, n.Config); err != nil {
			add("node %q: config does not match schema: %v", n.ID, err)
			nodeOutputs[n.ID] = info.Outputs
			continue
		}

		// A node instance's output ports may depend on its own config
		// (DynamicOutputs, e.g. switch/route's user-defined rule ports —
		// Flow-File-Format.md §2 "switch: dynamic out0..outN + default"),
		// so config is validated here too: constructing the instance is
		// the only way to ask it for its real ports.
		outputs := info.Outputs
		if instance, err := factory(n.Config); err != nil {
			add("node %q: invalid config: %v", n.ID, err)
		} else if dyn, ok := instance.(DynamicOutputs); ok {
			outputs = dyn.OutputPorts()
		}
		nodeOutputs[n.ID] = outputs
	}

	for _, w := range f.Graph.Wires {
		if w.ID == "" {
			add("wire has no id")
		} else if wireIDs[w.ID] {
			add("duplicate wire id %q", w.ID)
		}
		wireIDs[w.ID] = true

		validateEndpoint(w.ID, "from", w.From, nodeByID, nodeInfo, add, func(info NodeTypeInfo, port string) bool {
			return outputPortExists(nodeOutputs[w.From.Node], nodeByID[w.From.Node], port)
		})
		validateEndpoint(w.ID, "to", w.To, nodeByID, nodeInfo, add, func(info NodeTypeInfo, port string) bool {
			return containsString(info.Inputs, port)
		})
	}

	if f.Mode == "" {
		add("mode is required (ENG-100)")
	} else if hasSource && f.Mode != ModeStreaming {
		add("flow contains a source node but mode is %q, want %q (ENG-100)", f.Mode, ModeStreaming)
	}

	if len(problems) > 0 {
		return &ValidationError{Problems: problems}
	}
	return nil
}

func validateEndpoint(
	wireID, side string,
	ep Endpoint,
	nodeByID map[string]*Node,
	nodeInfo map[string]NodeTypeInfo,
	add func(format string, args ...any),
	portOK func(info NodeTypeInfo, port string) bool,
) {
	if _, ok := nodeByID[ep.Node]; !ok {
		add("wire %q: %s node %q does not exist", wireID, side, ep.Node)
		return
	}
	info, ok := nodeInfo[ep.Node]
	if !ok {
		return // already reported: unknown node type
	}
	if !portOK(info, ep.Port) {
		add("wire %q: %s port %q does not exist on node %q", wireID, side, ep.Port, ep.Node)
	}
}

// outputPortExists checks the "from" side against the node instance's
// actual output ports (static NodeTypeInfo.Outputs, or DynamicOutputs'
// per-config ports where implemented), plus the implicit "error" port
// (Flow-File-Format §2 "Ports": "every node implicitly has error when
// errorPolicy.onError == 'errorPort'").
func outputPortExists(outputs []string, n *Node, port string) bool {
	if containsString(outputs, port) {
		return true
	}
	return port == "error" && n != nil && n.ErrorPolicy != nil && n.ErrorPolicy.OnError == "errorPort"
}

// validateConfigSchema checks config against a node type's declared JSON
// Schema (draft 2020-12). A type with no schema set (e.g. a test-only
// registration) is treated as unconstrained, not an error. config defaults
// to "{}" if empty, since an omitted config object is common for types with
// no required fields.
func validateConfigSchema(schemaJSON json.RawMessage, config json.RawMessage) error {
	if len(schemaJSON) == 0 {
		return nil
	}
	schema, err := jsonschema.CompileString("config.json", string(schemaJSON))
	if err != nil {
		return fmt.Errorf("node type's own schema is invalid: %w", err)
	}
	if len(config) == 0 {
		config = json.RawMessage("{}")
	}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.UseNumber()
	var doc any
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("invalid config JSON: %w", err)
	}
	return schema.Validate(doc)
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
