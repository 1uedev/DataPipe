// Validation implements the subset of Flow-File-Format.md §7 that applies
// once node types exist: id uniqueness, wire endpoint/port/direction
// checks, registered node types, and the ENG-100 streaming/triggered mode
// check. Connection-reference and JSON-Schema config validation (§7 rules
// 2-3) land with the connection registry (Increment 3+) and node-manifest
// schemas (SDK track).
package flow

import (
	"fmt"
	"strings"
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
	for _, n := range f.Graph.Nodes {
		info, _, ok := Lookup(n.Type)
		if !ok {
			add("node %q: unknown node type %q (is the plugin installed?)", n.ID, n.Type)
			continue
		}
		nodeInfo[n.ID] = info
		if info.Kind == KindSource {
			hasSource = true
		}
	}

	for _, w := range f.Graph.Wires {
		if w.ID == "" {
			add("wire has no id")
		} else if wireIDs[w.ID] {
			add("duplicate wire id %q", w.ID)
		}
		wireIDs[w.ID] = true

		validateEndpoint(w.ID, "from", w.From, nodeByID, nodeInfo, add, func(info NodeTypeInfo, port string) bool {
			return outputPortExists(info, nodeByID[w.From.Node], port)
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

// outputPortExists checks the "to" side against the node type's declared
// outputs, plus the implicit "error" port (Flow-File-Format §2 "Ports":
// "every node implicitly has error when errorPolicy.onError == 'errorPort'").
func outputPortExists(info NodeTypeInfo, n *Node, port string) bool {
	if containsString(info.Outputs, port) {
		return true
	}
	return port == "error" && n != nil && n.ErrorPolicy != nil && n.ErrorPolicy.OnError == "errorPort"
}

func containsString(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
