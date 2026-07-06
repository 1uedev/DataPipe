package flow

import (
	"encoding/json"
	"fmt"
	"sort"
	"sync"
)

// NodeKind distinguishes the two node shapes Increment 2 supports; sinks are
// just Processors with no declared output ports.
type NodeKind int

const (
	KindSource NodeKind = iota
	KindProcessor
)

// Category is UI-120's palette grouping: "sources green, processors blue,
// sinks orange, control violet".
type Category string

const (
	CategorySource    Category = "source"
	CategoryProcessor Category = "processor"
	CategorySink      Category = "sink"
	CategoryControl   Category = "control"
)

// NodeTypeInfo is the registry-side description of a node type: enough for
// the validator and graph builder to wire it up without knowing anything
// node-type-specific, plus the display metadata and JSON Schema the editor
// needs to render a palette entry and a generated config form (UI-110,
// UI-170; CLAUDE.md: "Node config UIs are generated from JSON Schema in the
// node manifest — never hand-build a config form for a specific node
// type").
type NodeTypeInfo struct {
	Kind    NodeKind
	Inputs  []string // empty for Source
	Outputs []string // ports the node can emit on, in addition to the implicit "error" port

	DisplayName  string
	Category     Category
	Description  string
	ConfigSchema json.RawMessage // a JSON Schema (draft 2020-12) for this type's "config" object
}

// Factory builds a running node instance from its raw, not-yet-validated
// config. It returns a Source or a Processor depending on the registered
// Kind.
type Factory func(config json.RawMessage) (any, error)

type registration struct {
	info    NodeTypeInfo
	factory Factory
}

var (
	registryMu sync.RWMutex
	registry   = map[string]registration{}
)

// Register adds a node type to the global registry. Node packages call this
// from an init() (engine/nodes/inject, .../set, .../debuglog, ...), the same
// pattern database/sql drivers use.
func Register(typeName string, info NodeTypeInfo, factory Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	if _, exists := registry[typeName]; exists {
		panic(fmt.Sprintf("flow: node type %q already registered", typeName))
	}
	registry[typeName] = registration{info: info, factory: factory}
}

// Lookup returns the registered type info and factory, if any.
func Lookup(typeName string) (NodeTypeInfo, Factory, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	r, ok := registry[typeName]
	return r.info, r.factory, ok
}

// NamedNodeTypeInfo pairs a registered type's name with its info, for
// listing (e.g. the editor's palette, GET /node-types).
type NamedNodeTypeInfo struct {
	Type string
	Info NodeTypeInfo
}

// ListNodeTypes returns every registered node type, sorted by Type.
func ListNodeTypes() []NamedNodeTypeInfo {
	registryMu.RLock()
	defer registryMu.RUnlock()

	list := make([]NamedNodeTypeInfo, 0, len(registry))
	for name, r := range registry {
		list = append(list, NamedNodeTypeInfo{Type: name, Info: r.info})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Type < list[j].Type })
	return list
}
