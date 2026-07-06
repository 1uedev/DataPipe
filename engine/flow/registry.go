package flow

import (
	"encoding/json"
	"fmt"
	"sync"
)

// NodeKind distinguishes the two node shapes Increment 2 supports; sinks are
// just Processors with no declared output ports.
type NodeKind int

const (
	KindSource NodeKind = iota
	KindProcessor
)

// NodeTypeInfo is the registry-side description of a node type: enough for
// the validator and graph builder to wire it up without knowing anything
// node-type-specific.
type NodeTypeInfo struct {
	Kind    NodeKind
	Inputs  []string // empty for Source
	Outputs []string // ports the node can emit on, in addition to the implicit "error" port
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
