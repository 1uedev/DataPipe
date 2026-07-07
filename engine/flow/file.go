// Package flow implements the flow file model, graph instantiation, and
// node lifecycle described in docs/Flow-File-Format.md and
// DataPipe-Requirements-Specification.md §14 (Development-Plan Increment 2).
package flow

import "encoding/json"

// Mode values (ENG-100).
const (
	ModeStreaming = "streaming"
	ModeTriggered = "triggered"
)

// Kind values.
const (
	KindFlow    = "flow"
	KindSubflow = "subflow"
)

// FlowFile is the top-level document described by Flow-File-Format.md §2.
type FlowFile struct {
	FormatVersion     int                `json:"formatVersion"`
	Kind              string             `json:"kind"`
	ID                string             `json:"id"`
	Name              string             `json:"name"`
	Description       string             `json:"description,omitempty"`
	Mode              string             `json:"mode,omitempty"`
	Disabled          bool               `json:"disabled,omitempty"`
	RuntimeAssignment *RuntimeAssignment `json:"runtimeAssignment,omitempty"`
	Settings          Settings           `json:"settings,omitempty"`
	Env               []EnvVar           `json:"env,omitempty"`
	Graph             Graph              `json:"graph"`
	Layout            *Layout            `json:"layout,omitempty"`
	// Interface is only present for Kind == KindSubflow (§3); subflow
	// instantiation (subflow:<id> node types, PROC-160) is not implemented
	// until a later increment, but the field round-trips.
	Interface *SubflowInterface `json:"interface,omitempty"`
}

// RuntimeAssignment is UI-220's deploy-target selection; nil means the
// default runtime.
type RuntimeAssignment struct {
	Group string `json:"group,omitempty"`
}

// Settings are flow-level execution settings.
type Settings struct {
	ErrorFlow          string `json:"errorFlow,omitempty"`
	GuaranteedDelivery bool   `json:"guaranteedDelivery,omitempty"`
	MaxConcurrency     *int   `json:"maxConcurrency,omitempty"`
	// ConcurrencyPolicy is "queue" (default, blocks new triggers until a
	// slot frees) or "reject" (ENG-130), applied once MaxConcurrency is
	// reached. Triggered mode only.
	ConcurrencyPolicy  string `json:"concurrencyPolicy,omitempty"`
	ExecutionTimeoutMs *int   `json:"executionTimeoutMs,omitempty"`
}

// EnvVar is a flow-level variable, overridable by environment profiles
// (VCS-140).
type EnvVar struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Default any    `json:"default,omitempty"`
}

// Graph holds the node/wire definitions.
type Graph struct {
	Nodes []Node `json:"nodes"`
	Wires []Wire `json:"wires"`
}

// Node is one instance of a node type in the graph.
type Node struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	TypeVersion int             `json:"typeVersion"`
	Name        string          `json:"name,omitempty"`
	Disabled    bool            `json:"disabled,omitempty"`
	Connection  string          `json:"connection,omitempty"`
	Config      json.RawMessage `json:"config,omitempty"`
	ErrorPolicy *ErrorPolicy    `json:"errorPolicy,omitempty"`
	// Overflow is the BUS-110 per-input policy: "block" | "dropOldest" |
	// "dropNewest" | "sample:n".
	Overflow string `json:"overflow,omitempty"`
}

// ErrorPolicy is ERR-100's uniform per-node error handling.
type ErrorPolicy struct {
	// OnError is "fail" | "retry" | "errorPort" | "discard" | "storeForward".
	OnError      string              `json:"onError,omitempty"`
	Retry        *RetryPolicy        `json:"retry,omitempty"`
	StoreForward *StoreForwardPolicy `json:"storeForward,omitempty"`
}

// RetryPolicy configures the "retry" OnError mode.
type RetryPolicy struct {
	Max          int  `json:"max,omitempty"`
	BackoffMs    int  `json:"backoffMs,omitempty"`
	MaxBackoffMs int  `json:"maxBackoffMs,omitempty"`
	Jitter       bool `json:"jitter,omitempty"`
}

// StoreForwardPolicy configures the "storeForward" OnError mode (EDGE-130):
// a size- and time-bounded durable on-disk queue, drained in the background
// until delivery succeeds. Zero means unbounded on that dimension.
type StoreForwardPolicy struct {
	MaxSizeMb int `json:"maxSizeMb,omitempty"`
	MaxAgeSec int `json:"maxAgeSec,omitempty"`
}

// Wire connects one node's output port to another node's input port.
type Wire struct {
	ID   string   `json:"id"`
	From Endpoint `json:"from"`
	To   Endpoint `json:"to"`
}

// Endpoint identifies a (node, port) pair.
type Endpoint struct {
	Node string `json:"node"`
	Port string `json:"port"`
}

// Layout is purely cosmetic (positions, groups, notes) and never affects
// graph semantics.
type Layout struct {
	Nodes  map[string]NodePosition `json:"nodes,omitempty"`
	Groups []Group                 `json:"groups,omitempty"`
	Notes  []Note                  `json:"notes,omitempty"`
}

type NodePosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type Group struct {
	ID    string   `json:"id"`
	Label string   `json:"label,omitempty"`
	Nodes []string `json:"nodes,omitempty"`
	Color string   `json:"color,omitempty"`
}

type Note struct {
	ID string  `json:"id"`
	X  float64 `json:"x"`
	Y  float64 `json:"y"`
	Md string  `json:"md,omitempty"`
}

// SubflowInterface is added to flow files with Kind == KindSubflow (§3).
type SubflowInterface struct {
	Inputs  []Port         `json:"inputs,omitempty"`
	Outputs []Port         `json:"outputs,omitempty"`
	Params  []SubflowParam `json:"params,omitempty"`
}

type Port struct {
	Port        string `json:"port"`
	Description string `json:"description,omitempty"`
}

type SubflowParam struct {
	Name           string `json:"name"`
	Type           string `json:"type"`
	Default        any    `json:"default,omitempty"`
	ConnectionType string `json:"connectionType,omitempty"`
}
