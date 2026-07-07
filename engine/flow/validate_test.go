package flow

import (
	"encoding/json"
	"strings"
	"testing"
)

const testSchemaProcessorSchema = `{
	"type": "object",
	"properties": {
		"count": { "type": "integer", "minimum": 1 }
	},
	"required": ["count"]
}`

func init() {
	Register("test-source", NodeTypeInfo{Kind: KindSource, Outputs: []string{"out"}}, func(json.RawMessage) (any, error) {
		return nil, nil
	})
	Register("test-processor", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, func(json.RawMessage) (any, error) {
		return nil, nil
	})
	Register("test-sink", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, func(json.RawMessage) (any, error) {
		return nil, nil
	})
	// A trigger source (ENG-100/ENG-130), for §7 rule 4 tests.
	Register("test-trigger", NodeTypeInfo{Kind: KindSource, Trigger: true, Outputs: []string{"out"}}, func(json.RawMessage) (any, error) {
		return nil, nil
	})
	// A schema-bearing type, for rule 3 (node-config JSON-Schema
	// validation) tests — the other test types above deliberately have no
	// ConfigSchema, to also prove that's treated as unconstrained rather
	// than an error.
	Register("test-schema-processor", NodeTypeInfo{
		Kind: KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"},
		ConfigSchema: json.RawMessage(testSchemaProcessorSchema),
	}, func(json.RawMessage) (any, error) { return nil, nil })
}

func validFlow() *FlowFile {
	return &FlowFile{
		FormatVersion: 1,
		Kind:          KindFlow,
		ID:            "flow_1",
		Name:          "test",
		Mode:          ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "n1", Type: "test-source", TypeVersion: 1},
				{ID: "n2", Type: "test-processor", TypeVersion: 1},
				{ID: "n3", Type: "test-sink", TypeVersion: 1},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "n1", Port: "out"}, To: Endpoint{Node: "n2", Port: "in"}},
				{ID: "w2", From: Endpoint{Node: "n2", Port: "out"}, To: Endpoint{Node: "n3", Port: "in"}},
			},
		},
	}
}

func TestFLOWVALIDATE_ValidFlowPasses(t *testing.T) {
	if err := Validate(validFlow()); err != nil {
		t.Fatalf("Validate(valid flow) = %v, want nil", err)
	}
}

func TestFLOWVALIDATE_DuplicateNodeID(t *testing.T) {
	f := validFlow()
	f.Graph.Nodes[1].ID = "n1"
	assertProblem(t, f, "duplicate node id")
}

func TestFLOWVALIDATE_DuplicateWireID(t *testing.T) {
	f := validFlow()
	f.Graph.Wires[1].ID = "w1"
	assertProblem(t, f, "duplicate wire id")
}

func TestFLOWVALIDATE_UnknownNodeType(t *testing.T) {
	f := validFlow()
	f.Graph.Nodes[0].Type = "does-not-exist"
	assertProblem(t, f, "unknown node type")
}

func TestFLOWVALIDATE_WireToNonexistentNode(t *testing.T) {
	f := validFlow()
	f.Graph.Wires[0].To.Node = "ghost"
	assertProblem(t, f, "does not exist")
}

func TestFLOWVALIDATE_WireToNonexistentInputPort(t *testing.T) {
	f := validFlow()
	f.Graph.Wires[0].To.Port = "bogus"
	assertProblem(t, f, `port "bogus" does not exist`)
}

func TestFLOWVALIDATE_WireFromNonexistentOutputPort(t *testing.T) {
	f := validFlow()
	f.Graph.Wires[0].From.Port = "bogus"
	assertProblem(t, f, `port "bogus" does not exist`)
}

func TestFLOWVALIDATE_WireIntoSourceOnlyNodeRejected(t *testing.T) {
	f := validFlow()
	// n1 is a Source (test-source) with no declared Inputs; wiring into it
	// must be rejected ("no wire into a source-only port", §7 rule 1).
	f.Graph.Wires = append(f.Graph.Wires, Wire{
		ID:   "w3",
		From: Endpoint{Node: "n3", Port: "out"}, // n3 (test-sink) doesn't even have an "out" — also invalid, but we care about the "to" side here
		To:   Endpoint{Node: "n1", Port: "in"},
	})
	assertProblem(t, f, `port "in" does not exist on node "n1"`)
}

func TestFLOWVALIDATE_ImplicitErrorPortAllowedWhenConfigured(t *testing.T) {
	f := validFlow()
	f.Graph.Nodes[1].ErrorPolicy = &ErrorPolicy{OnError: "errorPort"}
	f.Graph.Wires = append(f.Graph.Wires, Wire{
		ID:   "w-err",
		From: Endpoint{Node: "n2", Port: "error"},
		To:   Endpoint{Node: "n3", Port: "in"},
	})
	if err := Validate(f); err != nil {
		t.Fatalf("Validate with configured error port = %v, want nil", err)
	}
}

func TestFLOWVALIDATE_ImplicitErrorPortRejectedWithoutOnErrorPort(t *testing.T) {
	f := validFlow()
	// n2 has no errorPolicy at all, so "error" is not a valid output port.
	f.Graph.Wires = append(f.Graph.Wires, Wire{
		ID:   "w-err",
		From: Endpoint{Node: "n2", Port: "error"},
		To:   Endpoint{Node: "n3", Port: "in"},
	})
	assertProblem(t, f, `port "error" does not exist on node "n2"`)
}

func TestFLOWVALIDATE_ModeMissing(t *testing.T) {
	f := validFlow()
	f.Mode = ""
	assertProblem(t, f, "mode is required")
}

func TestFLOWVALIDATE_ModeMismatchWithSourceNode(t *testing.T) {
	f := validFlow()
	f.Mode = ModeTriggered
	assertProblem(t, f, "flow contains a source node but mode")
}

func triggerFlow() *FlowFile {
	return &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_trigger", Name: "t", Mode: ModeTriggered,
		Graph: Graph{
			Nodes: []Node{
				{ID: "n1", Type: "test-trigger", TypeVersion: 1},
				{ID: "n2", Type: "test-sink", TypeVersion: 1},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "n1", Port: "out"}, To: Endpoint{Node: "n2", Port: "in"}},
			},
		},
	}
}

func TestFLOWVALIDATE_TriggeredFlowWithTriggerNodePasses(t *testing.T) {
	if err := Validate(triggerFlow()); err != nil {
		t.Fatalf("Validate(valid trigger flow) = %v, want nil", err)
	}
}

func TestFLOWVALIDATE_TriggeredModeWithoutTriggerNodeRejected(t *testing.T) {
	f := triggerFlow()
	f.Graph.Nodes[0].Type = "test-sink" // no entry node at all now (n2 already has an input)
	f.Graph.Wires = nil
	assertProblem(t, f, "triggered flows must start with a trigger node")
}

func TestFLOWVALIDATE_TriggerNodeWithStreamingModeRejected(t *testing.T) {
	f := triggerFlow()
	f.Mode = ModeStreaming
	assertProblem(t, f, "flow contains a trigger node but mode")
}

func TestFLOWVALIDATE_MixingSourceAndTriggerEntryNodesRejected(t *testing.T) {
	f := triggerFlow()
	f.Graph.Nodes = append(f.Graph.Nodes, Node{ID: "n3", Type: "test-source", TypeVersion: 1})
	assertProblem(t, f, "mixes a streaming source node with a trigger node")
}

func TestFLOWVALIDATE_MultipleProblemsAllReported(t *testing.T) {
	f := validFlow()
	f.Graph.Nodes[1].ID = "n1"
	f.Graph.Wires[0].To.Node = "ghost"
	err := Validate(f)
	if err == nil {
		t.Fatal("expected validation error")
	}
	ve, ok := err.(*ValidationError)
	if !ok {
		t.Fatalf("error type = %T, want *ValidationError", err)
	}
	if len(ve.Problems) < 2 {
		t.Errorf("expected multiple problems reported together, got %v", ve.Problems)
	}
}

func TestFLOWVALIDATE_ConfigMatchingSchemaPasses(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_schema_ok", Name: "t", Mode: ModeStreaming,
		Graph: Graph{Nodes: []Node{{ID: "n1", Type: "test-schema-processor", Config: json.RawMessage(`{"count": 3}`)}}},
	}
	if err := Validate(f); err != nil {
		t.Fatalf("Validate: %v, want nil", err)
	}
}

func TestFLOWVALIDATE_ConfigViolatingSchemaIsRejected(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_schema_bad", Name: "t", Mode: ModeStreaming,
		Graph: Graph{Nodes: []Node{{ID: "n1", Type: "test-schema-processor", Config: json.RawMessage(`{"count": "not-a-number"}`)}}},
	}
	assertProblem(t, f, "config does not match schema")
}

func TestFLOWVALIDATE_MissingRequiredConfigFieldIsRejected(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_schema_missing", Name: "t", Mode: ModeStreaming,
		Graph: Graph{Nodes: []Node{{ID: "n1", Type: "test-schema-processor", Config: json.RawMessage(`{}`)}}},
	}
	assertProblem(t, f, "config does not match schema")
}

func TestFLOWVALIDATE_TypeWithNoSchemaIsUnconstrained(t *testing.T) {
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_no_schema", Name: "t", Mode: ModeStreaming,
		Graph: Graph{Nodes: []Node{{ID: "n1", Type: "test-processor", Config: json.RawMessage(`{"anything": true}`)}}},
	}
	if err := Validate(f); err != nil {
		t.Fatalf("Validate: %v, want nil (no schema declared)", err)
	}
}

func assertProblem(t *testing.T, f *FlowFile, substr string) {
	t.Helper()
	err := Validate(f)
	if err == nil {
		t.Fatalf("Validate: expected error containing %q, got nil", substr)
	}
	if !strings.Contains(err.Error(), substr) {
		t.Errorf("Validate error = %v, want it to contain %q", err, substr)
	}
}
