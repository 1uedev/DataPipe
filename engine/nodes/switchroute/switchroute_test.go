package switchroute

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func newTestNode(t *testing.T, cfg Config) *node {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return instance.(*node)
}

func TestPROC300_NewRequiresRules(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error: rules is required")
	}
}

func TestPROC300_NewRejectsUnknownMode(t *testing.T) {
	cfg := Config{Rules: []Rule{{Expression: "true"}}, Mode: "bogus"}
	raw, _ := json.Marshal(cfg)
	if _, err := New(raw); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
}

func TestPROC300_OutputPortsAreDynamicOut0ThroughNPlusDefault(t *testing.T) {
	n := newTestNode(t, Config{Rules: []Rule{{Expression: "true"}, {Expression: "false"}, {Expression: "false"}}})
	got := n.OutputPorts()
	want := []string{"out0", "out1", "out2", "default"}
	if len(got) != len(want) {
		t.Fatalf("OutputPorts() = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("OutputPorts()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestPROC300_FirstMatchRoutesToFirstMatchingRuleOnly(t *testing.T) {
	n := newTestNode(t, Config{
		Rules: []Rule{
			{Expression: "payload.value > 100"},
			{Expression: "payload.value > 10"},
		},
		Mode: "firstMatch",
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 50.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "out1" {
		t.Fatalf("results = %+v, want exactly out1", results)
	}
}

func TestPROC300_AllMatchesRoutesToEveryMatchingRule(t *testing.T) {
	n := newTestNode(t, Config{
		Rules: []Rule{
			{Expression: "payload.value > 10"},
			{Expression: "payload.value < 1000"},
		},
		Mode: "allMatches",
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 50.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 2 || results[0].Port != "out0" || results[1].Port != "out1" {
		t.Fatalf("results = %+v, want out0 and out1", results)
	}
}

func TestPROC300_NoMatchRoutesToDefault(t *testing.T) {
	n := newTestNode(t, Config{Rules: []Rule{{Expression: "payload.value > 1000"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 1.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "default" {
		t.Fatalf("results = %+v, want exactly default", results)
	}
}

func TestPROC300_RegexAndTagAndQualityPredicates(t *testing.T) {
	n := newTestNode(t, Config{Rules: []Rule{
		{Expression: `/^err/.test(payload.code)`},
		{Expression: `tags.line === "3"`},
		{Expression: `header.quality !== "good"`},
	}, Mode: "allMatches"})

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"code": "err_timeout"}})
	in.Header.Tags = map[string]string{"line": "3"}
	in.Header.Quality = datagram.QualityBad

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected all 3 rules to match, got %+v", results)
	}
}

func TestPROC300_TypeCheckPredicate(t *testing.T) {
	n := newTestNode(t, Config{Rules: []Rule{{Expression: `typeof payload.value === "number"`}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": "not a number"}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Port != "default" {
		t.Fatalf("expected the string value to fail the typeof number check, got %+v", results)
	}
}
