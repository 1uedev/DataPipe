package template

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

func TestPROC130_NewRequiresTemplate(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error: template is required")
	}
}

func TestPROC130_RendersLiteralTextAndPlaceholders(t *testing.T) {
	n := newTestNode(t, Config{Template: "sensor {{tags.line}}: {{payload.value}}C"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 21.5}})
	in.Header.Tags = map[string]string{"line": "3"}

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value != "sensor 3: 21.5C" {
		t.Errorf("payload = %v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC130_LogicCapableViaTernaryExpression(t *testing.T) {
	n := newTestNode(t, Config{Template: "status: {{payload.value > 20 ? 'HIGH' : 'OK'}}"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 25.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value != "status: HIGH" {
		t.Errorf("payload = %v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC130_ParseJSONOptionParsesRenderedText(t *testing.T) {
	n := newTestNode(t, Config{Template: `{"line": "{{tags.line}}", "value": {{payload.value}}}`, ParseJSON: true})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"value": 21.5}})
	in.Header.Tags = map[string]string{"line": "3"}

	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m, ok := results[0].Datagram.Payload.Value.(map[string]any)
	if !ok || m["line"] != "3" || m["value"] != 21.5 {
		t.Errorf("payload = %+v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC130_ParseJSONOptionErrorsOnInvalidJSON(t *testing.T) {
	n := newTestNode(t, Config{Template: "not json", ParseJSON: true})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error: rendered text is not valid JSON")
	}
}

func TestPROC130_TemplateWithNoPlaceholdersPassesThroughLiteral(t *testing.T) {
	n := newTestNode(t, Config{Template: "static text"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value != "static text" {
		t.Errorf("payload = %v", results[0].Datagram.Payload.Value)
	}
}
