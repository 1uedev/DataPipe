package mqttout

import (
	"encoding/json"
	"testing"
)

func TestSNK110_NewRequiresTopic(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error when topic is missing")
	}
}

func TestSNK110_NewValidatesQoSRange(t *testing.T) {
	if _, err := New(json.RawMessage(`{"topic":"x","qos":3}`)); err == nil {
		t.Fatal("expected an error for qos out of range")
	}
}

func TestSNK110_RenderTemplateSubstitutesDotPath(t *testing.T) {
	payload := map[string]any{"site": map[string]any{"id": "A1"}}
	got := renderTemplate("sensors/{{site.id}}/temp", payload)
	if got != "sensors/A1/temp" {
		t.Errorf("renderTemplate = %q", got)
	}
}

func TestSNK110_RenderTemplateMissingPathBecomesEmpty(t *testing.T) {
	got := renderTemplate("sensors/{{missing.field}}/temp", map[string]any{})
	if got != "sensors//temp" {
		t.Errorf("renderTemplate = %q, want an empty substitution", got)
	}
}

func TestSNK110_EncodePayloadStringVerbatim(t *testing.T) {
	b, err := encodePayload("hello")
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "hello" {
		t.Errorf("encodePayload(string) = %q", b)
	}
}

func TestSNK110_EncodePayloadObjectAsJSON(t *testing.T) {
	b, err := encodePayload(map[string]any{"a": 1.0})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatalf("encodePayload output did not decode as JSON: %v (%s)", err, b)
	}
	if decoded["a"] != 1.0 {
		t.Errorf("decoded = %+v", decoded)
	}
}
