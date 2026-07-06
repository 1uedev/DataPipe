package mqttin

import (
	"encoding/json"
	"testing"
)

func TestCON200_NewRequiresTopic(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error when topic is missing")
	}
}

func TestCON200_NewValidatesQoSRange(t *testing.T) {
	if _, err := New(json.RawMessage(`{"topic":"x","qos":3}`)); err == nil {
		t.Fatal("expected an error for qos out of range")
	}
	if _, err := New(json.RawMessage(`{"topic":"x","qos":-1}`)); err == nil {
		t.Fatal("expected an error for a negative qos")
	}
	for _, qos := range []int{0, 1, 2} {
		raw, err := json.Marshal(Config{Topic: "x", QoS: qos})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := New(raw); err != nil {
			t.Errorf("New with qos=%d: %v", qos, err)
		}
	}
}

func TestCON200_DecodePayloadParsesJSONWhenPossible(t *testing.T) {
	v := decodePayload([]byte(`{"temp":21.5}`))
	m, ok := v.(map[string]any)
	if !ok || m["temp"] != 21.5 {
		t.Errorf("decodePayload(JSON) = %#v", v)
	}
}

func TestCON200_DecodePayloadFallsBackToRawStringForNonJSON(t *testing.T) {
	v := decodePayload([]byte("not json"))
	if v != "not json" {
		t.Errorf("decodePayload(non-JSON) = %#v, want the raw string", v)
	}
}
