package s3sink

import (
	"encoding/json"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestSNK_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{}`)); err == nil {
		t.Error("expected error for missing key")
	}
	if _, err := New([]byte(`{"key":"k","format":"bogus"}`)); err == nil {
		t.Error("expected error for unknown format")
	}
	raw, err := json.Marshal(Config{Key: "readings/{{ sensorId }}.json"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(raw); err != nil {
		t.Errorf("New(valid config) error = %v", err)
	}
}

func TestSNK_ResolveKeySubstitutesPayloadFields(t *testing.T) {
	n := &node{cfg: Config{Key: "readings/{{ sensorId }}.json"}}
	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"sensorId": "room1"}})
	if got := n.resolveKey(d); got != "readings/room1.json" {
		t.Errorf("resolveKey = %q, want %q", got, "readings/room1.json")
	}
}

func TestSNK_EncodeBodyJSONAndRaw(t *testing.T) {
	n := &node{cfg: Config{Format: "json"}}
	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"a": 1}})
	body, err := n.encodeBody(d)
	if err != nil || string(body) != `{"a":1}` {
		t.Errorf("encodeBody(json) = %q, %v", body, err)
	}

	n = &node{cfg: Config{Format: "raw"}}
	d = datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "hello"})
	body, err = n.encodeBody(d)
	if err != nil || string(body) != "hello" {
		t.Errorf("encodeBody(raw) = %q, %v", body, err)
	}

	d = datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 42})
	if _, err := n.encodeBody(d); err == nil {
		t.Error("expected an error for non-string payload with format \"raw\"")
	}
}
