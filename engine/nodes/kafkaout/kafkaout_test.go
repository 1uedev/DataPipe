package kafkaout

import (
	"encoding/json"
	"testing"
)

func TestSNK_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{}`)); err == nil {
		t.Error("expected error for missing topic")
	}
	raw, err := json.Marshal(Config{Topic: "readings"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(raw); err != nil {
		t.Errorf("New(valid config) error = %v", err)
	}
}
