package sqlsource

import (
	"encoding/json"
	"testing"
)

func TestCON500_NewRequiresQuery(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"once"}`)); err == nil {
		t.Fatal("expected an error when query is missing")
	}
}

func TestCON500_NewRequiresIntervalForPeriodic(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"periodic","query":"SELECT 1"}`)); err == nil {
		t.Fatal("expected an error when intervalMs is missing in periodic mode")
	}
	raw, err := json.Marshal(Config{Mode: "periodic", Query: "SELECT 1", IntervalMs: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(raw); err != nil {
		t.Errorf("New with valid periodic config: %v", err)
	}
}

func TestCON500_NewRejectsUnknownMode(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"bogus","query":"SELECT 1"}`)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
}
