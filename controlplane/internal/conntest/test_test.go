package conntest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestCON140_UnknownConnectionTypeReportsNoLiveTestAvailable(t *testing.T) {
	result := Test(context.Background(), "http", json.RawMessage(`{}`), nil)
	if !result.OK {
		t.Fatalf("expected OK=true for a type with no live test, got %+v", result)
	}
}

func TestCON140_PostgresMissingHostFailsFast(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"database": "datapipe"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "postgres", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: host is required")
	}
	if !strings.Contains(result.Message, "host") {
		t.Errorf("message = %q, want it to mention the missing host", result.Message)
	}
}

func TestCON140_PostgresInvalidConfigJSONFailsFast(t *testing.T) {
	result := Test(context.Background(), "postgres", json.RawMessage(`not json`), nil)
	if result.OK {
		t.Fatal("expected failure: invalid config JSON")
	}
}

func TestCON140_PostgresUnreachableHostFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"host": "127.0.0.1", "port": 1, "database": "datapipe"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	result := Test(context.Background(), "postgres", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
	if elapsed := time.Since(start); elapsed > Timeout+2*time.Second {
		t.Errorf("Test took %v, expected to fail well within the %v timeout", elapsed, Timeout)
	}
}

func TestCON140_MQTTMissingBrokerURLFailsFast(t *testing.T) {
	result := Test(context.Background(), "mqtt", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: brokerUrl is required")
	}
	if !strings.Contains(result.Message, "brokerUrl") {
		t.Errorf("message = %q, want it to mention the missing brokerUrl", result.Message)
	}
}

func TestCON140_MQTTInvalidConfigJSONFailsFast(t *testing.T) {
	result := Test(context.Background(), "mqtt", json.RawMessage(`not json`), nil)
	if result.OK {
		t.Fatal("expected failure: invalid config JSON")
	}
}

func TestCON140_MQTTInvalidCredentialJSONFailsFast(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"brokerUrl": "tcp://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "mqtt", cfg, json.RawMessage(`not json`))
	if result.OK {
		t.Fatal("expected failure: invalid credential JSON")
	}
}
