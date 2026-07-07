package lookup

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

func TestPROC400_NewValidatesConfig(t *testing.T) {
	cases := []Config{
		{Source: "static", As: "x"},                              // missing keyExpression
		{KeyExpression: "payload.id", Source: "static"},          // missing as
		{KeyExpression: "payload.id", Source: "static", As: "x"}, // missing static table
		{KeyExpression: "payload.id", Source: "sql", As: "x"},    // missing sql.query
		{KeyExpression: "payload.id", Source: "http", As: "x"},   // missing http.urlTemplate
		{KeyExpression: "payload.id", Source: "bogus", As: "x"},  // unknown source
	}
	for i, cfg := range cases {
		raw, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := New(raw); err == nil {
			t.Errorf("case %d: expected an error for %+v", i, cfg)
		}
	}
}

func TestPROC400_StaticLookupEnrichesPayload(t *testing.T) {
	n := newTestNode(t, Config{
		KeyExpression: "payload.sensorId", Source: "static", As: "meta",
		Static: map[string]any{"s1": map[string]any{"name": "Boiler 1"}},
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"sensorId": "s1"}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	meta := m["meta"].(map[string]any)
	if meta["name"] != "Boiler 1" {
		t.Errorf("meta = %+v", meta)
	}
	if m["sensorId"] != "s1" {
		t.Errorf("original field should survive, got %+v", m)
	}
}

func TestPROC400_CacheMissPolicyFail(t *testing.T) {
	n := newTestNode(t, Config{KeyExpression: "payload.id", Source: "static", As: "x", Static: map[string]any{"unrelated": 1.0}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "missing"}})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error for cacheMissPolicy \"fail\"")
	}
}

func TestPROC400_CacheMissPolicyPassthrough(t *testing.T) {
	n := newTestNode(t, Config{KeyExpression: "payload.id", Source: "static", As: "x", Static: map[string]any{"unrelated": 1.0}, CacheMissPolicy: "passthrough"})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "missing"}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value.(map[string]any)["id"] != "missing" {
		t.Errorf("expected the original payload unchanged, got %+v", results[0].Datagram.Payload.Value)
	}
}

func TestPROC400_CacheMissPolicyDefault(t *testing.T) {
	n := newTestNode(t, Config{
		KeyExpression: "payload.id", Source: "static", As: "meta", Static: map[string]any{"unrelated": 1.0},
		CacheMissPolicy: "default", DefaultValue: "unknown",
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "missing"}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value.(map[string]any)["meta"] != "unknown" {
		t.Errorf("meta = %v", results[0].Datagram.Payload.Value.(map[string]any)["meta"])
	}
}

func TestPROC400_HTTPLookupAndCacheAvoidsSecondRequest(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{"name":"from-http"}`))
	}))
	defer server.Close()

	n := newTestNode(t, Config{
		KeyExpression: "payload.id", Source: "http", As: "meta",
		HTTP: HTTPConfig{URLTemplate: server.URL + "/sensors/{key}"},
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "s1"}})

	for i := 0; i < 2; i++ {
		results, err := n.Process(context.Background(), in)
		if err != nil {
			t.Fatalf("Process %d: %v", i, err)
		}
		meta := results[0].Datagram.Payload.Value.(map[string]any)["meta"].(map[string]any)
		if meta["name"] != "from-http" {
			t.Errorf("meta = %+v", meta)
		}
	}
	if requests != 1 {
		t.Errorf("expected exactly 1 HTTP request (second lookup served from cache), got %d", requests)
	}
}

func TestPROC400_HTTPLookup404IsACacheMiss(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	n := newTestNode(t, Config{
		KeyExpression: "payload.id", Source: "http", As: "meta",
		HTTP: HTTPConfig{URLTemplate: server.URL + "/sensors/{key}"},
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "missing"}})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected a cache-miss error (default policy \"fail\") for a 404")
	}
}

func TestPROC400_CacheTTLExpiresEntries(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{"n":1}`))
	}))
	defer server.Close()

	n := newTestNode(t, Config{
		KeyExpression: "payload.id", Source: "http", As: "meta",
		HTTP:  HTTPConfig{URLTemplate: server.URL + "/{key}"},
		Cache: CacheConfig{TTLMs: 30},
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": "s1"}})

	if _, err := n.Process(context.Background(), in); err != nil {
		t.Fatalf("Process 1: %v", err)
	}
	time.Sleep(60 * time.Millisecond)
	if _, err := n.Process(context.Background(), in); err != nil {
		t.Fatalf("Process 2: %v", err)
	}
	if requests != 2 {
		t.Errorf("expected the expired cache entry to trigger a second request, got %d requests", requests)
	}
}

func TestPROC400_CacheMaxEntriesEvictsOldest(t *testing.T) {
	n := newTestNode(t, Config{
		KeyExpression: "payload.id", Source: "static", As: "x",
		Static: map[string]any{"a": 1.0, "b": 2.0, "c": 3.0},
		Cache:  CacheConfig{MaxEntries: 2},
	})
	for _, id := range []string{"a", "b", "c"} {
		in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"id": id}})
		if _, err := n.Process(context.Background(), in); err != nil {
			t.Fatalf("Process(%s): %v", id, err)
		}
	}
	n.mu.Lock()
	cacheSize := len(n.cache)
	_, hasA := n.cache["a"]
	n.mu.Unlock()
	if cacheSize != 2 {
		t.Errorf("cache size = %d, want 2 (maxEntries)", cacheSize)
	}
	if hasA {
		t.Error("expected the oldest entry (\"a\") to have been evicted")
	}
}
