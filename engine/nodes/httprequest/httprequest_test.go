package httprequest

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func testDgm(value any) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: value})
}

func TestCON315_NewRequiresURLAndMethod(t *testing.T) {
	if _, err := New(json.RawMessage(`{"method":"GET"}`)); err == nil {
		t.Fatal("expected an error when url is missing")
	}
	if _, err := New(json.RawMessage(`{"url":"http://x"}`)); err == nil {
		t.Fatal("expected an error when method is missing")
	}
}

func TestCON315_GETSendsNoBodyAndParsesJSONResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		if len(body) != 0 {
			t.Errorf("expected no request body for GET, got %q", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","value":42}`))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "GET", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	results, err := proc.Process(context.Background(), testDgm(nil))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("results = %+v", results)
	}
	value, ok := results[0].Datagram.Payload.Value.(map[string]any)
	if !ok {
		t.Fatalf("value = %T, want map[string]any", results[0].Datagram.Payload.Value)
	}
	if value["status"] != "ok" {
		t.Errorf("status = %v, want ok", value["status"])
	}
}

func TestCON315_POSTSendsPayloadAsJSONBody(t *testing.T) {
	var receivedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedBody, _ = io.ReadAll(r.Body)
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", ct)
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":1}`))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "POST", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	if _, err := proc.Process(context.Background(), testDgm(map[string]any{"name": "widget"})); err != nil {
		t.Fatalf("Process: %v", err)
	}
	var sent map[string]any
	if err := json.Unmarshal(receivedBody, &sent); err != nil {
		t.Fatalf("request body did not decode as JSON: %v (%s)", err, receivedBody)
	}
	if sent["name"] != "widget" {
		t.Errorf("sent = %+v", sent)
	}
}

func TestCON315_URLAndHeaderTemplatingFromPayload(t *testing.T) {
	var gotPath, gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotHeader = r.Header.Get("X-Order-Id")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{
		Method:  "GET",
		URL:     srv.URL + "/orders/{{order.id}}",
		Headers: map[string]string{"X-Order-Id": "{{order.id}}"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	if _, err := proc.Process(context.Background(), testDgm(map[string]any{"order": map[string]any{"id": "abc123"}})); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if gotPath != "/orders/abc123" {
		t.Errorf("path = %q, want /orders/abc123", gotPath)
	}
	if gotHeader != "abc123" {
		t.Errorf("X-Order-Id header = %q, want abc123", gotHeader)
	}
}

func TestCON315_NonJSONResponseFallsBackToRawString(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("plain text"))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "GET", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	results, err := proc.Process(context.Background(), testDgm(nil))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if results[0].Datagram.Payload.Value != "plain text" {
		t.Errorf("value = %v, want the raw string", results[0].Datagram.Payload.Value)
	}
}

func TestCON315_ResponseFieldSelectsSubPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"id":7}}`))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "GET", URL: srv.URL, ResponseField: "data.id"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	results, err := proc.Process(context.Background(), testDgm(nil))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if v, _ := results[0].Datagram.Payload.Value.(float64); v != 7 {
		t.Errorf("value = %v, want 7", results[0].Datagram.Payload.Value)
	}
}

func TestCON315_ErrorStatusReturnsAnErrorForERR100ToHandle(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "GET", URL: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	if _, err := proc.Process(context.Background(), testDgm(nil)); err == nil {
		t.Fatal("expected an error for a 500 response, so ERR-100's policy can retry/route it")
	}
}

func TestSNK160_BasicAuthCredentialFromConnection(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "GET", URL: srv.URL, Auth: AuthConfig{Type: "basic"}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	resolver := stubResolver{credentialJSON: json.RawMessage(`{"username":"u","password":"p"}`)}
	ctx := flow.WithConnection(context.Background(), resolver, "conn-1")
	if _, err := proc.Process(ctx, testDgm(nil)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if gotUser != "u" || gotPass != "p" {
		t.Errorf("basic auth = %q/%q, want u/p", gotUser, gotPass)
	}
}

func TestSNK160_BearerAuthCredentialFromConnection(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	raw, err := json.Marshal(Config{Method: "GET", URL: srv.URL, Auth: AuthConfig{Type: "bearer"}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	resolver := stubResolver{credentialJSON: json.RawMessage(`{"token":"tok-123"}`)}
	ctx := flow.WithConnection(context.Background(), resolver, "conn-1")
	if _, err := proc.Process(ctx, testDgm(nil)); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if gotAuth != "Bearer tok-123" {
		t.Errorf("Authorization = %q, want Bearer tok-123", gotAuth)
	}
}

type stubResolver struct {
	credentialJSON json.RawMessage
}

func (s stubResolver) ResolveConnection(context.Context, string) (flow.ConnectionInfo, error) {
	return flow.ConnectionInfo{CredentialJSON: s.credentialJSON}, nil
}
