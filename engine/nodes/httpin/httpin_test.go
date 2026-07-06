package httpin

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/webhook"
)

func TestCON300_NewRequiresPathAndMethod(t *testing.T) {
	if _, err := New(json.RawMessage(`{"method":"POST"}`)); err == nil {
		t.Fatal("expected an error when path is missing")
	}
	if _, err := New(json.RawMessage(`{"path":"/x"}`)); err == nil {
		t.Fatal("expected an error when method is missing")
	}
}

// startNode registers n's route on webhook.DefaultRegistry until ctx is
// cancelled, capturing every emitted datagram.
func startNode(t *testing.T, n flow.Source) (stop func(), emitted func() []datagram.Datagram) {
	t.Helper()
	var mu sync.Mutex
	var got []datagram.Datagram
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = n.Run(ctx, func(port string, d datagram.Datagram) error {
			mu.Lock()
			got = append(got, d)
			mu.Unlock()
			return nil
		})
	}()
	time.Sleep(20 * time.Millisecond) // let Run register its route
	return func() { cancel(); <-done }, func() []datagram.Datagram {
		mu.Lock()
		defer mu.Unlock()
		return append([]datagram.Datagram(nil), got...)
	}
}

func TestCON300_RequestBecomesADatagramWithNoAuth(t *testing.T) {
	n, err := New(json.RawMessage(`{"path":"/hooks/x","method":"POST","responseTimeoutMs":50}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, emitted := startNode(t, n.(flow.Source))
	defer stop()

	req := httptest.NewRequest(http.MethodPost, "/hooks/x?a=1", bytes.NewBufferString(`{"v":1}`))
	req.Header.Set("X-Custom", "hello")
	rec := httptest.NewRecorder()
	webhook.DefaultRegistry.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (default reply, no http-response node)", rec.Code)
	}
	got := emitted()
	if len(got) != 1 {
		t.Fatalf("expected 1 emitted datagram, got %d", len(got))
	}
	value, ok := got[0].Payload.Value.(map[string]any)
	if !ok {
		t.Fatalf("payload = %T, want map[string]any", got[0].Payload.Value)
	}
	if value["body"] != `{"v":1}` {
		t.Errorf("body = %v", value["body"])
	}
	query, _ := value["query"].(map[string]string)
	if query["a"] != "1" {
		t.Errorf("query = %v, want a=1", query)
	}
	headers, _ := value["headers"].(map[string]string)
	if headers["X-Custom"] != "hello" {
		t.Errorf("headers = %v, want X-Custom=hello", headers)
	}
}

func TestCON300_PairedResponseNodeControlsTheReply(t *testing.T) {
	n, err := New(json.RawMessage(`{"path":"/hooks/y","method":"POST"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, emitted := startNode(t, n.(flow.Source))
	defer stop()

	// Simulate a downstream "http-response" node replying, racing the
	// actual HTTP handler's wait on webhook.Default.
	go func() {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if got := emitted(); len(got) == 1 {
				webhook.Default.Reply(got[0].Header.ID, webhook.Response{Status: 201, Body: []byte("created")})
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	req := httptest.NewRequest(http.MethodPost, "/hooks/y", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()
	webhook.DefaultRegistry.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Errorf("status = %d, want 201 (from the paired response node)", rec.Code)
	}
	if rec.Body.String() != "created" {
		t.Errorf("body = %q, want created", rec.Body.String())
	}
}

func TestCON300_NoPairedResponseFallsBackToDefaultAfterTimeout(t *testing.T) {
	n, err := New(json.RawMessage(`{"path":"/hooks/z","method":"POST","responseTimeoutMs":50}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, _ := startNode(t, n.(flow.Source))
	defer stop()

	req := httptest.NewRequest(http.MethodPost, "/hooks/z", bytes.NewBufferString(`{}`))
	rec := httptest.NewRecorder()

	start := time.Now()
	webhook.DefaultRegistry.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (default fallback)", rec.Code)
	}
	if elapsed < 50*time.Millisecond {
		t.Errorf("returned after %v, expected to wait out the configured 50ms timeout", elapsed)
	}
}

func TestCON300_BasicAuthRejectsWrongCredentials(t *testing.T) {
	n, err := New(json.RawMessage(`{"path":"/hooks/auth","method":"POST","auth":{"type":"basic"},"responseTimeoutMs":50}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	resolver := stubResolver{credentialJSON: json.RawMessage(`{"username":"u","password":"p"}`)}
	ctx = flow.WithConnection(ctx, resolver, "conn-1")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, func(string, datagram.Datagram) error { return nil })
	}()
	time.Sleep(20 * time.Millisecond)
	defer func() { cancel(); <-done }()

	req := httptest.NewRequest(http.MethodPost, "/hooks/auth", bytes.NewBufferString(`{}`))
	req.SetBasicAuth("u", "wrong")
	rec := httptest.NewRecorder()
	webhook.DefaultRegistry.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 for wrong password", rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodPost, "/hooks/auth", bytes.NewBufferString(`{}`))
	req2.SetBasicAuth("u", "p")
	rec2 := httptest.NewRecorder()
	webhook.DefaultRegistry.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 for correct credentials", rec2.Code)
	}
}

// stubResolver implements flow.ConnectionResolver for tests in this
// package that need auth credentials resolved.
type stubResolver struct {
	credentialJSON json.RawMessage
}

func (s stubResolver) ResolveConnection(context.Context, string) (flow.ConnectionInfo, error) {
	return flow.ConnectionInfo{CredentialJSON: s.credentialJSON}, nil
}
