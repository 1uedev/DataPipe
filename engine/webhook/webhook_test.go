package webhook

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCON300_RegistryDispatchesByMethodAndPath(t *testing.T) {
	r := NewRegistry()
	cancel := r.Register("POST", "/hooks/a", func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	defer cancel()

	req := httptest.NewRequest(http.MethodPost, "/hooks/a", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusTeapot {
		t.Errorf("status = %d, want %d", rec.Code, http.StatusTeapot)
	}
}

func TestCON300_RegistryReturns404ForUnregisteredRoute(t *testing.T) {
	r := NewRegistry()
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404", rec.Code)
	}
}

func TestCON300_RegistryIsMethodSpecific(t *testing.T) {
	r := NewRegistry()
	cancel := r.Register("POST", "/hooks/a", func(w http.ResponseWriter, req *http.Request) { w.WriteHeader(http.StatusOK) })
	defer cancel()

	req := httptest.NewRequest(http.MethodGet, "/hooks/a", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("GET on a POST-only route: status = %d, want 404", rec.Code)
	}
}

func TestCON300_RegistryCancelRemovesRoute(t *testing.T) {
	r := NewRegistry()
	cancel := r.Register("GET", "/x", func(w http.ResponseWriter, req *http.Request) { w.WriteHeader(http.StatusOK) })
	cancel()

	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("status after cancel = %d, want 404", rec.Code)
	}
}

func TestSNK170_PendingResponsesDeliversToAwaiter(t *testing.T) {
	p := NewPendingResponses()
	ch, cancel := p.Await("corr-1")
	defer cancel()

	if !p.Reply("corr-1", Response{Status: 201, Body: []byte("ok")}) {
		t.Fatal("expected Reply to find the awaiter")
	}
	select {
	case resp := <-ch:
		if resp.Status != 201 || string(resp.Body) != "ok" {
			t.Errorf("resp = %+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("expected the response to be delivered")
	}
}

func TestSNK170_ReplyToUnknownCorrelationIDReturnsFalse(t *testing.T) {
	p := NewPendingResponses()
	if p.Reply("no-such-id", Response{}) {
		t.Fatal("expected Reply to report false for an unknown correlation id")
	}
}

func TestSNK170_CancelStopsFurtherDelivery(t *testing.T) {
	p := NewPendingResponses()
	_, cancel := p.Await("corr-2")
	cancel()
	if p.Reply("corr-2", Response{}) {
		t.Fatal("expected Reply to report false after cancel")
	}
}
