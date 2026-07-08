package alerting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type fakeStore struct {
	mu     sync.Mutex
	rules  []Rule
	alerts map[string]string // ruleID -> open alert id, if any
	fired  []string
}

func (f *fakeStore) ListEnabledRules(context.Context) ([]Rule, error) { return f.rules, nil }

func (f *fakeStore) OpenAlert(_ context.Context, ruleID string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id, ok := f.alerts[ruleID]
	return id, ok, nil
}

func (f *fakeStore) CreateAlert(_ context.Context, ruleID, message string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alerts[ruleID] = ruleID + "-alert"
	f.fired = append(f.fired, "fire:"+ruleID+":"+message)
	return nil
}

func (f *fakeStore) ResolveAlert(_ context.Context, alertID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for rid, aid := range f.alerts {
		if aid == alertID {
			delete(f.alerts, rid)
		}
	}
	f.fired = append(f.fired, "resolve:"+alertID)
	return nil
}

type fakeRuntimes struct{ statuses []RuntimeStatus }

func (f *fakeRuntimes) ListRuntimeStatuses(context.Context) ([]RuntimeStatus, error) {
	return f.statuses, nil
}

func TestOBS140_ConnectionDownFiresWhenTargetOffline(t *testing.T) {
	store := &fakeStore{
		rules:  []Rule{{ID: "r1", Name: "rt-1 down", Metric: MetricConnectionDown, TargetRuntimeID: "rt-1"}},
		alerts: map[string]string{},
	}
	runtimes := &fakeRuntimes{statuses: []RuntimeStatus{{RuntimeID: "rt-1", Online: false}}}
	e := NewEvaluator(store, runtimes)

	e.evaluateOnce(context.Background())

	if len(store.fired) != 1 || store.fired[0] != "fire:r1:runtime rt-1 is offline" {
		t.Fatalf("fired = %+v", store.fired)
	}
}

func TestOBS140_ConnectionDownResolvesWhenTargetComesBackOnline(t *testing.T) {
	store := &fakeStore{
		rules:  []Rule{{ID: "r1", Name: "rt-1 down", Metric: MetricConnectionDown, TargetRuntimeID: "rt-1"}},
		alerts: map[string]string{"r1": "r1-alert"},
	}
	runtimes := &fakeRuntimes{statuses: []RuntimeStatus{{RuntimeID: "rt-1", Online: true}}}
	e := NewEvaluator(store, runtimes)

	e.evaluateOnce(context.Background())

	if len(store.fired) != 1 || store.fired[0] != "resolve:r1-alert" {
		t.Fatalf("fired = %+v", store.fired)
	}
}

func TestOBS140_NoTransitionFiresNothing(t *testing.T) {
	store := &fakeStore{
		rules:  []Rule{{ID: "r1", Name: "rt-1 down", Metric: MetricConnectionDown, TargetRuntimeID: "rt-1"}},
		alerts: map[string]string{},
	}
	runtimes := &fakeRuntimes{statuses: []RuntimeStatus{{RuntimeID: "rt-1", Online: true}}}
	e := NewEvaluator(store, runtimes)

	e.evaluateOnce(context.Background())

	if len(store.fired) != 0 {
		t.Fatalf("expected no alert activity, got %+v", store.fired)
	}
}

func TestOBS140_EdgeOfflineFiresForAnyOfflineRuntime(t *testing.T) {
	store := &fakeStore{
		rules:  []Rule{{ID: "r1", Name: "fleet health", Metric: MetricEdgeOffline}},
		alerts: map[string]string{},
	}
	runtimes := &fakeRuntimes{statuses: []RuntimeStatus{{RuntimeID: "edge-1", Online: false}, {RuntimeID: "edge-2", Online: true}}}
	e := NewEvaluator(store, runtimes)

	e.evaluateOnce(context.Background())

	if len(store.fired) != 1 {
		t.Fatalf("fired = %+v", store.fired)
	}
}

func TestOBS140_FiresWebhookOnTransition(t *testing.T) {
	received := make(chan map[string]string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]string
		_ = json.NewDecoder(r.Body).Decode(&body)
		received <- body
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	store := &fakeStore{
		rules:  []Rule{{ID: "r1", Name: "rt-1 down", Metric: MetricConnectionDown, TargetRuntimeID: "rt-1", WebhookURL: srv.URL}},
		alerts: map[string]string{},
	}
	runtimes := &fakeRuntimes{statuses: []RuntimeStatus{{RuntimeID: "rt-1", Online: false}}}
	e := NewEvaluator(store, runtimes)

	e.evaluateOnce(context.Background())

	select {
	case body := <-received:
		if body["state"] != "firing" || body["rule"] != "rt-1 down" {
			t.Errorf("webhook body = %+v", body)
		}
	default:
		t.Fatal("expected a webhook delivery")
	}
}
