package api

import (
	"net/http"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func TestOBS140_CreateAlertRuleRequiresSystemAdmin(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/alert-rules", token, map[string]string{
		"name": "rt-1 down", "metric": "connectionDown", "targetRuntimeId": "rt-1",
	})
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a non-System-Admin", resp.status)
	}
}

func TestOBS140_CreateListAndDeleteAlertRule(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := e.request(http.MethodPost, "/alert-rules", token, map[string]string{
		"name": "rt-1 down", "metric": "connectionDown", "targetRuntimeId": "rt-1",
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.status, resp.body)
	}
	var rule AlertRule
	resp.decode(t, &rule)
	if rule.Name != "rt-1 down" || !rule.Enabled {
		t.Errorf("rule = %+v", rule)
	}

	resp = e.request(http.MethodGet, "/alert-rules", token, nil)
	var rules []AlertRule
	resp.decode(t, &rules)
	if len(rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(rules))
	}

	resp = e.request(http.MethodDelete, "/alert-rules/"+rule.ID, token, nil)
	if resp.status != http.StatusNoContent {
		t.Fatalf("delete status = %d", resp.status)
	}
	resp = e.request(http.MethodGet, "/alert-rules", token, nil)
	resp.decode(t, &rules)
	if len(rules) != 0 {
		t.Fatalf("expected 0 rules after delete, got %d", len(rules))
	}
}

func TestOBS140_CreateAlertRuleValidatesMetric(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := e.request(http.MethodPost, "/alert-rules", token, map[string]string{"name": "bad", "metric": "cpuHigh"})
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown metric", resp.status)
	}
}

func TestOBS140_ListAlertsRequiresAuth(t *testing.T) {
	e := newTestEnv(t)
	resp := e.request(http.MethodGet, "/alerts", "", nil)
	if resp.status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.status)
	}
}
