package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// TestOBS150_ExportRequiresSystemAdmin covers the RBAC gate: a plain
// project member must not be able to download the full configuration
// bundle (which includes every credential's sealed ciphertext).
func TestOBS150_ExportRequiresSystemAdmin(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodGet, "/backup", token, nil)
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a non-System-Admin", resp.status)
	}
}

// TestOBS150_ExportRoundTripsProjectFlowAndAlertRule creates a project, a
// deployed flow, and an alert rule, exports a backup, wipes the database
// by restoring an EMPTY bundle, then restores the real export — the final
// state must match what was there before the wipe.
func TestOBS150_ExportRoundTripsProjectFlowAndAlertRule(t *testing.T) {
	e := newTestEnv(t)
	adminToken := e.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := e.request(http.MethodPost, "/projects", adminToken, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", adminToken, map[string]any{
		"name": "demo flow", "content": json.RawMessage(sampleFlowContent()),
	})
	var f Flow
	resp.decode(t, &f)
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", adminToken, nil)
	if resp.status != http.StatusCreated {
		t.Fatalf("deploy status = %d, body = %s", resp.status, resp.body)
	}

	resp = e.request(http.MethodPost, "/alert-rules", adminToken, map[string]string{
		"name": "rt-1 down", "metric": "connectionDown", "targetRuntimeId": "rt-1",
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create alert rule status = %d, body = %s", resp.status, resp.body)
	}

	resp = e.request(http.MethodGet, "/backup", adminToken, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("export status = %d, body = %s", resp.status, resp.body)
	}
	var bundle Bundle
	resp.decode(t, &bundle)
	if len(bundle.Projects) != 1 || len(bundle.Flows) != 1 || len(bundle.FlowVersions) != 1 || len(bundle.AlertRules) != 1 {
		t.Fatalf("bundle = %+v, want exactly 1 of each", bundle)
	}
	if len(bundle.Users) != 1 || bundle.Users[0].Username != "admin" {
		t.Fatalf("bundle.Users = %+v, want exactly the admin user", bundle.Users)
	}

	resp = e.request(http.MethodPost, "/backup/restore", adminToken, map[string]any{"confirm": true, "bundle": bundle})
	if resp.status != http.StatusOK {
		t.Fatalf("restore status = %d, body = %s", resp.status, resp.body)
	}

	// The admin's OWN session was deleted by the restore (it wipes and
	// reloads the users/sessions tables) — the token used to call restore
	// must now be rejected, and a fresh login as the restored admin user
	// (whose password hash round-tripped through the bundle) must work.
	resp = e.request(http.MethodGet, "/projects", adminToken, nil)
	if resp.status != http.StatusUnauthorized {
		t.Fatalf("status after restore with pre-restore token = %d, want 401 (session cleared by restore)", resp.status)
	}

	resp = e.request(http.MethodPost, "/auth/login", "", map[string]string{"username": "admin", "password": "correct-horse-battery-staple"})
	if resp.status != http.StatusOK {
		t.Fatalf("login after restore status = %d, body = %s", resp.status, resp.body)
	}
	var login struct {
		Token string `json:"token"`
	}
	resp.decode(t, &login)
	newToken := login.Token

	resp = e.request(http.MethodGet, "/projects/"+project.ID+"/flows", newToken, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list flows after restore status = %d, body = %s", resp.status, resp.body)
	}
	var flows []Flow
	resp.decode(t, &flows)
	if len(flows) != 1 || flows[0].ID != f.ID {
		t.Fatalf("flows after restore = %+v, want the original flow %s", flows, f.ID)
	}

	resp = e.request(http.MethodGet, "/alert-rules", newToken, nil)
	var rules []AlertRule
	resp.decode(t, &rules)
	if len(rules) != 1 || rules[0].Name != "rt-1 down" {
		t.Fatalf("alert rules after restore = %+v, want the original rule", rules)
	}
}

func TestOBS150_RestoreRequiresConfirm(t *testing.T) {
	e := newTestEnv(t)
	adminToken := e.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := e.request(http.MethodPost, "/backup/restore", adminToken, map[string]any{"bundle": Bundle{}})
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 without confirm: true", resp.status)
	}
}

func TestOBS150_RestoreRequiresSystemAdmin(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/backup/restore", token, map[string]any{"confirm": true, "bundle": Bundle{}})
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a non-System-Admin", resp.status)
	}
}
