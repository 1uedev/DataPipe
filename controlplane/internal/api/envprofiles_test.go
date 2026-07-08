package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// flowContentWithEnv declares two env vars: MQTT_HOST (has a default) and
// REQUIRED_VAR (no default, so it must come from an active profile or
// deploy is rejected — VCS-140's "missing-variable check at deploy").
func flowContentWithEnv() json.RawMessage {
	return json.RawMessage(`{
		"formatVersion": 1,
		"kind": "flow",
		"id": "flow_env_test",
		"name": "env test",
		"mode": "streaming",
		"env": [
			{"name": "MQTT_HOST", "type": "string", "default": "localhost"},
			{"name": "REQUIRED_VAR", "type": "string"}
		],
		"graph": {
			"nodes": [
				{"id": "n1", "type": "inject", "typeVersion": 1, "config": {"payload": {"value": 1}, "repeatMs": 0}},
				{"id": "n2", "type": "debug-log", "typeVersion": 1, "config": {}}
			],
			"wires": [
				{"id": "w1", "from": {"node": "n1", "port": "out"}, "to": {"node": "n2", "port": "in"}}
			]
		}
	}`)
}

func TestVCS140_CreateListUpdateDeleteProfile(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/profiles", token, map[string]any{
		"name": "dev", "values": map[string]string{"MQTT_HOST": "dev-broker"},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create status = %d, body = %s", resp.status, resp.body)
	}
	var profile EnvironmentProfile
	resp.decode(t, &profile)
	if profile.Name != "dev" || profile.Values["MQTT_HOST"] != "dev-broker" {
		t.Fatalf("profile = %+v", profile)
	}

	resp = e.request(http.MethodGet, "/projects/"+project.ID+"/profiles", token, nil)
	var profiles []EnvironmentProfile
	resp.decode(t, &profiles)
	if len(profiles) != 1 {
		t.Fatalf("expected 1 profile, got %d", len(profiles))
	}

	resp = e.request(http.MethodPatch, "/profiles/"+profile.ID, token, map[string]any{
		"values": map[string]string{"MQTT_HOST": "dev-broker-2", "REQUIRED_VAR": "x"},
	})
	if resp.status != http.StatusOK {
		t.Fatalf("update status = %d, body = %s", resp.status, resp.body)
	}
	var updated EnvironmentProfile
	resp.decode(t, &updated)
	if updated.Values["MQTT_HOST"] != "dev-broker-2" || updated.Values["REQUIRED_VAR"] != "x" {
		t.Fatalf("updated profile = %+v", updated)
	}

	resp = e.request(http.MethodDelete, "/profiles/"+profile.ID, token, nil)
	if resp.status != http.StatusNoContent {
		t.Fatalf("delete status = %d", resp.status)
	}
	resp = e.request(http.MethodGet, "/projects/"+project.ID+"/profiles", token, nil)
	resp.decode(t, &profiles)
	if len(profiles) != 0 {
		t.Fatalf("expected 0 profiles after delete, got %d", len(profiles))
	}
}

func TestVCS140_DeployRejectsWhenRequiredVariableMissing(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": flowContentWithEnv(),
	})
	var f Flow
	resp.decode(t, &f)

	// No profile selected, and REQUIRED_VAR has no default — deploy must
	// be rejected with a 400 naming the missing variable, not silently
	// deployed with a hole in its configuration.
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, nil)
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for a missing required env var, body = %s", resp.status, resp.body)
	}
	if !strings.Contains(string(resp.body), "REQUIRED_VAR") {
		t.Errorf("error body = %s, want it to name REQUIRED_VAR", resp.body)
	}
	if len(e.deployer.deployedTo) != 0 {
		t.Errorf("expected no deploy to have been pushed, got %d", len(e.deployer.deployedTo))
	}
}

func TestVCS140_DeployResolvesFromProfileAndFallsBackToDefault(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/profiles", token, map[string]any{
		"name": "prod", "values": map[string]string{"REQUIRED_VAR": "prod-value"},
	})
	var profile EnvironmentProfile
	resp.decode(t, &profile)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": flowContentWithEnv(),
	})
	var f Flow
	resp.decode(t, &f)

	// MQTT_HOST is NOT in the profile — it must fall back to its declared
	// default ("localhost"). REQUIRED_VAR comes from the profile.
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, map[string]any{"profileId": profile.ID})
	if resp.status != http.StatusCreated {
		t.Fatalf("deploy status = %d, body = %s", resp.status, resp.body)
	}

	if len(e.deployer.deployedTo) != 1 {
		t.Fatalf("expected exactly 1 deploy, got %d", len(e.deployer.deployedTo))
	}
	resolved := e.deployer.deployedTo[0].ResolvedEnv
	if resolved["MQTT_HOST"] != "localhost" {
		t.Errorf("resolved MQTT_HOST = %q, want the declared default \"localhost\"", resolved["MQTT_HOST"])
	}
	if resolved["REQUIRED_VAR"] != "prod-value" {
		t.Errorf("resolved REQUIRED_VAR = %q, want the profile's value \"prod-value\"", resolved["REQUIRED_VAR"])
	}

	// The flow's active profile should now be persisted, so a later
	// re-deploy without repeating profileId still resolves the same way.
	resp = e.request(http.MethodGet, "/flows/"+f.ID, token, nil)
	var reloaded Flow
	resp.decode(t, &reloaded)
	if reloaded.ActiveProfileID == nil || *reloaded.ActiveProfileID != profile.ID {
		t.Errorf("flow.ActiveProfileID = %v, want %q", reloaded.ActiveProfileID, profile.ID)
	}
}

func TestVCS140_ProfileCreateRequiresEditorRole(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)

	viewerToken := e.createUserAndLogin("viewer", "")
	viewerID := viewerUserID(t, e, "viewer")
	resp = e.request(http.MethodPut, "/projects/"+project.ID+"/members/"+viewerID, owner, map[string]string{"role": "viewer"})
	if resp.status != http.StatusOK {
		t.Fatalf("adding viewer member status = %d, body = %s", resp.status, resp.body)
	}

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/profiles", viewerToken, map[string]any{"name": "dev"})
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a Viewer creating a profile", resp.status)
	}
}
