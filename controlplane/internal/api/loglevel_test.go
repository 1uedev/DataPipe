package api

import (
	"encoding/json"
	"net/http"
	"testing"
)

// TestOBS120_SetFlowLogLevelPersistsAndRePushesDeployedFlow covers OBS-120's
// "per-flow log level at runtime without redeploy": setting the level on an
// already-deployed flow persists it (visible on GetFlow) and re-pushes the
// flow's unchanged deployed content through the same Deployer path a real
// deploy uses, carrying the new level.
func TestOBS120_SetFlowLogLevelPersistsAndRePushesDeployedFlow(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": json.RawMessage(sampleFlowContent()),
	})
	var f Flow
	resp.decode(t, &f)
	if f.LogLevel != "info" {
		t.Errorf("new flow LogLevel = %q, want \"info\" (default)", f.LogLevel)
	}

	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, nil)
	if resp.status != http.StatusCreated {
		t.Fatalf("deploy status = %d, body = %s", resp.status, resp.body)
	}
	e.deployer.deployedTo = nil // reset so we can observe only the log-level re-push below

	resp = e.request(http.MethodPatch, "/flows/"+f.ID+"/log-level", token, map[string]string{"level": "debug"})
	if resp.status != http.StatusOK {
		t.Fatalf("set log level status = %d, body = %s", resp.status, resp.body)
	}
	var updated Flow
	resp.decode(t, &updated)
	if updated.LogLevel != "debug" {
		t.Errorf("LogLevel after set = %q, want \"debug\"", updated.LogLevel)
	}

	resp = e.request(http.MethodGet, "/flows/"+f.ID, token, nil)
	var reloaded Flow
	resp.decode(t, &reloaded)
	if reloaded.LogLevel != "debug" {
		t.Errorf("GetFlow LogLevel = %q, want \"debug\" (persisted)", reloaded.LogLevel)
	}

	if len(e.deployer.deployedTo) != 1 {
		t.Fatalf("expected exactly one re-push to the deployer, got %d", len(e.deployer.deployedTo))
	}
}

func TestOBS120_SetFlowLogLevelRejectsUnknownValue(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": json.RawMessage(sampleFlowContent()),
	})
	var f Flow
	resp.decode(t, &f)

	resp = e.request(http.MethodPatch, "/flows/"+f.ID+"/log-level", token, map[string]string{"level": "verbose"})
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for an unknown level", resp.status)
	}
}

func TestOBS120_SetFlowLogLevelOnUndeployedFlowDoesNotCallDeployer(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": json.RawMessage(sampleFlowContent()),
	})
	var f Flow
	resp.decode(t, &f)

	resp = e.request(http.MethodPatch, "/flows/"+f.ID+"/log-level", token, map[string]string{"level": "warn"})
	if resp.status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.status, resp.body)
	}
	if len(e.deployer.deployedTo) != 0 {
		t.Errorf("expected no deployer call for an undeployed flow, got %d", len(e.deployer.deployedTo))
	}
}
