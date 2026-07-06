package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// TestARC110_FullFlowLifecycleViaRESTOnly is Increment 3's headline
// acceptance criterion: create a project, create a flow, deploy it (which
// validates, snapshots an immutable version, and pushes it to a runtime),
// list/get its version history, and roll back — entirely through the REST
// API, no other path.
func TestARC110_FullFlowLifecycleViaRESTOnly(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	// Create project.
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 3", "description": "demo"})
	if resp.status != http.StatusCreated {
		t.Fatalf("create project status = %d, body = %s", resp.status, resp.body)
	}
	var project Project
	resp.decode(t, &project)

	// Create flow (draft).
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name":    "demo flow",
		"content": json.RawMessage(sampleFlowContent()),
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create flow status = %d, body = %s", resp.status, resp.body)
	}
	var f Flow
	resp.decode(t, &f)
	if f.DeployedVersion != nil {
		t.Errorf("new flow DeployedVersion = %v, want nil (not deployed yet)", f.DeployedVersion)
	}

	// Deploy it.
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, map[string]string{"comment": "first deploy"})
	if resp.status != http.StatusCreated {
		t.Fatalf("deploy status = %d, body = %s", resp.status, resp.body)
	}
	var v1 FlowVersion
	resp.decode(t, &v1)
	if v1.Version != 1 {
		t.Errorf("first deploy version = %d, want 1", v1.Version)
	}
	if len(e.deployer.deployedTo) != 1 || e.deployer.deployedTo[0].FlowID != f.ID {
		t.Fatalf("deployer.deployedTo = %+v, want one call for flow %s", e.deployer.deployedTo, f.ID)
	}

	// The flow now reports deployedVersion = 1.
	resp = e.request(http.MethodGet, "/flows/"+f.ID, token, nil)
	var afterDeploy Flow
	resp.decode(t, &afterDeploy)
	if afterDeploy.DeployedVersion == nil || *afterDeploy.DeployedVersion != 1 {
		t.Errorf("flow.DeployedVersion after deploy = %v, want 1", afterDeploy.DeployedVersion)
	}

	// Edit the draft and deploy again -> version 2.
	resp = e.request(http.MethodPatch, "/flows/"+f.ID, token, map[string]any{"content": json.RawMessage(strings.Replace(string(sampleFlowContent()), `"value": 1`, `"value": 2`, 1))})
	if resp.status != http.StatusOK {
		t.Fatalf("update draft status = %d, body = %s", resp.status, resp.body)
	}
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, map[string]string{"comment": "second deploy"})
	if resp.status != http.StatusCreated {
		t.Fatalf("second deploy status = %d, body = %s", resp.status, resp.body)
	}
	var v2 FlowVersion
	resp.decode(t, &v2)
	if v2.Version != 2 {
		t.Errorf("second deploy version = %d, want 2", v2.Version)
	}

	// Version history: newest first, both versions present, v1 unchanged.
	resp = e.request(http.MethodGet, "/flows/"+f.ID+"/versions", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list versions status = %d, body = %s", resp.status, resp.body)
	}
	var versions []FlowVersion
	resp.decode(t, &versions)
	if len(versions) != 2 || versions[0].Version != 2 || versions[1].Version != 1 {
		t.Fatalf("versions = %+v, want [2, 1]", versions)
	}

	// Roll back to version 1: creates version 3 with v1's content, never
	// rewrites history (VCS-110's "one-click rollback as a new version").
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/versions/1/rollback", token, nil)
	if resp.status != http.StatusCreated {
		t.Fatalf("rollback status = %d, body = %s", resp.status, resp.body)
	}
	var v3 FlowVersion
	resp.decode(t, &v3)
	if v3.Version != 3 {
		t.Errorf("rollback created version = %d, want 3", v3.Version)
	}
	if string(v3.Content) != string(v1.Content) {
		t.Errorf("rollback content = %s, want v1's content %s", v3.Content, v1.Content)
	}

	resp = e.request(http.MethodGet, "/flows/"+f.ID+"/versions", token, nil)
	resp.decode(t, &versions)
	if len(versions) != 3 {
		t.Fatalf("after rollback, versions = %+v, want 3 entries (history never rewritten)", versions)
	}
}

func TestARC110_DeployRejectsInvalidFlowContent(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name":    "bad flow",
		"content": json.RawMessage(`{"formatVersion":1,"kind":"flow","id":"f1","name":"x","mode":"streaming","graph":{"nodes":[{"id":"n1","type":"does-not-exist","typeVersion":1}],"wires":[]}}`),
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create flow status = %d", resp.status)
	}
	var f Flow
	resp.decode(t, &f)

	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, nil)
	if resp.status != http.StatusBadRequest {
		t.Errorf("deploy with unregistered node type status = %d, want 400", resp.status)
	}
	if len(e.deployer.deployedTo) != 0 {
		t.Error("deployer should never be called for an invalid flow")
	}
}

func TestDeploy_NoRuntimeConnectedReturns409(t *testing.T) {
	e := newTestEnv(t)
	e.deployer.fail = true
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{"name": "f", "content": sampleFlowContent()})
	var f Flow
	resp.decode(t, &f)

	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", token, nil)
	if resp.status != http.StatusConflict {
		t.Errorf("deploy with no runtime status = %d, want 409", resp.status)
	}
}
