package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

// flowContentWithConnection is a minimal valid flow whose single node
// references a connection — used to prove export/import correctly
// rewrites and remaps that reference.
func flowContentWithConnection(connectionID string) json.RawMessage {
	return json.RawMessage(`{
		"formatVersion": 1,
		"kind": "flow",
		"id": "flow_conn_test",
		"name": "conn test",
		"mode": "streaming",
		"graph": {
			"nodes": [
				{"id": "n1", "type": "inject", "typeVersion": 1, "config": {"payload": {"value": 1}, "repeatMs": 0}},
				{"id": "n2", "type": "debug-log", "typeVersion": 1, "connection": "` + connectionID + `", "config": {}}
			],
			"wires": [
				{"id": "w1", "from": {"node": "n1", "port": "out"}, "to": {"node": "n2", "port": "in"}}
			]
		}
	}`)
}

// TestVCS130_ExportFlowRewritesConnectionToRef proves a single-flow export
// replaces the node's real connection id with a bundle-local ref and
// includes that connection's portable (secret-free) definition, without
// ever exposing a credential id or value.
func TestVCS130_ExportFlowRewritesConnectionToRef(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/connections", token, map[string]any{
		"name": "mqtt-broker", "type": "mqtt", "config": map[string]any{"host": "localhost"},
	})
	var conn Connection
	resp.decode(t, &conn)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": flowContentWithConnection(conn.ID),
	})
	var f Flow
	resp.decode(t, &f)

	resp = e.request(http.MethodGet, "/flows/"+f.ID+"/export", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("export status = %d, body = %s", resp.status, resp.body)
	}
	var bundle FlowExportBundle
	resp.decode(t, &bundle)

	if len(bundle.Flows) != 1 || len(bundle.Connections) != 1 {
		t.Fatalf("bundle = %+v, want exactly 1 flow and 1 connection", bundle)
	}
	if bundle.Connections[0].Name != "mqtt-broker" || bundle.Connections[0].Type != "mqtt" {
		t.Errorf("exported connection = %+v", bundle.Connections[0])
	}
	if strings.Contains(string(bundle.Flows[0].Content), conn.ID) {
		t.Errorf("exported flow content still contains the real connection id %q — should be rewritten to a ref", conn.ID)
	}
	if !strings.Contains(string(bundle.Flows[0].Content), bundle.Connections[0].Ref) {
		t.Errorf("exported flow content does not reference the bundle's connection ref %q", bundle.Connections[0].Ref)
	}
	if raw, _ := json.Marshal(bundle); strings.Contains(strings.ToLower(string(raw)), "credential") == false {
		// hasCredential field is expected to be present (false) — this just
		// confirms we didn't accidentally strip the field name itself away.
		t.Errorf("expected a hasCredential field in the export, found none in %s", raw)
	}
}

// TestVCS130_ImportMatchesExistingConnectionByNameAndType covers the
// "remap" half: importing into a project that already has a same-name-
// and-type connection reuses it rather than creating a duplicate.
func TestVCS130_ImportMatchesExistingConnectionByNameAndType(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Source"})
	var source Project
	resp.decode(t, &source)
	resp = e.request(http.MethodPost, "/projects/"+source.ID+"/connections", token, map[string]any{
		"name": "mqtt-broker", "type": "mqtt", "config": map[string]any{"host": "localhost"},
	})
	var sourceConn Connection
	resp.decode(t, &sourceConn)
	resp = e.request(http.MethodPost, "/projects/"+source.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": flowContentWithConnection(sourceConn.ID),
	})
	var sourceFlow Flow
	resp.decode(t, &sourceFlow)
	resp = e.request(http.MethodGet, "/flows/"+sourceFlow.ID+"/export", token, nil)
	var bundle FlowExportBundle
	resp.decode(t, &bundle)

	resp = e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Target"})
	var target Project
	resp.decode(t, &target)
	resp = e.request(http.MethodPost, "/projects/"+target.ID+"/connections", token, map[string]any{
		"name": "mqtt-broker", "type": "mqtt", "config": map[string]any{"host": "prod-broker"},
	})
	var targetConn Connection
	resp.decode(t, &targetConn)

	resp = e.request(http.MethodPost, "/projects/"+target.ID+"/import", token, bundle)
	if resp.status != http.StatusCreated {
		t.Fatalf("import status = %d, body = %s", resp.status, resp.body)
	}
	var result ImportResult
	resp.decode(t, &result)

	if len(result.ConnectionsCreated) != 0 {
		t.Errorf("ConnectionsCreated = %+v, want none (should match the existing target connection)", result.ConnectionsCreated)
	}
	if len(result.ConnectionsMatched) != 1 || result.ConnectionsMatched[0].ID != targetConn.ID {
		t.Fatalf("ConnectionsMatched = %+v, want exactly the target's existing connection %s", result.ConnectionsMatched, targetConn.ID)
	}
	if len(result.Flows) != 1 {
		t.Fatalf("Flows = %+v, want exactly 1 imported flow", result.Flows)
	}
	if !strings.Contains(string(result.Flows[0].Content), targetConn.ID) {
		t.Errorf("imported flow content does not reference the matched target connection id %q", targetConn.ID)
	}
}

// TestVCS130_ImportWithoutMatchCreatesCredentialLessConnection covers the
// other half: no matching connection in the target project creates a new
// one with no credential attached.
func TestVCS130_ImportWithoutMatchCreatesCredentialLessConnection(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Source"})
	var source Project
	resp.decode(t, &source)
	resp = e.request(http.MethodPost, "/projects/"+source.ID+"/connections", token, map[string]any{
		"name": "mqtt-broker", "type": "mqtt", "config": map[string]any{"host": "localhost"},
	})
	var sourceConn Connection
	resp.decode(t, &sourceConn)
	resp = e.request(http.MethodPost, "/projects/"+source.ID+"/flows", token, map[string]any{
		"name": "demo flow", "content": flowContentWithConnection(sourceConn.ID),
	})
	var sourceFlow Flow
	resp.decode(t, &sourceFlow)
	resp = e.request(http.MethodGet, "/flows/"+sourceFlow.ID+"/export", token, nil)
	var bundle FlowExportBundle
	resp.decode(t, &bundle)

	resp = e.request(http.MethodPost, "/projects", token, map[string]string{"name": "Target"})
	var target Project
	resp.decode(t, &target)

	resp = e.request(http.MethodPost, "/projects/"+target.ID+"/import", token, bundle)
	if resp.status != http.StatusCreated {
		t.Fatalf("import status = %d, body = %s", resp.status, resp.body)
	}
	var result ImportResult
	resp.decode(t, &result)

	if len(result.ConnectionsCreated) != 1 || result.ConnectionsCreated[0].CredentialID != nil {
		t.Fatalf("ConnectionsCreated = %+v, want exactly 1 new connection with no credential", result.ConnectionsCreated)
	}
	if result.ConnectionsCreated[0].Name != "mqtt-broker" {
		t.Errorf("created connection name = %q, want mqtt-broker", result.ConnectionsCreated[0].Name)
	}
}

func TestVCS130_ExportRequiresProjectMembership(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	outsider := e.createUserAndLogin("outsider", "")

	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "Private"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodGet, "/projects/"+project.ID+"/export", outsider, nil)
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a non-member", resp.status)
	}
}

func TestVCS130_ImportRequiresEditorRole(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")

	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "Line 1"})
	var project Project
	resp.decode(t, &project)

	viewer := e.createUserAndLogin("viewer", "")
	resp = e.request(http.MethodPut, "/projects/"+project.ID+"/members/"+viewerUserID(t, e, "viewer"), owner, map[string]string{"role": "viewer"})
	if resp.status != http.StatusOK {
		t.Fatalf("adding viewer member status = %d, body = %s", resp.status, resp.body)
	}

	bundle := FlowExportBundle{FormatVersion: 1, Flows: []ExportedFlow{{Name: "x", Content: sampleFlowContent()}}}
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/import", viewer, bundle)
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a Viewer trying to import", resp.status)
	}
}

// viewerUserID looks up a just-created user's id via the auth store
// directly — there is no "get user by username" REST endpoint, and the
// project-members endpoint needs a user id, not a username.
func viewerUserID(t *testing.T, e *testEnv, username string) string {
	t.Helper()
	user, err := e.authStore.Authenticate(t.Context(), username, "correct-horse-battery-staple")
	if err != nil {
		t.Fatalf("looking up user %q: %v", username, err)
	}
	return user.ID
}
