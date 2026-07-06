package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func TestCON140_TestConnectionUnknownTypeReportsNoLiveTestAvailable(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	project := createProjectOnly(t, e, token)

	resp := e.request(http.MethodPost, "/projects/"+project.ID+"/connections", token, map[string]any{
		"name": "some-http-conn", "type": "http", "config": map[string]any{},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create connection status = %d, body = %s", resp.status, resp.body)
	}
	var conn Connection
	resp.decode(t, &conn)

	resp = e.request(http.MethodPost, "/connections/"+conn.ID+"/test", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("test connection status = %d, body = %s", resp.status, resp.body)
	}
	var result struct {
		OK      bool   `json:"ok"`
		Message string `json:"message"`
	}
	resp.decode(t, &result)
	if !result.OK {
		t.Errorf("expected ok=true for a type with no live test, got %+v", result)
	}
}

func TestCON140_TestConnectionPostgresUnreachableFails(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	project := createProjectOnly(t, e, token)

	resp := e.request(http.MethodPost, "/projects/"+project.ID+"/connections", token, map[string]any{
		"name": "unreachable-pg", "type": "postgres",
		"config": map[string]any{"host": "127.0.0.1", "port": 1, "database": "nope"},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create connection status = %d, body = %s", resp.status, resp.body)
	}
	var conn Connection
	resp.decode(t, &conn)

	resp = e.request(http.MethodPost, "/connections/"+conn.ID+"/test", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("test connection status = %d, body = %s", resp.status, resp.body)
	}
	var result struct {
		OK      bool   `json:"ok"`
		Message string `json:"message"`
	}
	resp.decode(t, &result)
	if result.OK {
		t.Error("expected ok=false: nothing listens on that port")
	}
}

func TestCON140_TestConnectionUnknownConnectionId(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodPost, "/connections/does-not-exist/test", token, nil)
	if resp.status != http.StatusNotFound {
		t.Errorf("status = %d, want 404, body = %s", resp.status, resp.body)
	}
}

func TestCON140_TestConnectionRequiresEditor(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project := createProjectOnly(t, e, owner)

	resp := e.request(http.MethodPost, "/projects/"+project.ID+"/connections", owner, map[string]any{
		"name": "some-http-conn", "type": "http", "config": map[string]any{},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create connection status = %d, body = %s", resp.status, resp.body)
	}
	var conn Connection
	resp.decode(t, &conn)

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), auth.RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	resp = e.request(http.MethodPost, "/connections/"+conn.ID+"/test", viewer, nil)
	if resp.status != http.StatusForbidden {
		t.Errorf("viewer testing a connection status = %d, want 403", resp.status)
	}
}

func createProjectOnly(t *testing.T, e *testEnv, token string) Project {
	t.Helper()
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "conntest-project"})
	if resp.status != http.StatusCreated {
		t.Fatalf("create project status = %d, body = %s", resp.status, resp.body)
	}
	var project Project
	resp.decode(t, &project)
	return project
}
