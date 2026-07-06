package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func TestSEC110_ViewerCannotCreateFlow(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), auth.RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", viewer, map[string]any{"name": "f", "content": sampleFlowContent()})
	if resp.status != http.StatusForbidden {
		t.Errorf("viewer creating a flow status = %d, want 403", resp.status)
	}

	// But a viewer CAN read.
	resp = e.request(http.MethodGet, "/projects/"+project.ID+"/flows", viewer, nil)
	if resp.status != http.StatusOK {
		t.Errorf("viewer listing flows status = %d, want 200", resp.status)
	}
}

func TestSEC110_OperatorCannotEditOrDeploy(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", owner, map[string]any{"name": "f", "content": sampleFlowContent()})
	var f Flow
	resp.decode(t, &f)

	operator := e.createUserAndLogin("operator", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, operator), auth.RoleOperator); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	resp = e.request(http.MethodPatch, "/flows/"+f.ID, operator, map[string]any{"name": "renamed"})
	if resp.status != http.StatusForbidden {
		t.Errorf("operator editing a flow status = %d, want 403", resp.status)
	}
	resp = e.request(http.MethodPost, "/flows/"+f.ID+"/deploy", operator, nil)
	if resp.status != http.StatusForbidden {
		t.Errorf("operator deploying a flow status = %d, want 403", resp.status)
	}
}

func TestSEC110_EditorCannotDeleteProjectOrManageMembers(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	editor := e.createUserAndLogin("editor", "")
	editorID := mustUserID(t, e, editor)
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, editorID, auth.RoleEditor); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	// Editor CAN create a flow...
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", editor, map[string]any{"name": "f", "content": sampleFlowContent()})
	if resp.status != http.StatusCreated {
		t.Errorf("editor creating a flow status = %d, want 201", resp.status)
	}
	// ...but cannot delete the project or grant roles (Project Admin only).
	resp = e.request(http.MethodDelete, "/projects/"+project.ID, editor, nil)
	if resp.status != http.StatusForbidden {
		t.Errorf("editor deleting project status = %d, want 403", resp.status)
	}
	resp = e.request(http.MethodPut, "/projects/"+project.ID+"/members/someone", editor, map[string]string{"role": "viewer"})
	if resp.status != http.StatusForbidden {
		t.Errorf("editor granting a role status = %d, want 403", resp.status)
	}
}

func TestSEC110_NonMemberCannotSeeProject(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	outsider := e.createUserAndLogin("outsider", "")
	resp = e.request(http.MethodGet, "/projects/"+project.ID, outsider, nil)
	if resp.status != http.StatusForbidden {
		t.Errorf("non-member reading project status = %d, want 403", resp.status)
	}
}

func TestSEC110_SystemAdminBypassesEveryProjectCheck(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	resp := e.request(http.MethodPost, "/projects", owner, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	admin := e.createUserAndLogin("root", auth.SystemRoleAdmin)
	resp = e.request(http.MethodPatch, "/projects/"+project.ID, admin, map[string]string{"name": "renamed by admin"})
	if resp.status != http.StatusOK {
		t.Errorf("system admin editing someone else's project status = %d, want 200", resp.status)
	}
}

func TestSEC110_OnlySystemAdminCanCreateUsers(t *testing.T) {
	e := newTestEnv(t)
	regular := e.createUserAndLogin("alice", "")
	resp := e.request(http.MethodPost, "/users", regular, map[string]string{"username": "bob", "password": "correct-horse-battery-staple"})
	if resp.status != http.StatusForbidden {
		t.Errorf("non-admin creating a user status = %d, want 403", resp.status)
	}

	admin := e.createUserAndLogin("root", auth.SystemRoleAdmin)
	resp = e.request(http.MethodPost, "/users", admin, map[string]string{"username": "bob", "password": "correct-horse-battery-staple"})
	if resp.status != http.StatusCreated {
		t.Errorf("admin creating a user status = %d, want 201", resp.status)
	}
}

func TestUnauthenticated_RequestsAreRejected(t *testing.T) {
	e := newTestEnv(t)
	resp := e.request(http.MethodGet, "/projects", "", nil)
	if resp.status != http.StatusUnauthorized {
		t.Errorf("unauthenticated request status = %d, want 401", resp.status)
	}
}

// --- test helpers specific to RBAC tests ---

func mustUserID(t *testing.T, e *testEnv, token string) string {
	t.Helper()
	resp := e.request(http.MethodGet, "/auth/me", token, nil)
	var u userResponse
	resp.decode(t, &u)
	return u.ID
}
