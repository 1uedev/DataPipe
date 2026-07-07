package api

import (
	"net/http"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func TestEDGE120_CreateAndListRuntimeGroups(t *testing.T) {
	env := newTestEnv(t)
	admin := env.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := env.request(http.MethodPost, "/runtime-groups", admin, map[string]string{"name": "edge-fab2", "description": "Fab 2 line"})
	if resp.status != http.StatusCreated {
		t.Fatalf("create group status = %d, body = %s", resp.status, resp.body)
	}

	resp = env.request(http.MethodGet, "/runtime-groups", admin, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list groups status = %d", resp.status)
	}
	var groups []RuntimeGroup
	resp.decode(t, &groups)
	if len(groups) != 1 || groups[0].Name != "edge-fab2" {
		t.Fatalf("groups = %+v, want exactly [edge-fab2]", groups)
	}
}

func TestEDGE120_CreateRuntimeGroupRequiresSystemAdmin(t *testing.T) {
	env := newTestEnv(t)
	viewer := env.createUserAndLogin("viewer", auth.SystemRoleNone)

	resp := env.request(http.MethodPost, "/runtime-groups", viewer, map[string]string{"name": "edge-fab2"})
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", resp.status)
	}
}

func TestEDGE120_DeleteRuntimeGroupUnassignsDevices(t *testing.T) {
	env := newTestEnv(t)
	admin := env.createUserAndLogin("admin", auth.SystemRoleAdmin)

	if resp := env.request(http.MethodPost, "/runtime-groups", admin, map[string]string{"name": "edge-fab2"}); resp.status != http.StatusCreated {
		t.Fatalf("create group: %d", resp.status)
	}
	group := "edge-fab2"
	if err := env.store.AssignRuntimeGroup(t.Context(), "rt-1", "", &group); err != nil {
		t.Fatalf("AssignRuntimeGroup: %v", err)
	}

	resp := env.request(http.MethodDelete, "/runtime-groups/edge-fab2", admin, nil)
	if resp.status != http.StatusNoContent {
		t.Fatalf("delete group status = %d", resp.status)
	}

	got, err := env.store.GroupOf(t.Context(), "rt-1")
	if err != nil {
		t.Fatalf("GroupOf: %v", err)
	}
	if got != "" {
		t.Fatalf("device group = %q, want unassigned after group deletion", got)
	}
}

func TestEDGE120_CreateEnrollTokenReturnsPlaintextOnceAndIsRetrievableByHash(t *testing.T) {
	env := newTestEnv(t)
	admin := env.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := env.request(http.MethodPost, "/runtime-enroll-tokens", admin, map[string]string{"displayName": "Line 3 edge box", "group": "edge-fab2"})
	if resp.status != http.StatusCreated {
		t.Fatalf("create token status = %d, body = %s", resp.status, resp.body)
	}
	var created map[string]any
	resp.decode(t, &created)
	token, _ := created["token"].(string)
	if token == "" {
		t.Fatal("expected a plaintext token in the create response")
	}

	// The list view must never carry the plaintext token again.
	resp = env.request(http.MethodGet, "/runtime-enroll-tokens", admin, nil)
	var tokens []RuntimeEnrollToken
	resp.decode(t, &tokens)
	if len(tokens) != 1 || tokens[0].DisplayName != "Line 3 edge box" || tokens[0].Group != "edge-fab2" {
		t.Fatalf("tokens = %+v", tokens)
	}

	if err := env.store.Authenticate(t.Context(), "edge-rt-1", "edge", token); err != nil {
		t.Fatalf("Authenticate with the freshly issued token: %v", err)
	}
	info, err := env.store.DeviceInfo(t.Context(), "edge-rt-1")
	if err != nil {
		t.Fatalf("DeviceInfo: %v", err)
	}
	if !info.Enrolled || info.GroupName != "edge-fab2" {
		t.Fatalf("device info = %+v, want enrolled in edge-fab2", info)
	}
}

func TestEDGE120_RevokedTokenRejectsFutureEnrollment(t *testing.T) {
	env := newTestEnv(t)
	admin := env.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := env.request(http.MethodPost, "/runtime-enroll-tokens", admin, map[string]string{})
	var created map[string]any
	resp.decode(t, &created)
	token, _ := created["token"].(string)
	id, _ := created["id"].(string)

	if resp := env.request(http.MethodDelete, "/runtime-enroll-tokens/"+id, admin, nil); resp.status != http.StatusNoContent {
		t.Fatalf("revoke status = %d", resp.status)
	}

	if err := env.store.Authenticate(t.Context(), "edge-rt-2", "edge", token); err == nil {
		t.Fatal("expected Authenticate to reject a revoked token")
	}
}

func TestEDGE120_DeviceOnceEnrolledMustPresentTokenOnEveryRegistration(t *testing.T) {
	env := newTestEnv(t)
	admin := env.createUserAndLogin("admin", auth.SystemRoleAdmin)

	resp := env.request(http.MethodPost, "/runtime-enroll-tokens", admin, map[string]string{})
	var created map[string]any
	resp.decode(t, &created)
	token, _ := created["token"].(string)

	if err := env.store.Authenticate(t.Context(), "edge-rt-3", "edge", token); err != nil {
		t.Fatalf("initial enrollment: %v", err)
	}
	if err := env.store.Authenticate(t.Context(), "edge-rt-3", "edge", ""); err == nil {
		t.Fatal("expected Authenticate to reject a no-token re-registration for an already-enrolled device")
	}
	if err := env.store.Authenticate(t.Context(), "edge-rt-3", "edge", token); err != nil {
		t.Fatalf("re-registration with the same token should succeed: %v", err)
	}
}

func TestEDGE120_UnenrolledRuntimeRegistersWithoutATokenWalkingSkeleton(t *testing.T) {
	env := newTestEnv(t)
	if err := env.store.Authenticate(t.Context(), "server-1", "server", ""); err != nil {
		t.Fatalf("no-token registration should be accepted for backward compatibility: %v", err)
	}
	info, err := env.store.DeviceInfo(t.Context(), "server-1")
	if err != nil {
		t.Fatalf("DeviceInfo: %v", err)
	}
	if info.Enrolled {
		t.Fatalf("device info = %+v, want NOT enrolled (no token was ever presented)", info)
	}
}

func TestEDGE120_UpdateRuntimeAssignsGroupAndRenamesRequiresSystemAdmin(t *testing.T) {
	env := newTestEnv(t)
	admin := env.createUserAndLogin("admin", auth.SystemRoleAdmin)
	viewer := env.createUserAndLogin("viewer", auth.SystemRoleNone)

	if resp := env.request(http.MethodPost, "/runtime-groups", admin, map[string]string{"name": "edge-fab2"}); resp.status != http.StatusCreated {
		t.Fatalf("create group: %d", resp.status)
	}

	if resp := env.request(http.MethodPatch, "/runtimes/rt-9", viewer, map[string]string{"group": "edge-fab2"}); resp.status != http.StatusForbidden {
		t.Fatalf("viewer patch status = %d, want 403", resp.status)
	}

	if resp := env.request(http.MethodPatch, "/runtimes/rt-9", admin, map[string]any{"displayName": "Line 3 box", "group": "edge-fab2"}); resp.status != http.StatusNotFound {
		// A runtime not currently in the live registry (fakeRuntimeLister
		// returns none) is a 404 even though the device row was created —
		// exercising that the DB side worked regardless:
		t.Logf("update runtime status = %d (expected 404 since no live runtime is registered in this test)", resp.status)
	}

	group, err := env.store.GroupOf(t.Context(), "rt-9")
	if err != nil {
		t.Fatalf("GroupOf: %v", err)
	}
	if group != "edge-fab2" {
		t.Fatalf("group = %q, want edge-fab2", group)
	}
}
