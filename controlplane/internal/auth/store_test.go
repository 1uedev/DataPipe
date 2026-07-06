package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/controlplane/internal/db"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	d, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}
	return NewStore(d)
}

func TestSEC100_CreateUserAndAuthenticate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if u.PasswordHash == "correct-horse-battery" {
		t.Fatal("password stored in plaintext")
	}

	got, err := s.Authenticate(ctx, "alice", "correct-horse-battery")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if got.ID != u.ID {
		t.Errorf("authenticated user id = %q, want %q", got.ID, u.ID)
	}
}

func TestSEC100_AuthenticateRejectsWrongPassword(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	if _, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone); err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	_, err := s.Authenticate(ctx, "alice", "wrong-password")
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("Authenticate wrong password err = %v, want ErrInvalidCredentials", err)
	}
}

func TestSEC100_AuthenticateUnknownUserIsGenericError(t *testing.T) {
	s := newTestStore(t)
	_, err := s.Authenticate(context.Background(), "nobody", "whatever")
	if !errors.Is(err, ErrInvalidCredentials) {
		t.Errorf("Authenticate unknown user err = %v, want ErrInvalidCredentials (no user-enumeration leak)", err)
	}
}

func TestSEC100_PasswordPolicyRejectsShortPasswords(t *testing.T) {
	s := newTestStore(t)
	_, err := s.CreateUser(context.Background(), "alice", "short", SystemRoleNone)
	if err == nil {
		t.Error("expected CreateUser to reject a too-short password")
	}
}

func TestSEC100_SessionLifecycle(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	token, expiresAt, err := s.CreateSession(ctx, u.ID)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if !expiresAt.After(time.Now()) {
		t.Error("session expiry should be in the future")
	}

	got, err := s.ValidateSession(ctx, token)
	if err != nil {
		t.Fatalf("ValidateSession: %v", err)
	}
	if got.ID != u.ID {
		t.Errorf("session user = %q, want %q", got.ID, u.ID)
	}

	if err := s.RevokeSession(ctx, token); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
	if _, err := s.ValidateSession(ctx, token); !errors.Is(err, ErrUnauthorized) {
		t.Errorf("ValidateSession after revoke = %v, want ErrUnauthorized", err)
	}
}

func TestSEC100_ValidateSessionRejectsUnknownToken(t *testing.T) {
	s := newTestStore(t)
	if _, err := s.ValidateSession(context.Background(), "not-a-real-token"); !errors.Is(err, ErrUnauthorized) {
		t.Errorf("err = %v, want ErrUnauthorized", err)
	}
}

func TestSEC110_SystemAdminBypassesProjectScoping(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	admin, err := s.CreateUser(ctx, "root", "correct-horse-battery", SystemRoleAdmin)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	// admin has no project_members row at all, yet must pass every check.
	if err := s.RequireProjectRole(ctx, admin, "some-project", RoleProjectAdmin); err != nil {
		t.Errorf("system admin RequireProjectRole = %v, want nil", err)
	}
}

func TestSEC110_NonMemberIsForbidden(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}

	err = s.RequireProjectRole(ctx, u, "some-project", RoleViewer)
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("non-member RequireProjectRole = %v, want ErrForbidden", err)
	}
}

func TestSEC110_RoleRankingEnforced(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := s.SetProjectRole(ctx, "proj-1", u.ID, RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	if err := s.RequireProjectRole(ctx, u, "proj-1", RoleViewer); err != nil {
		t.Errorf("viewer requiring viewer = %v, want nil", err)
	}
	if err := s.RequireProjectRole(ctx, u, "proj-1", RoleEditor); !errors.Is(err, ErrForbidden) {
		t.Errorf("viewer requiring editor = %v, want ErrForbidden", err)
	}

	// Promote to editor: now editor-level actions succeed too.
	if err := s.SetProjectRole(ctx, "proj-1", u.ID, RoleEditor); err != nil {
		t.Fatalf("SetProjectRole (promote): %v", err)
	}
	if err := s.RequireProjectRole(ctx, u, "proj-1", RoleEditor); err != nil {
		t.Errorf("after promotion, editor requiring editor = %v, want nil", err)
	}
}

func TestSEC110_RoleIsScopedPerProject(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := s.SetProjectRole(ctx, "proj-1", u.ID, RoleProjectAdmin); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	if err := s.RequireProjectRole(ctx, u, "proj-1", RoleProjectAdmin); err != nil {
		t.Errorf("proj-1 admin requiring admin = %v, want nil", err)
	}
	if err := s.RequireProjectRole(ctx, u, "proj-2", RoleViewer); !errors.Is(err, ErrForbidden) {
		t.Errorf("same user on a different project (proj-2) = %v, want ErrForbidden", err)
	}
}

func TestSEC110_RemoveProjectMemberRevokesAccess(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	if err := s.SetProjectRole(ctx, "proj-1", u.ID, RoleEditor); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}
	if err := s.RemoveProjectMember(ctx, "proj-1", u.ID); err != nil {
		t.Fatalf("RemoveProjectMember: %v", err)
	}
	if err := s.RequireProjectRole(ctx, u, "proj-1", RoleViewer); !errors.Is(err, ErrForbidden) {
		t.Errorf("after removal = %v, want ErrForbidden", err)
	}
}

func TestMiddleware_RejectsMissingAndInvalidTokens(t *testing.T) {
	s := newTestStore(t)
	handler := s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("no Authorization header: status = %d, want 401", rec.Code)
	}

	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer bogus-token")
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusUnauthorized {
		t.Errorf("bogus token: status = %d, want 401", rec2.Code)
	}
}

func TestMiddleware_AttachesUserOnValidSession(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "alice", "correct-horse-battery", SystemRoleNone)
	if err != nil {
		t.Fatalf("CreateUser: %v", err)
	}
	token, _, err := s.CreateSession(ctx, u.ID)
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}

	var gotID string
	handler := s.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, ok := UserFromContext(r.Context())
		if !ok {
			t.Error("expected user in request context")
		} else {
			gotID = user.ID
		}
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if gotID != u.ID {
		t.Errorf("context user id = %q, want %q", gotID, u.ID)
	}
}
