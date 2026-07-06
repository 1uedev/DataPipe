// Package api implements the REST handlers described by docs/api/openapi.yaml
// (Development-Plan Increment 3): everything the editor UI can do goes
// through this API (ARC-110) — projects, flows CRUD + deploy + immutable
// version history (VCS-110), connections + write-only credentials
// (SEC-120), users/RBAC (SEC-100/110), audit log (SEC-140), runtimes.
package api

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func readJSON(r *http.Request, v any) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// currentUser fetches the authenticated user auth.Middleware attached, or
// writes 401 and returns ok=false.
func currentUser(w http.ResponseWriter, r *http.Request) (*auth.User, bool) {
	u, ok := auth.UserFromContext(r.Context())
	if !ok {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return nil, false
	}
	return u, true
}

// requireProjectRole writes 403 and returns false if user doesn't have min
// on projectID.
func requireProjectRole(w http.ResponseWriter, r *http.Request, authStore *auth.Store, user *auth.User, projectID string, min auth.ProjectRole) bool {
	if err := authStore.RequireProjectRole(r.Context(), user, projectID, min); err != nil {
		if errors.Is(err, auth.ErrForbidden) {
			writeError(w, http.StatusForbidden, "forbidden")
			return false
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return false
	}
	return true
}

func requireSystemAdmin(w http.ResponseWriter, user *auth.User) bool {
	if user.SystemRole != auth.SystemRoleAdmin {
		writeError(w, http.StatusForbidden, "forbidden")
		return false
	}
	return true
}
