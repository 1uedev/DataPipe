package api

import (
	"net/http"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// listAuditLog serves SEC-140's read side: System Admins see everything;
// everyone else must be at least Project Admin on the requested project
// (and a projectId is required for them — no cross-project browsing).
func (h *Handlers) listAuditLog(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.URL.Query().Get("projectId")

	if user.SystemRole != auth.SystemRoleAdmin {
		if projectID == "" {
			writeError(w, http.StatusForbidden, "forbidden")
			return
		}
		if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleProjectAdmin) {
			return
		}
	}

	entries, err := h.auditLog.List(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, entries)
}
