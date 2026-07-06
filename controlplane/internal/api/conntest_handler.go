// CON-140 "test connection": decrypts the connection's credential
// server-side (reusing the same ConnectionResolver built for the runtime's
// CON-110 resolution path) and attempts a real, bounded connectivity check
// via controlplane/internal/conntest. The decrypted credential never
// leaves this handler.
package api

import (
	"net/http"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/conntest"
)

func (h *Handlers) testConnection(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	c, ok := h.connectionAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}

	info, err := NewConnectionResolver(h.store, h.vault).ResolveConnection(r.Context(), c.ID)
	if err != nil {
		writeJSON(w, http.StatusOK, conntest.Result{OK: false, Message: err.Error()})
		return
	}
	result := conntest.Test(r.Context(), info.Type, info.ConfigJSON, info.CredentialJSON)
	h.audit(r, user.ID, "connection.test", "connection", c.ID, c.ProjectID, nil, result)
	writeJSON(w, http.StatusOK, result)
}
