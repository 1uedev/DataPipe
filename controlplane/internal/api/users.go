package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// userResponse is the public-safe view of auth.User — never carries
// PasswordHash (SEC-120's "write-only" spirit applies to password hashes
// too: they're for verification, never for display).
type userResponse struct {
	ID         string    `json:"id"`
	Username   string    `json:"username"`
	SystemRole string    `json:"systemRole"`
	CreatedAt  time.Time `json:"createdAt"`
}

func toUserResponse(u *auth.User) userResponse {
	return userResponse{ID: u.ID, Username: u.Username, SystemRole: string(u.SystemRole), CreatedAt: u.CreatedAt}
}

func (h *Handlers) login(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	user, err := h.authStore.Authenticate(r.Context(), req.Username, req.Password)
	if errors.Is(err, auth.ErrInvalidCredentials) {
		writeError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	token, expiresAt, err := h.authStore.CreateSession(r.Context(), user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "auth.login", "user", user.ID, "", nil, nil)
	writeJSON(w, http.StatusOK, map[string]any{"token": token, "expiresAt": expiresAt})
}

func (h *Handlers) logout(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	token := r.Header.Get("Authorization")
	const prefix = "Bearer "
	if len(token) > len(prefix) {
		token = token[len(prefix):]
	}
	if err := h.authStore.RevokeSession(r.Context(), token); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "auth.logout", "user", user.ID, "", nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) me(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, toUserResponse(user))
}

func (h *Handlers) listUsers(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	users, err := h.authStore.ListUsers(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	resp := make([]userResponse, len(users))
	for i, u := range users {
		resp[i] = toUserResponse(u)
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handlers) createUser(w http.ResponseWriter, r *http.Request) {
	actor, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, actor) {
		return
	}
	var req struct {
		Username   string `json:"username"`
		Password   string `json:"password"`
		SystemRole string `json:"systemRole"`
	}
	if err := readJSON(r, &req); err != nil || req.Username == "" || req.Password == "" {
		writeError(w, http.StatusBadRequest, "username and password are required")
		return
	}
	role := auth.SystemRoleNone
	if req.SystemRole == string(auth.SystemRoleAdmin) {
		role = auth.SystemRoleAdmin
	}

	u, err := h.authStore.CreateUser(r.Context(), req.Username, req.Password, role)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.audit(r, actor.ID, "user.create", "user", u.ID, "", nil, toUserResponse(u))
	writeJSON(w, http.StatusCreated, toUserResponse(u))
}
