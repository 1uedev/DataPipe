package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// Connection is CON-110's centrally managed connection definition:
// non-secret settings plus an optional reference to a credential (never a
// secret value inline).
type Connection struct {
	ID           string          `json:"id"`
	ProjectID    string          `json:"projectId"`
	Name         string          `json:"name"`
	Type         string          `json:"type"`
	Config       json.RawMessage `json:"config"`
	CredentialID *string         `json:"credentialId"`
}

func (s *Store) CreateConnection(ctx context.Context, projectID, name, connType string, config json.RawMessage, credentialID *string) (*Connection, error) {
	c := &Connection{ID: uuid.NewString(), ProjectID: projectID, Name: name, Type: connType, Config: config, CredentialID: credentialID}
	_, err := s.db.ExecContext(ctx, `INSERT INTO connections (id, project_id, name, type, config, credential_id) VALUES (?, ?, ?, ?, ?, ?)`,
		c.ID, c.ProjectID, c.Name, c.Type, string(c.Config), nullableStringPtr(c.CredentialID))
	if err != nil {
		return nil, fmt.Errorf("api: creating connection: %w", err)
	}
	return c, nil
}

func (s *Store) GetConnection(ctx context.Context, id string) (*Connection, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, project_id, name, type, config, credential_id FROM connections WHERE id = ?`, id)
	return scanConnection(row)
}

func (s *Store) ListConnections(ctx context.Context, projectID string) ([]*Connection, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, name, type, config, credential_id FROM connections WHERE project_id = ? ORDER BY name`, projectID)
	if err != nil {
		return nil, fmt.Errorf("api: listing connections: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var conns []*Connection
	for rows.Next() {
		c, err := scanConnection(rows)
		if err != nil {
			return nil, err
		}
		conns = append(conns, c)
	}
	return conns, rows.Err()
}

func (s *Store) UpdateConnection(ctx context.Context, id string, name *string, config json.RawMessage, credentialID *string) (*Connection, error) {
	c, err := s.GetConnection(ctx, id)
	if err != nil {
		return nil, err
	}
	if name != nil {
		c.Name = *name
	}
	if config != nil {
		c.Config = config
	}
	if credentialID != nil {
		c.CredentialID = credentialID
	}
	_, err = s.db.ExecContext(ctx, `UPDATE connections SET name = ?, config = ?, credential_id = ? WHERE id = ?`,
		c.Name, string(c.Config), nullableStringPtr(c.CredentialID), c.ID)
	if err != nil {
		return nil, fmt.Errorf("api: updating connection: %w", err)
	}
	return c, nil
}

func (s *Store) DeleteConnection(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM connections WHERE id = ?`, id)
	return err
}

func scanConnection(row rowScanner) (*Connection, error) {
	var c Connection
	var config string
	var credentialID sql.NullString
	if err := row.Scan(&c.ID, &c.ProjectID, &c.Name, &c.Type, &config, &credentialID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning connection: %w", err)
	}
	c.Config = json.RawMessage(config)
	if credentialID.Valid {
		c.CredentialID = &credentialID.String
	}
	return &c, nil
}

func nullableStringPtr(s *string) any {
	if s == nil {
		return nil
	}
	return *s
}

// --- HTTP handlers ---

func (h *Handlers) listConnections(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleViewer) {
		return
	}
	conns, err := h.store.ListConnections(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, conns)
}

func (h *Handlers) createConnection(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleEditor) {
		return
	}
	var req struct {
		Name         string          `json:"name"`
		Type         string          `json:"type"`
		Config       json.RawMessage `json:"config"`
		CredentialID *string         `json:"credentialId"`
	}
	if err := readJSON(r, &req); err != nil || req.Name == "" || req.Type == "" {
		writeError(w, http.StatusBadRequest, "name and type are required")
		return
	}
	c, err := h.store.CreateConnection(r.Context(), projectID, req.Name, req.Type, req.Config, req.CredentialID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "connection.create", "connection", c.ID, projectID, nil, c)
	writeJSON(w, http.StatusCreated, c)
}

func (h *Handlers) connectionAndAuthorize(w http.ResponseWriter, r *http.Request, user *auth.User, min auth.ProjectRole) (*Connection, bool) {
	c, err := h.store.GetConnection(r.Context(), r.PathValue("connectionId"))
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return nil, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if !requireProjectRole(w, r, h.authStore, user, c.ProjectID, min) {
		return nil, false
	}
	return c, true
}

func (h *Handlers) updateConnection(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	before, ok := h.connectionAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	var req struct {
		Name         *string         `json:"name"`
		Config       json.RawMessage `json:"config"`
		CredentialID *string         `json:"credentialId"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	after, err := h.store.UpdateConnection(r.Context(), before.ID, req.Name, req.Config, req.CredentialID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "connection.update", "connection", before.ID, before.ProjectID, before, after)
	writeJSON(w, http.StatusOK, after)
}

func (h *Handlers) deleteConnection(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	c, ok := h.connectionAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	if err := h.store.DeleteConnection(r.Context(), c.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "connection.delete", "connection", c.ID, c.ProjectID, c, nil)
	w.WriteHeader(http.StatusNoContent)
}
