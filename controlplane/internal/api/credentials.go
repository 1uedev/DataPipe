package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
)

// CredentialMeta is the only representation of a credential ever exposed by
// the API (SEC-120: "write-only from the UI after creation; values never
// re-displayed or exported"). This package never pairs a credential's
// metadata with its decrypted value — crypto.Vault.Open is deliberately
// not called anywhere in the api package, only wherever a real consumer of
// the plaintext value eventually needs it (a connector, Increment 6+).
type CredentialMeta struct {
	ID        string    `json:"id"`
	ProjectID string    `json:"projectId"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"createdAt"`
}

func (s *Store) CreateCredential(ctx context.Context, projectID, name string, sealed crypto.Sealed) (*CredentialMeta, error) {
	m := &CredentialMeta{ID: uuid.NewString(), ProjectID: projectID, Name: name, CreatedAt: time.Now().UTC()}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO credentials (id, project_id, name, key_version, wrapped_dek, wrapped_dek_nonce, ciphertext, nonce, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ProjectID, m.Name, sealed.KeyVersion, sealed.WrappedDEK, sealed.WrappedDEKNonce, sealed.Ciphertext, sealed.Nonce, m.CreatedAt.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("api: creating credential: %w", err)
	}
	return m, nil
}

func (s *Store) ListCredentials(ctx context.Context, projectID string) ([]*CredentialMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, name, created_at FROM credentials WHERE project_id = ? ORDER BY name`, projectID)
	if err != nil {
		return nil, fmt.Errorf("api: listing credentials: %w", err)
	}
	defer func() { _ = rows.Close() }()

	metas := make([]*CredentialMeta, 0)
	for rows.Next() {
		var m CredentialMeta
		var createdAt string
		if err := rows.Scan(&m.ID, &m.ProjectID, &m.Name, &createdAt); err != nil {
			return nil, fmt.Errorf("api: scanning credential: %w", err)
		}
		t, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			return nil, fmt.Errorf("api: parsing created_at: %w", err)
		}
		m.CreatedAt = t
		metas = append(metas, &m)
	}
	return metas, rows.Err()
}

func (s *Store) getCredentialProjectID(ctx context.Context, id string) (string, error) {
	var projectID string
	row := s.db.QueryRowContext(ctx, `SELECT project_id FROM credentials WHERE id = ?`, id)
	if err := row.Scan(&projectID); err != nil {
		return "", err
	}
	return projectID, nil
}

// GetCredentialSealed returns a credential's still-encrypted envelope. The
// one legitimate consumer is ConnectionResolver (Increment 6, CON-110):
// decrypting is only ever done to hand the plaintext to the runtime that
// actually needs it to connect — never through any HTTP handler.
func (s *Store) GetCredentialSealed(ctx context.Context, id string) (*crypto.Sealed, error) {
	var sealed crypto.Sealed
	row := s.db.QueryRowContext(ctx, `SELECT key_version, wrapped_dek, wrapped_dek_nonce, ciphertext, nonce FROM credentials WHERE id = ?`, id)
	if err := row.Scan(&sealed.KeyVersion, &sealed.WrappedDEK, &sealed.WrappedDEKNonce, &sealed.Ciphertext, &sealed.Nonce); err != nil {
		return nil, err
	}
	return &sealed, nil
}

func (s *Store) DeleteCredential(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM credentials WHERE id = ?`, id)
	return err
}

// --- HTTP handlers ---

func (h *Handlers) listCredentials(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleViewer) {
		return
	}
	creds, err := h.store.ListCredentials(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, creds)
}

func (h *Handlers) createCredential(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleEditor) {
		return
	}
	var req struct {
		Name  string `json:"name"`
		Value any    `json:"value"`
	}
	if err := readJSON(r, &req); err != nil || req.Name == "" || req.Value == nil {
		writeError(w, http.StatusBadRequest, "name and value are required")
		return
	}

	valueJSON, err := json.Marshal(req.Value)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid value")
		return
	}
	sealed, err := h.vault.Seal(valueJSON)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	m, err := h.store.CreateCredential(r.Context(), projectID, req.Name, sealed)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// The value itself never appears in the audit log either — only that a
	// credential with this name/id was created.
	h.audit(r, user.ID, "credential.create", "credential", m.ID, projectID, nil, m)
	writeJSON(w, http.StatusCreated, m)
}

func (h *Handlers) deleteCredential(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	id := r.PathValue("credentialId")
	projectID, err := h.store.getCredentialProjectID(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleEditor) {
		return
	}
	if err := h.store.DeleteCredential(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "credential.delete", "credential", id, projectID, nil, nil)
	w.WriteHeader(http.StatusNoContent)
}
