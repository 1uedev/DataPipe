// Fleet management (Increment 9, EDGE-120): runtime groups, enrollment
// tokens (per-device credentials, ARC-210), and enrolled device records.
// Live health (online/cpu/memory/flow status) is NOT stored here — it
// stays in controlplane/internal/registry's in-memory state, refreshed on
// every Heartbeat; this file only persists admin-configured fleet state
// that must survive a control-plane restart.
package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// RuntimeGroup is a named fleet deployment target (UI-220's
// runtimeAssignment.group).
type RuntimeGroup struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

// RuntimeEnrollToken is a per-device credential (ARC-210) used to
// authenticate a runtime's Register call as a managed fleet device
// (EDGE-120). The plaintext value is never stored — only its hash.
type RuntimeEnrollToken struct {
	ID              string     `json:"id"`
	DisplayName     string     `json:"displayName"`
	Group           string     `json:"group"`
	CreatedAt       time.Time  `json:"createdAt"`
	UsedByRuntimeID string     `json:"usedByRuntimeId"`
	Revoked         bool       `json:"revoked"`
	revokedAt       *time.Time // internal only, not serialized
}

// DeviceInfo is one runtime's admin-configured fleet metadata, used both
// by the GET /runtimes inventory view and by registry.Service's group-
// targeted deploy filtering.
type DeviceInfo struct {
	Kind        string
	DisplayName string
	GroupName   string
	Enrolled    bool
}

func hashEnrollToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func generateEnrollToken() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// --- Store: runtime groups ---

func (s *Store) CreateRuntimeGroup(ctx context.Context, name, description string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO runtime_groups (name, description, created_at) VALUES (?, ?, ?)`,
		name, description, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("api: creating runtime group: %w", err)
	}
	return nil
}

func (s *Store) ListRuntimeGroups(ctx context.Context) ([]*RuntimeGroup, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT name, description, created_at FROM runtime_groups ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("api: listing runtime groups: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]*RuntimeGroup, 0)
	for rows.Next() {
		var g RuntimeGroup
		var createdAt string
		if err := rows.Scan(&g.Name, &g.Description, &createdAt); err != nil {
			return nil, fmt.Errorf("api: scanning runtime group: %w", err)
		}
		t, err := time.Parse(time.RFC3339, createdAt)
		if err != nil {
			return nil, fmt.Errorf("api: parsing created_at: %w", err)
		}
		g.CreatedAt = t
		out = append(out, &g)
	}
	return out, rows.Err()
}

// DeleteRuntimeGroup removes a group, unassigning any device or
// outstanding enrollment token that referenced it (application-level
// cleanup rather than a database cascade, so this works identically on
// SQLite and Postgres).
func (s *Store) DeleteRuntimeGroup(ctx context.Context, name string) error {
	if _, err := s.db.ExecContext(ctx, `UPDATE devices SET group_name = NULL WHERE group_name = ?`, name); err != nil {
		return fmt.Errorf("api: unassigning devices from group: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE runtime_enroll_tokens SET group_name = NULL WHERE group_name = ?`, name); err != nil {
		return fmt.Errorf("api: unassigning tokens from group: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM runtime_groups WHERE name = ?`, name); err != nil {
		return fmt.Errorf("api: deleting runtime group: %w", err)
	}
	return nil
}

// --- Store: enrollment tokens ---

// CreateEnrollToken issues a new enrollment token, returning its plaintext
// value (shown to the caller exactly once) and its stored id.
func (s *Store) CreateEnrollToken(ctx context.Context, displayName, group, createdByUserID string) (id, plaintext string, err error) {
	plaintext, err = generateEnrollToken()
	if err != nil {
		return "", "", fmt.Errorf("api: generating enrollment token: %w", err)
	}
	id = uuid.NewString()
	var groupArg any
	if group != "" {
		groupArg = group
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO runtime_enroll_tokens (id, token_hash, display_name, group_name, created_by, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		id, hashEnrollToken(plaintext), displayName, groupArg, createdByUserID, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return "", "", fmt.Errorf("api: storing enrollment token: %w", err)
	}
	return id, plaintext, nil
}

func (s *Store) ListEnrollTokens(ctx context.Context) ([]*RuntimeEnrollToken, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, display_name, group_name, created_at, used_by_runtime_id, revoked_at FROM runtime_enroll_tokens ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("api: listing enrollment tokens: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]*RuntimeEnrollToken, 0)
	for rows.Next() {
		t, err := scanEnrollToken(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) GetEnrollToken(ctx context.Context, id string) (*RuntimeEnrollToken, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, display_name, group_name, created_at, used_by_runtime_id, revoked_at FROM runtime_enroll_tokens WHERE id = ?`, id)
	return scanEnrollToken(row)
}

// RevokeEnrollToken marks a token revoked; already-enrolled devices that
// used it keep their fleet membership (revoking a token only blocks future
// re-registration attempts presenting it).
func (s *Store) RevokeEnrollToken(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE runtime_enroll_tokens SET revoked_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339), id)
	return err
}

func scanEnrollToken(row rowScanner) (*RuntimeEnrollToken, error) {
	var t RuntimeEnrollToken
	var group, usedBy, revokedAt sql.NullString
	var createdAt string
	if err := row.Scan(&t.ID, &t.DisplayName, &group, &createdAt, &usedBy, &revokedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning enrollment token: %w", err)
	}
	t.Group = group.String
	t.UsedByRuntimeID = usedBy.String
	ct, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("api: parsing created_at: %w", err)
	}
	t.CreatedAt = ct
	if revokedAt.Valid {
		rt, err := time.Parse(time.RFC3339, revokedAt.String)
		if err != nil {
			return nil, fmt.Errorf("api: parsing revoked_at: %w", err)
		}
		t.revokedAt = &rt
		t.Revoked = true
	}
	return &t, nil
}

// --- Store: devices (registry.DeviceStore, via an adapter) ---

// Authenticate validates enrollmentToken (if non-empty) and upserts a
// devices row for runtimeID (Increment 9, EDGE-120/ARC-210). Behavior:
//
//   - enrollmentToken == "" and runtimeID has never been enrolled: accepted
//     (the walking-skeleton no-token path), device recorded unenrolled.
//   - enrollmentToken == "" but runtimeID WAS previously enrolled with a
//     token: rejected — a per-device credential, once established, is
//     required on every subsequent Register call.
//   - enrollmentToken is set: must hash-match a non-revoked token. If
//     runtimeID is new, it's enrolled into that token's group. If
//     runtimeID already exists, the token must be the SAME one it enrolled
//     with (or it's a hijack attempt) — otherwise rejected.
func (s *Store) Authenticate(ctx context.Context, runtimeID, kind, enrollmentToken string) error {
	existing, err := s.deviceRow(ctx, runtimeID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	exists := err == nil

	if enrollmentToken == "" {
		if exists && existing.enrollTokenID != "" {
			return fmt.Errorf("runtime %q was enrolled with a token; it must be presented on every registration", runtimeID)
		}
		if !exists {
			_, err := s.db.ExecContext(ctx,
				`INSERT INTO devices (runtime_id, kind, enrolled_at) VALUES (?, ?, ?)`,
				runtimeID, kind, time.Now().UTC().Format(time.RFC3339))
			return err
		}
		return nil
	}

	tok, err := s.findEnrollTokenByHash(ctx, hashEnrollToken(enrollmentToken))
	if err != nil {
		return fmt.Errorf("invalid enrollment token")
	}
	if tok.revoked {
		return fmt.Errorf("enrollment token has been revoked")
	}

	if exists {
		if existing.enrollTokenID != tok.id {
			return fmt.Errorf("runtime %q is already enrolled with a different token", runtimeID)
		}
		return nil
	}

	var groupArg any
	if tok.group != "" {
		groupArg = tok.group
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO devices (runtime_id, kind, group_name, enroll_token_id, enrolled_at) VALUES (?, ?, ?, ?, ?)`,
		runtimeID, kind, groupArg, tok.id, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("api: enrolling device: %w", err)
	}
	_, _ = s.db.ExecContext(ctx, `UPDATE runtime_enroll_tokens SET used_by_runtime_id = ? WHERE id = ?`, runtimeID, tok.id)
	return nil
}

// GroupOf returns runtimeID's currently assigned fleet group, "" if
// unassigned or never enrolled.
func (s *Store) GroupOf(ctx context.Context, runtimeID string) (string, error) {
	row, err := s.deviceRow(ctx, runtimeID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return row.groupName, nil
}

// DeviceInfo returns runtimeID's admin-configured fleet metadata for the
// GET /runtimes inventory view; a runtime that was never registered with
// any device row (shouldn't normally happen once Authenticate is wired in
// on every Register) reports zero-value/unenrolled.
func (s *Store) DeviceInfo(ctx context.Context, runtimeID string) (DeviceInfo, error) {
	row, err := s.deviceRow(ctx, runtimeID)
	if errors.Is(err, sql.ErrNoRows) {
		return DeviceInfo{}, nil
	}
	if err != nil {
		return DeviceInfo{}, err
	}
	return DeviceInfo{Kind: row.kind, DisplayName: row.displayName, GroupName: row.groupName, Enrolled: row.enrollTokenID != ""}, nil
}

// AssignRuntimeGroup renames a device's display name and/or (re)assigns
// its fleet group (System Admin operation, EDGE-120/UI-220). group == ""
// unassigns.
func (s *Store) AssignRuntimeGroup(ctx context.Context, runtimeID, displayName string, group *string) error {
	if _, err := s.deviceRow(ctx, runtimeID); errors.Is(err, sql.ErrNoRows) {
		// A runtime the control plane has seen (it's in the live registry)
		// but that has no device row yet (pre-Increment-9 or never
		// enrolled) gets one created on first fleet-management touch.
		if _, err := s.db.ExecContext(ctx, `INSERT INTO devices (runtime_id, kind, enrolled_at) VALUES (?, 'server', ?)`, runtimeID, time.Now().UTC().Format(time.RFC3339)); err != nil {
			return fmt.Errorf("api: creating device record: %w", err)
		}
	} else if err != nil {
		return err
	}

	if displayName != "" {
		if _, err := s.db.ExecContext(ctx, `UPDATE devices SET display_name = ? WHERE runtime_id = ?`, displayName, runtimeID); err != nil {
			return fmt.Errorf("api: renaming device: %w", err)
		}
	}
	if group != nil {
		var groupArg any
		if *group != "" {
			groupArg = *group
		}
		if _, err := s.db.ExecContext(ctx, `UPDATE devices SET group_name = ? WHERE runtime_id = ?`, groupArg, runtimeID); err != nil {
			return fmt.Errorf("api: assigning device group: %w", err)
		}
	}
	return nil
}

type deviceRowResult struct {
	kind, displayName, groupName, enrollTokenID string
}

func (s *Store) deviceRow(ctx context.Context, runtimeID string) (deviceRowResult, error) {
	var d deviceRowResult
	var group, tokenID sql.NullString
	row := s.db.QueryRowContext(ctx, `SELECT kind, display_name, group_name, enroll_token_id FROM devices WHERE runtime_id = ?`, runtimeID)
	if err := row.Scan(&d.kind, &d.displayName, &group, &tokenID); err != nil {
		return deviceRowResult{}, err
	}
	d.groupName = group.String
	d.enrollTokenID = tokenID.String
	return d, nil
}

type enrollTokenRow struct {
	id, group string
	revoked   bool
}

func (s *Store) findEnrollTokenByHash(ctx context.Context, hash string) (enrollTokenRow, error) {
	var row enrollTokenRow
	var group sql.NullString
	var revokedAt sql.NullString
	r := s.db.QueryRowContext(ctx, `SELECT id, group_name, revoked_at FROM runtime_enroll_tokens WHERE token_hash = ?`, hash)
	if err := r.Scan(&row.id, &group, &revokedAt); err != nil {
		return enrollTokenRow{}, err
	}
	row.group = group.String
	row.revoked = revokedAt.Valid
	return row, nil
}

// --- HTTP handlers ---

func (h *Handlers) listRuntimeGroups(w http.ResponseWriter, r *http.Request) {
	if _, ok := currentUser(w, r); !ok {
		return
	}
	groups, err := h.store.ListRuntimeGroups(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, groups)
}

func (h *Handlers) createRuntimeGroup(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	var req struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	if err := readJSON(r, &req); err != nil || req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := h.store.CreateRuntimeGroup(r.Context(), req.Name, req.Description); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.audit(r, user.ID, "runtimegroup.create", "runtime_group", req.Name, "", nil, req)
	writeJSON(w, http.StatusCreated, &RuntimeGroup{Name: req.Name, Description: req.Description, CreatedAt: time.Now().UTC()})
}

func (h *Handlers) deleteRuntimeGroup(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	name := r.PathValue("name")
	if err := h.store.DeleteRuntimeGroup(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "runtimegroup.delete", "runtime_group", name, "", nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) listEnrollTokens(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	tokens, err := h.store.ListEnrollTokens(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, tokens)
}

func (h *Handlers) createEnrollToken(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	var req struct {
		DisplayName string `json:"displayName"`
		Group       string `json:"group"`
	}
	_ = readJSON(r, &req) // both fields optional

	id, plaintext, err := h.store.CreateEnrollToken(r.Context(), req.DisplayName, req.Group, user.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "enrolltoken.create", "runtime_enroll_token", id, "", nil, map[string]string{"displayName": req.DisplayName, "group": req.Group})
	writeJSON(w, http.StatusCreated, map[string]any{
		"id": id, "displayName": req.DisplayName, "group": req.Group,
		"createdAt": time.Now().UTC(), "usedByRuntimeId": "", "revoked": false,
		"token": plaintext,
	})
}

func (h *Handlers) deleteEnrollToken(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	id := r.PathValue("tokenId")
	if err := h.store.RevokeEnrollToken(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "enrolltoken.revoke", "runtime_enroll_token", id, "", nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) updateRuntime(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	runtimeID := r.PathValue("runtimeId")
	var req struct {
		DisplayName string  `json:"displayName"`
		Group       *string `json:"group"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if err := h.store.AssignRuntimeGroup(r.Context(), runtimeID, req.DisplayName, req.Group); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "runtime.update", "runtime", runtimeID, "", nil, req)

	for _, rt := range h.runtimes.ListRuntimes(r.Context()) {
		if rt.RuntimeID == runtimeID {
			writeJSON(w, http.StatusOK, rt)
			return
		}
	}
	writeError(w, http.StatusNotFound, "unknown runtime")
}
