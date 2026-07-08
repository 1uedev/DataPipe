// backup.go implements OBS-150's consistent export/restore of all
// configuration: users, projects, flows + versions, connections, and
// credentials (already envelope-encrypted at rest under SEC-120 — a backup
// bundle carries only the sealed ciphertext/wrapped-DEK fields, never a
// decrypted value), plus the fleet and alerting admin state that lives in
// the same database. Deliberately excluded as NOT "configuration":
// sessions (a restore always forces re-login — see RestoreBackup), the
// audit log, and operational/debug history (executions, execution node
// I/O, dead letters, debug pins, fired alert instances) — restoring those
// would either be meaningless (rows referencing flow/rule ids that may no
// longer exist) or actively misleading (replaying history that never
// happened on this instance). Scheduled backups (OBS-150's SHOULD/P2 half)
// are not implemented — see TODO.md.
package api

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"time"
)

// Bundle is the full exported/restorable configuration snapshot.
type Bundle struct {
	FormatVersion int       `json:"formatVersion"`
	ExportedAt    time.Time `json:"exportedAt"`

	Users               []backupUser          `json:"users"`
	Projects            []backupProject       `json:"projects"`
	ProjectMembers      []backupProjectMember `json:"projectMembers"`
	Flows               []backupFlow          `json:"flows"`
	FlowVersions        []backupFlowVersion   `json:"flowVersions"`
	Connections         []backupConnection    `json:"connections"`
	Credentials         []backupCredential    `json:"credentials"`
	RuntimeGroups       []backupRuntimeGroup  `json:"runtimeGroups"`
	RuntimeEnrollTokens []backupEnrollToken   `json:"runtimeEnrollTokens"`
	Devices             []backupDevice        `json:"devices"`
	AlertRules          []backupAlertRule     `json:"alertRules"`
}

const backupFormatVersion = 1

type backupUser struct {
	ID           string `json:"id"`
	Username     string `json:"username"`
	PasswordHash string `json:"passwordHash"`
	SystemRole   string `json:"systemRole"`
	CreatedAt    string `json:"createdAt"`
}

type backupProject struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	Description      string  `json:"description"`
	CreatedAt        string  `json:"createdAt"`
	DefaultErrorFlow *string `json:"defaultErrorFlow,omitempty"`
}

type backupProjectMember struct {
	ProjectID string `json:"projectId"`
	UserID    string `json:"userId"`
	Role      string `json:"role"`
}

type backupFlow struct {
	ID              string `json:"id"`
	ProjectID       string `json:"projectId"`
	Name            string `json:"name"`
	DraftContent    string `json:"draftContent"`
	DeployedVersion *int64 `json:"deployedVersion,omitempty"`
	CreatedAt       string `json:"createdAt"`
	UpdatedAt       string `json:"updatedAt"`
	LogLevel        string `json:"logLevel"`
}

type backupFlowVersion struct {
	FlowID     string  `json:"flowId"`
	Version    int64   `json:"version"`
	Content    string  `json:"content"`
	Author     string  `json:"author"`
	Comment    string  `json:"comment"`
	CreatedAt  string  `json:"createdAt"`
	DeployedAt *string `json:"deployedAt,omitempty"`
}

type backupConnection struct {
	ID           string  `json:"id"`
	ProjectID    string  `json:"projectId"`
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	Config       string  `json:"config"`
	CredentialID *string `json:"credentialId,omitempty"`
}

// backupCredential carries only the already-sealed (SEC-120 envelope
// encryption) fields — never a decrypted value. Restoring it onto a
// control plane running under a DIFFERENT DATAPIPE_MASTER_KEY than the one
// that sealed it will make that credential permanently unreadable (Open
// fails); the restore procedure doc calls this out.
type backupCredential struct {
	ID              string `json:"id"`
	ProjectID       string `json:"projectId"`
	Name            string `json:"name"`
	KeyVersion      int    `json:"keyVersion"`
	WrappedDEK      string `json:"wrappedDek"`
	WrappedDEKNonce string `json:"wrappedDekNonce"`
	Ciphertext      string `json:"ciphertext"`
	Nonce           string `json:"nonce"`
	CreatedAt       string `json:"createdAt"`
}

type backupRuntimeGroup struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	CreatedAt   string `json:"createdAt"`
}

type backupEnrollToken struct {
	ID              string  `json:"id"`
	TokenHash       string  `json:"tokenHash"`
	DisplayName     string  `json:"displayName"`
	GroupName       *string `json:"groupName,omitempty"`
	CreatedBy       string  `json:"createdBy"`
	CreatedAt       string  `json:"createdAt"`
	UsedByRuntimeID *string `json:"usedByRuntimeId,omitempty"`
	RevokedAt       *string `json:"revokedAt,omitempty"`
}

type backupDevice struct {
	RuntimeID     string  `json:"runtimeId"`
	Kind          string  `json:"kind"`
	DisplayName   string  `json:"displayName"`
	GroupName     *string `json:"groupName,omitempty"`
	EnrollTokenID *string `json:"enrollTokenId,omitempty"`
	EnrolledAt    string  `json:"enrolledAt"`
}

type backupAlertRule struct {
	ID              string `json:"id"`
	Name            string `json:"name"`
	Metric          string `json:"metric"`
	TargetRuntimeID string `json:"targetRuntimeId"`
	WebhookURL      string `json:"webhookUrl"`
	Enabled         bool   `json:"enabled"`
	CreatedAt       string `json:"createdAt"`
}

// ExportBackup snapshots every configuration table into one Bundle. It
// runs inside a single read-only transaction so the snapshot is
// consistent even if writes are happening concurrently.
func (s *Store) ExportBackup(ctx context.Context) (*Bundle, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("api: beginning export transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	b := &Bundle{FormatVersion: backupFormatVersion, ExportedAt: time.Now().UTC()}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, username, password_hash, system_role, created_at FROM users ORDER BY id`), &b.Users, func(rows *sql.Rows, v *backupUser) error {
		return rows.Scan(&v.ID, &v.Username, &v.PasswordHash, &v.SystemRole, &v.CreatedAt)
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, name, description, created_at, default_error_flow FROM projects ORDER BY id`), &b.Projects, func(rows *sql.Rows, v *backupProject) error {
		var defErr sql.NullString
		if err := rows.Scan(&v.ID, &v.Name, &v.Description, &v.CreatedAt, &defErr); err != nil {
			return err
		}
		if defErr.Valid {
			v.DefaultErrorFlow = &defErr.String
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT project_id, user_id, role FROM project_members ORDER BY project_id, user_id`), &b.ProjectMembers, func(rows *sql.Rows, v *backupProjectMember) error {
		return rows.Scan(&v.ProjectID, &v.UserID, &v.Role)
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, project_id, name, draft_content, deployed_version, created_at, updated_at, log_level FROM flows ORDER BY id`), &b.Flows, func(rows *sql.Rows, v *backupFlow) error {
		var deployedVersion sql.NullInt64
		if err := rows.Scan(&v.ID, &v.ProjectID, &v.Name, &v.DraftContent, &deployedVersion, &v.CreatedAt, &v.UpdatedAt, &v.LogLevel); err != nil {
			return err
		}
		if deployedVersion.Valid {
			v.DeployedVersion = &deployedVersion.Int64
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT flow_id, version, content, author, comment, created_at, deployed_at FROM flow_versions ORDER BY flow_id, version`), &b.FlowVersions, func(rows *sql.Rows, v *backupFlowVersion) error {
		var deployedAt sql.NullString
		if err := rows.Scan(&v.FlowID, &v.Version, &v.Content, &v.Author, &v.Comment, &v.CreatedAt, &deployedAt); err != nil {
			return err
		}
		if deployedAt.Valid {
			v.DeployedAt = &deployedAt.String
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, project_id, name, type, config, credential_id FROM connections ORDER BY id`), &b.Connections, func(rows *sql.Rows, v *backupConnection) error {
		var credentialID sql.NullString
		if err := rows.Scan(&v.ID, &v.ProjectID, &v.Name, &v.Type, &v.Config, &credentialID); err != nil {
			return err
		}
		if credentialID.Valid {
			v.CredentialID = &credentialID.String
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, project_id, name, key_version, wrapped_dek, wrapped_dek_nonce, ciphertext, nonce, created_at FROM credentials ORDER BY id`), &b.Credentials, func(rows *sql.Rows, v *backupCredential) error {
		return rows.Scan(&v.ID, &v.ProjectID, &v.Name, &v.KeyVersion, &v.WrappedDEK, &v.WrappedDEKNonce, &v.Ciphertext, &v.Nonce, &v.CreatedAt)
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT name, description, created_at FROM runtime_groups ORDER BY name`), &b.RuntimeGroups, func(rows *sql.Rows, v *backupRuntimeGroup) error {
		return rows.Scan(&v.Name, &v.Description, &v.CreatedAt)
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, token_hash, display_name, group_name, created_by, created_at, used_by_runtime_id, revoked_at FROM runtime_enroll_tokens ORDER BY id`), &b.RuntimeEnrollTokens, func(rows *sql.Rows, v *backupEnrollToken) error {
		var groupName, usedBy, revokedAt sql.NullString
		if err := rows.Scan(&v.ID, &v.TokenHash, &v.DisplayName, &groupName, &v.CreatedBy, &v.CreatedAt, &usedBy, &revokedAt); err != nil {
			return err
		}
		if groupName.Valid {
			v.GroupName = &groupName.String
		}
		if usedBy.Valid {
			v.UsedByRuntimeID = &usedBy.String
		}
		if revokedAt.Valid {
			v.RevokedAt = &revokedAt.String
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT runtime_id, kind, display_name, group_name, enroll_token_id, enrolled_at FROM devices ORDER BY runtime_id`), &b.Devices, func(rows *sql.Rows, v *backupDevice) error {
		var groupName, enrollTokenID sql.NullString
		if err := rows.Scan(&v.RuntimeID, &v.Kind, &v.DisplayName, &groupName, &enrollTokenID, &v.EnrolledAt); err != nil {
			return err
		}
		if groupName.Valid {
			v.GroupName = &groupName.String
		}
		if enrollTokenID.Valid {
			v.EnrollTokenID = &enrollTokenID.String
		}
		return nil
	}); err != nil {
		return nil, err
	}

	if err := queryAll(ctx, tx, s.db.Rebind(`SELECT id, name, metric, target_runtime_id, webhook_url, enabled, created_at FROM alert_rules ORDER BY id`), &b.AlertRules, func(rows *sql.Rows, v *backupAlertRule) error {
		var enabled int
		if err := rows.Scan(&v.ID, &v.Name, &v.Metric, &v.TargetRuntimeID, &v.WebhookURL, &enabled, &v.CreatedAt); err != nil {
			return err
		}
		v.Enabled = enabled != 0
		return nil
	}); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("api: committing export transaction: %w", err)
	}
	return b, nil
}

// queryAll runs query, scanning each row into a fresh *T via scan and
// appending it to *out — a small generic helper to keep ExportBackup's
// dozen near-identical table dumps free of repeated boilerplate.
func queryAll[T any](ctx context.Context, tx *sql.Tx, query string, out *[]T, scan func(*sql.Rows, *T) error) error {
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("api: querying backup data: %w", err)
	}
	defer func() { _ = rows.Close() }()
	result := []T{}
	for rows.Next() {
		var v T
		if err := scan(rows, &v); err != nil {
			return fmt.Errorf("api: scanning backup row: %w", err)
		}
		result = append(result, v)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("api: iterating backup rows: %w", err)
	}
	*out = result
	return nil
}

// RestoreBackup replaces the ENTIRE current configuration with b's
// contents, inside one transaction: every table Bundle covers is deleted
// and reloaded, along with the operational tables that reference them
// (executions/dead-letters/debug-pins/fired-alerts/sessions) since those
// would otherwise dangle against ids that may no longer exist post-
// restore — this is a full disaster-recovery restore, not a merge. Every
// existing session is deleted as a consequence of replacing the users
// table, so the caller (and everyone else) will need to log in again
// afterward.
func (s *Store) RestoreBackup(ctx context.Context, b *Bundle) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("api: beginning restore transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Children first, respecting every real foreign key in the schema.
	for _, table := range []string{
		"debug_pins", "execution_node_io", "executions", "dead_letters", "alerts",
		"project_members", "flow_versions", "flows", "connections", "credentials",
		"devices", "runtime_enroll_tokens", "runtime_groups", "alert_rules",
		"sessions", "projects", "users",
	} {
		if _, err := tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return fmt.Errorf("api: clearing %s for restore: %w", table, err)
		}
	}

	for _, u := range b.Users {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO users (id, username, password_hash, system_role, created_at) VALUES (?, ?, ?, ?, ?)`),
			u.ID, u.Username, u.PasswordHash, u.SystemRole, u.CreatedAt); err != nil {
			return fmt.Errorf("api: restoring user %s: %w", u.ID, err)
		}
	}
	for _, p := range b.Projects {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO projects (id, name, description, created_at, default_error_flow) VALUES (?, ?, ?, ?, ?)`),
			p.ID, p.Name, p.Description, p.CreatedAt, p.DefaultErrorFlow); err != nil {
			return fmt.Errorf("api: restoring project %s: %w", p.ID, err)
		}
	}
	for _, m := range b.ProjectMembers {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO project_members (project_id, user_id, role) VALUES (?, ?, ?)`),
			m.ProjectID, m.UserID, m.Role); err != nil {
			return fmt.Errorf("api: restoring project member %s/%s: %w", m.ProjectID, m.UserID, err)
		}
	}
	for _, f := range b.Flows {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO flows (id, project_id, name, draft_content, deployed_version, created_at, updated_at, log_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`),
			f.ID, f.ProjectID, f.Name, f.DraftContent, f.DeployedVersion, f.CreatedAt, f.UpdatedAt, f.LogLevel); err != nil {
			return fmt.Errorf("api: restoring flow %s: %w", f.ID, err)
		}
	}
	for _, v := range b.FlowVersions {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO flow_versions (flow_id, version, content, author, comment, created_at, deployed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`),
			v.FlowID, v.Version, v.Content, v.Author, v.Comment, v.CreatedAt, v.DeployedAt); err != nil {
			return fmt.Errorf("api: restoring flow version %s/%d: %w", v.FlowID, v.Version, err)
		}
	}
	for _, c := range b.Connections {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO connections (id, project_id, name, type, config, credential_id) VALUES (?, ?, ?, ?, ?, ?)`),
			c.ID, c.ProjectID, c.Name, c.Type, c.Config, c.CredentialID); err != nil {
			return fmt.Errorf("api: restoring connection %s: %w", c.ID, err)
		}
	}
	for _, c := range b.Credentials {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO credentials (id, project_id, name, key_version, wrapped_dek, wrapped_dek_nonce, ciphertext, nonce, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`),
			c.ID, c.ProjectID, c.Name, c.KeyVersion, c.WrappedDEK, c.WrappedDEKNonce, c.Ciphertext, c.Nonce, c.CreatedAt); err != nil {
			return fmt.Errorf("api: restoring credential %s: %w", c.ID, err)
		}
	}
	for _, g := range b.RuntimeGroups {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO runtime_groups (name, description, created_at) VALUES (?, ?, ?)`),
			g.Name, g.Description, g.CreatedAt); err != nil {
			return fmt.Errorf("api: restoring runtime group %s: %w", g.Name, err)
		}
	}
	for _, t := range b.RuntimeEnrollTokens {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO runtime_enroll_tokens (id, token_hash, display_name, group_name, created_by, created_at, used_by_runtime_id, revoked_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`),
			t.ID, t.TokenHash, t.DisplayName, t.GroupName, t.CreatedBy, t.CreatedAt, t.UsedByRuntimeID, t.RevokedAt); err != nil {
			return fmt.Errorf("api: restoring enroll token %s: %w", t.ID, err)
		}
	}
	for _, d := range b.Devices {
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO devices (runtime_id, kind, display_name, group_name, enroll_token_id, enrolled_at) VALUES (?, ?, ?, ?, ?, ?)`),
			d.RuntimeID, d.Kind, d.DisplayName, d.GroupName, d.EnrollTokenID, d.EnrolledAt); err != nil {
			return fmt.Errorf("api: restoring device %s: %w", d.RuntimeID, err)
		}
	}
	for _, r := range b.AlertRules {
		enabled := 0
		if r.Enabled {
			enabled = 1
		}
		if _, err := tx.ExecContext(ctx, s.db.Rebind(`INSERT INTO alert_rules (id, name, metric, target_runtime_id, webhook_url, enabled, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`),
			r.ID, r.Name, r.Metric, r.TargetRuntimeID, r.WebhookURL, enabled, r.CreatedAt); err != nil {
			return fmt.Errorf("api: restoring alert rule %s: %w", r.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("api: committing restore transaction: %w", err)
	}
	return nil
}

// --- HTTP handlers ---

func (h *Handlers) exportBackup(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	bundle, err := h.store.ExportBackup(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "backup.export", "backup", "", "", nil, nil)
	w.Header().Set("Content-Disposition", `attachment; filename="datapipe-backup.json"`)
	writeJSON(w, http.StatusOK, bundle)
}

func (h *Handlers) restoreBackup(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	var req struct {
		Confirm bool   `json:"confirm"`
		Bundle  Bundle `json:"bundle"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if !req.Confirm {
		writeError(w, http.StatusBadRequest, "restore replaces ALL current configuration; set \"confirm\": true to proceed")
		return
	}
	if err := h.store.RestoreBackup(r.Context(), &req.Bundle); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "backup.restore", "backup", "", "", nil, map[string]any{"exportedAt": req.Bundle.ExportedAt})
	writeJSON(w, http.StatusOK, map[string]bool{"restored": true})
}
