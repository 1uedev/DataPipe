package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/engine/flow"
)

// Flow holds a project's flow metadata and current editable draft; deploys
// snapshot the draft into an immutable FlowVersion (VCS-110).
type Flow struct {
	ID              string          `json:"id"`
	ProjectID       string          `json:"projectId"`
	Name            string          `json:"name"`
	Content         json.RawMessage `json:"content"`
	DeployedVersion *int64          `json:"deployedVersion"`
	// LogLevel is OBS-120's per-flow log level ("debug"|"info"|"warn"|
	// "error"), deliberately outside the versioned flow content — see
	// SetFlowLogLevel.
	LogLevel  string    `json:"logLevel"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// FlowVersion is immutable once created (VCS-110): "every deploy creates an
// immutable version with author, timestamp, comment".
type FlowVersion struct {
	FlowID     string          `json:"flowId"`
	Version    int64           `json:"version"`
	Content    json.RawMessage `json:"content"`
	Author     string          `json:"author"`
	Comment    string          `json:"comment"`
	CreatedAt  time.Time       `json:"createdAt"`
	DeployedAt *time.Time      `json:"deployedAt"`
}

func (s *Store) CreateFlow(ctx context.Context, projectID, name string, content json.RawMessage) (*Flow, error) {
	now := time.Now().UTC()
	f := &Flow{ID: uuid.NewString(), ProjectID: projectID, Name: name, Content: content, LogLevel: "info", CreatedAt: now, UpdatedAt: now}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO flows (id, project_id, name, draft_content, deployed_version, created_at, updated_at) VALUES (?, ?, ?, ?, NULL, ?, ?)`,
		f.ID, f.ProjectID, f.Name, string(f.Content), now.Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("api: creating flow: %w", err)
	}
	return f, nil
}

func (s *Store) GetFlow(ctx context.Context, id string) (*Flow, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, project_id, name, draft_content, deployed_version, log_level, created_at, updated_at FROM flows WHERE id = ?`, id)
	return scanFlow(row)
}

func (s *Store) ListFlows(ctx context.Context, projectID string) ([]*Flow, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, name, draft_content, deployed_version, log_level, created_at, updated_at FROM flows WHERE project_id = ? ORDER BY name`, projectID)
	if err != nil {
		return nil, fmt.Errorf("api: listing flows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	flows := make([]*Flow, 0)
	for rows.Next() {
		f, err := scanFlow(rows)
		if err != nil {
			return nil, err
		}
		flows = append(flows, f)
	}
	return flows, rows.Err()
}

func (s *Store) UpdateFlowDraft(ctx context.Context, id string, name *string, content json.RawMessage) (*Flow, error) {
	f, err := s.GetFlow(ctx, id)
	if err != nil {
		return nil, err
	}
	if name != nil {
		f.Name = *name
	}
	if content != nil {
		f.Content = content
	}
	f.UpdatedAt = time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `UPDATE flows SET name = ?, draft_content = ?, updated_at = ? WHERE id = ?`,
		f.Name, string(f.Content), f.UpdatedAt.Format(time.RFC3339), f.ID)
	if err != nil {
		return nil, fmt.Errorf("api: updating flow draft: %w", err)
	}
	return f, nil
}

func (s *Store) DeleteFlow(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM flows WHERE id = ?`, id)
	return err
}

// SetFlowLogLevel persists OBS-120's per-flow log level, deliberately
// outside flow content/versioning — callers are responsible for re-pushing
// the flow's current deployed content (unchanged) with the new level so a
// connected runtime picks it up without a real redeploy.
func (s *Store) SetFlowLogLevel(ctx context.Context, id, level string) error {
	res, err := s.db.ExecContext(ctx, `UPDATE flows SET log_level = ? WHERE id = ?`, level, id)
	if err != nil {
		return fmt.Errorf("api: setting flow log level: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("api: setting flow log level: %w", err)
	}
	if n == 0 {
		return sql.ErrNoRows
	}
	return nil
}

// DeployedFlow is one currently-deployed flow's canonical content, for
// pushing to a runtime that just (re)registered (ERR-150: "runtime restart
// restores all deployed flows... automatically").
type DeployedFlow struct {
	FlowID           string
	Version          int64
	ContentJSON      string
	DefaultErrorFlow string
	TargetGroup      string
	LogLevel         string
}

// ListDeployedFlows returns every flow that currently has a deployed
// version, with its deployed content, owning project's ERR-120 default
// error-handler flow id, OBS-120 log level, and UI-220 runtime-group
// assignment (parsed out of the content itself — runtimeAssignment isn't a
// separate column).
func (s *Store) ListDeployedFlows(ctx context.Context) ([]DeployedFlow, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT f.id, fv.version, fv.content, p.default_error_flow, f.log_level
		FROM flows f
		JOIN flow_versions fv ON fv.flow_id = f.id AND fv.version = f.deployed_version
		JOIN projects p ON p.id = f.project_id
		WHERE f.deployed_version IS NOT NULL`)
	if err != nil {
		return nil, fmt.Errorf("api: listing deployed flows: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []DeployedFlow
	for rows.Next() {
		var d DeployedFlow
		var content string
		var defaultErrorFlow sql.NullString
		if err := rows.Scan(&d.FlowID, &d.Version, &content, &defaultErrorFlow, &d.LogLevel); err != nil {
			return nil, fmt.Errorf("api: scanning deployed flow: %w", err)
		}
		d.ContentJSON = content
		if defaultErrorFlow.Valid {
			d.DefaultErrorFlow = defaultErrorFlow.String
		}
		if ff, err := flow.Parse([]byte(content)); err == nil && ff.RuntimeAssignment != nil {
			d.TargetGroup = ff.RuntimeAssignment.Group
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// CreateDeployedVersion snapshots flow's current draft as the next
// immutable version (VCS-110) and marks it deployed, atomically.
func (s *Store) CreateDeployedVersion(ctx context.Context, flowID, author, comment string) (*FlowVersion, error) {
	f, err := s.GetFlow(ctx, flowID)
	if err != nil {
		return nil, err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("api: begin deploy tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var maxVersion sql.NullInt64
	row := tx.QueryRowContext(ctx, s.db.Rebind(`SELECT MAX(version) FROM flow_versions WHERE flow_id = ?`), flowID)
	if err := row.Scan(&maxVersion); err != nil {
		return nil, fmt.Errorf("api: finding next version: %w", err)
	}
	nextVersion := int64(1)
	if maxVersion.Valid {
		nextVersion = maxVersion.Int64 + 1
	}

	now := time.Now().UTC()
	v := &FlowVersion{FlowID: flowID, Version: nextVersion, Content: f.Content, Author: author, Comment: comment, CreatedAt: now, DeployedAt: &now}

	_, err = tx.ExecContext(ctx, s.db.Rebind(
		`INSERT INTO flow_versions (flow_id, version, content, author, comment, created_at, deployed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`),
		v.FlowID, v.Version, string(v.Content), v.Author, v.Comment, now.Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("api: inserting flow version: %w", err)
	}
	_, err = tx.ExecContext(ctx, s.db.Rebind(`UPDATE flows SET deployed_version = ?, updated_at = ? WHERE id = ?`), v.Version, now.Format(time.RFC3339), flowID)
	if err != nil {
		return nil, fmt.Errorf("api: updating flow deployed_version: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("api: committing deploy: %w", err)
	}
	return v, nil
}

func (s *Store) ListFlowVersions(ctx context.Context, flowID string) ([]*FlowVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT flow_id, version, content, author, comment, created_at, deployed_at FROM flow_versions WHERE flow_id = ? ORDER BY version DESC`, flowID)
	if err != nil {
		return nil, fmt.Errorf("api: listing flow versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	versions := make([]*FlowVersion, 0)
	for rows.Next() {
		v, err := scanFlowVersion(rows)
		if err != nil {
			return nil, err
		}
		versions = append(versions, v)
	}
	return versions, rows.Err()
}

func (s *Store) GetFlowVersion(ctx context.Context, flowID string, version int64) (*FlowVersion, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT flow_id, version, content, author, comment, created_at, deployed_at FROM flow_versions WHERE flow_id = ? AND version = ?`, flowID, version)
	return scanFlowVersion(row)
}

func scanFlow(row rowScanner) (*Flow, error) {
	var f Flow
	var content, createdAt, updatedAt string
	var deployedVersion sql.NullInt64
	if err := row.Scan(&f.ID, &f.ProjectID, &f.Name, &content, &deployedVersion, &f.LogLevel, &createdAt, &updatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning flow: %w", err)
	}
	f.Content = json.RawMessage(content)
	if deployedVersion.Valid {
		f.DeployedVersion = &deployedVersion.Int64
	}
	var err error
	if f.CreatedAt, err = time.Parse(time.RFC3339, createdAt); err != nil {
		return nil, fmt.Errorf("api: parsing created_at: %w", err)
	}
	if f.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt); err != nil {
		return nil, fmt.Errorf("api: parsing updated_at: %w", err)
	}
	return &f, nil
}

func scanFlowVersion(row rowScanner) (*FlowVersion, error) {
	var v FlowVersion
	var content, createdAt string
	var deployedAt sql.NullString
	if err := row.Scan(&v.FlowID, &v.Version, &content, &v.Author, &v.Comment, &createdAt, &deployedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning flow version: %w", err)
	}
	v.Content = json.RawMessage(content)
	var err error
	if v.CreatedAt, err = time.Parse(time.RFC3339, createdAt); err != nil {
		return nil, fmt.Errorf("api: parsing created_at: %w", err)
	}
	if deployedAt.Valid {
		t, err := time.Parse(time.RFC3339, deployedAt.String)
		if err != nil {
			return nil, fmt.Errorf("api: parsing deployed_at: %w", err)
		}
		v.DeployedAt = &t
	}
	return &v, nil
}

// --- HTTP handlers ---

func (h *Handlers) listFlows(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleViewer) {
		return
	}
	flows, err := h.store.ListFlows(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, flows)
}

func (h *Handlers) createFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleEditor) {
		return
	}
	var req struct {
		Name    string          `json:"name"`
		Content json.RawMessage `json:"content"`
	}
	if err := readJSON(r, &req); err != nil || req.Name == "" || len(req.Content) == 0 {
		writeError(w, http.StatusBadRequest, "name and content are required")
		return
	}
	f, err := h.store.CreateFlow(r.Context(), projectID, req.Name, req.Content)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "flow.create", "flow", f.ID, projectID, nil, f)
	writeJSON(w, http.StatusCreated, f)
}

// flowAndAuthorize loads the flow and enforces min role on its project;
// writes the response and returns ok=false on any failure.
func (h *Handlers) flowAndAuthorize(w http.ResponseWriter, r *http.Request, user *auth.User, min auth.ProjectRole) (*Flow, bool) {
	f, err := h.store.GetFlow(r.Context(), r.PathValue("flowId"))
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return nil, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if !requireProjectRole(w, r, h.authStore, user, f.ProjectID, min) {
		return nil, false
	}
	return f, true
}

func (h *Handlers) getFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, f)
}

func (h *Handlers) updateFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	before, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	var req struct {
		Name    *string         `json:"name"`
		Content json.RawMessage `json:"content"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	after, err := h.store.UpdateFlowDraft(r.Context(), before.ID, req.Name, req.Content)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "flow.update", "flow", before.ID, before.ProjectID, before, after)
	writeJSON(w, http.StatusOK, after)
}

func (h *Handlers) deleteFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	if err := h.store.DeleteFlow(r.Context(), f.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "flow.delete", "flow", f.ID, f.ProjectID, f, nil)
	w.WriteHeader(http.StatusNoContent)
}

var validLogLevels = map[string]bool{"debug": true, "info": true, "warn": true, "error": true}

// setFlowLogLevel implements OBS-120's "per-flow log level at runtime
// without redeploy": it persists the new level (outside flow content/
// versioning, so it never bumps the flow's version) and, if the flow is
// currently deployed, re-pushes its unchanged deployed content with the new
// level so a connected runtime picks it up immediately — ENG-140's
// fingerprint-based reconciliation means this restarts no node.
func (h *Handlers) setFlowLogLevel(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}

	var req struct {
		Level string `json:"level"`
	}
	if err := readJSON(r, &req); err != nil || !validLogLevels[req.Level] {
		writeError(w, http.StatusBadRequest, "level must be one of debug, info, warn, error")
		return
	}

	if err := h.store.SetFlowLogLevel(r.Context(), f.ID, req.Level); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	f.LogLevel = req.Level

	if f.DeployedVersion != nil {
		deployed, err := h.store.GetFlowVersion(r.Context(), f.ID, *f.DeployedVersion)
		if err == nil {
			ff, perr := flow.Parse(deployed.Content)
			if perr == nil {
				project, perr2 := h.store.GetProject(r.Context(), f.ProjectID)
				if perr2 == nil {
					defaultErrorFlow := ""
					if project.DefaultErrorFlow != nil {
						defaultErrorFlow = *project.DefaultErrorFlow
					}
					targetGroup := ""
					if ff.RuntimeAssignment != nil {
						targetGroup = ff.RuntimeAssignment.Group
					}
					// Best-effort: a runtime being briefly unreachable
					// shouldn't block persisting the setting — it's picked
					// up on the runtime's next reconnect either way
					// (DeployStream re-push already includes LogLevel).
					_ = h.deployer.DeployFlow(r.Context(), f.ID, *f.DeployedVersion, string(deployed.Content), defaultErrorFlow, targetGroup, req.Level)
				}
			}
		}
	}

	h.audit(r, user.ID, "flow.setLogLevel", "flow", f.ID, f.ProjectID, nil, map[string]string{"level": req.Level})
	writeJSON(w, http.StatusOK, f)
}

// deployFlow implements VCS-110 + ARC-110: validate the current draft
// against the same rules the runtime enforces (engine/flow.Validate),
// snapshot it as a new immutable version, and push it to the assigned
// runtime.
func (h *Handlers) deployFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}

	ff, err := flow.Parse(f.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid flow file: "+err.Error())
		return
	}
	if err := flow.Validate(ff); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	var req struct {
		Comment string `json:"comment"`
	}
	_ = readJSON(r, &req) // body is optional

	version, err := h.store.CreateDeployedVersion(r.Context(), f.ID, user.Username, req.Comment)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	canonical, err := ff.MarshalCanonical()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	project, err := h.store.GetProject(r.Context(), f.ProjectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	defaultErrorFlow := ""
	if project.DefaultErrorFlow != nil {
		defaultErrorFlow = *project.DefaultErrorFlow
	}
	targetGroup := ""
	if ff.RuntimeAssignment != nil {
		targetGroup = ff.RuntimeAssignment.Group
	}

	if err := h.deployer.DeployFlow(r.Context(), f.ID, version.Version, string(canonical), defaultErrorFlow, targetGroup, f.LogLevel); err != nil {
		writeError(w, http.StatusConflict, "no runtime available to deploy to: "+err.Error())
		return
	}

	h.audit(r, user.ID, "flow.deploy", "flow", f.ID, f.ProjectID, nil, version)
	writeJSON(w, http.StatusCreated, version)
}

func (h *Handlers) listFlowVersions(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	versions, err := h.store.ListFlowVersions(r.Context(), f.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, versions)
}

func (h *Handlers) getFlowVersion(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	version, err := parseVersionParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid version")
		return
	}
	v, err := h.store.GetFlowVersion(r.Context(), f.ID, version)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, v)
}

// rollbackFlow copies an old version's content into the draft and
// redeploys it as a brand-new version — "one-click rollback (as a new
// version)" (VCS-110): history is never rewritten.
func (h *Handlers) rollbackFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	version, err := parseVersionParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid version")
		return
	}
	old, err := h.store.GetFlowVersion(r.Context(), f.ID, version)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if _, err := h.store.UpdateFlowDraft(r.Context(), f.ID, nil, old.Content); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}

	ff, err := flow.Parse(old.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid flow file: "+err.Error())
		return
	}
	if err := flow.Validate(ff); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	newVersion, err := h.store.CreateDeployedVersion(r.Context(), f.ID, user.Username, fmt.Sprintf("rollback to version %d", version))
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	canonical, err := ff.MarshalCanonical()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	project, err := h.store.GetProject(r.Context(), f.ProjectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	defaultErrorFlow := ""
	if project.DefaultErrorFlow != nil {
		defaultErrorFlow = *project.DefaultErrorFlow
	}
	targetGroup := ""
	if ff.RuntimeAssignment != nil {
		targetGroup = ff.RuntimeAssignment.Group
	}

	if err := h.deployer.DeployFlow(r.Context(), f.ID, newVersion.Version, string(canonical), defaultErrorFlow, targetGroup, f.LogLevel); err != nil {
		writeError(w, http.StatusConflict, "no runtime available to deploy to: "+err.Error())
		return
	}

	h.audit(r, user.ID, "flow.rollback", "flow", f.ID, f.ProjectID, old, newVersion)
	writeJSON(w, http.StatusCreated, newVersion)
}

func parseVersionParam(r *http.Request) (int64, error) {
	return strconv.ParseInt(r.PathValue("version"), 10, 64)
}
