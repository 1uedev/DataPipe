// envprofiles.go implements VCS-140's environment profiles: named variable
// sets (dev/test/prod) resolved against a flow's declared env[]
// (Flow-File-Format.md §2 EnvVar, §5 profile shape) at deploy time, so the
// same flow content deploys unmodified against whichever profile is
// selected. Resolution itself (resolveEnv) is a pure function so
// deployFlow/rollbackFlow/setFlowLogLevel can all share the exact same
// missing-variable check. The full git-syncable profiles/<name>.json file
// layout Flow-File-Format.md §6 describes is VCS-120 territory (git
// integration, deferred per TODO.md) — this DB/REST-backed store realizes
// the same "name + values" shape (§5) without that file-sync machinery,
// consistent with how projects/flows/connections are already DB-backed
// rather than filesystem-backed everywhere else in this codebase.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/engine/flow"
)

// EnvironmentProfile is one named variable set (e.g. "dev", "prod") —
// Flow-File-Format.md §5's { "name", "values" } shape.
type EnvironmentProfile struct {
	ID        string            `json:"id"`
	ProjectID string            `json:"projectId"`
	Name      string            `json:"name"`
	Values    map[string]string `json:"values"`
	CreatedAt string            `json:"createdAt"`
}

func (s *Store) CreateEnvironmentProfile(ctx context.Context, projectID, name string, values map[string]string) (*EnvironmentProfile, error) {
	if values == nil {
		values = map[string]string{}
	}
	valuesJSON, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("api: encoding profile values: %w", err)
	}
	p := &EnvironmentProfile{ID: uuid.NewString(), ProjectID: projectID, Name: name, Values: values, CreatedAt: time.Now().UTC().Format(time.RFC3339)}
	_, err = s.db.ExecContext(ctx, `INSERT INTO environment_profiles (id, project_id, name, variables, created_at) VALUES (?, ?, ?, ?, ?)`,
		p.ID, p.ProjectID, p.Name, string(valuesJSON), p.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("api: creating environment profile: %w", err)
	}
	return p, nil
}

func (s *Store) ListEnvironmentProfiles(ctx context.Context, projectID string) ([]*EnvironmentProfile, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, name, variables, created_at FROM environment_profiles WHERE project_id = ? ORDER BY name`, projectID)
	if err != nil {
		return nil, fmt.Errorf("api: listing environment profiles: %w", err)
	}
	defer func() { _ = rows.Close() }()

	profiles := []*EnvironmentProfile{}
	for rows.Next() {
		p, err := scanEnvironmentProfile(rows)
		if err != nil {
			return nil, err
		}
		profiles = append(profiles, p)
	}
	return profiles, rows.Err()
}

func (s *Store) GetEnvironmentProfile(ctx context.Context, id string) (*EnvironmentProfile, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, project_id, name, variables, created_at FROM environment_profiles WHERE id = ?`, id)
	return scanEnvironmentProfile(row)
}

func (s *Store) UpdateEnvironmentProfileValues(ctx context.Context, id string, values map[string]string) (*EnvironmentProfile, error) {
	p, err := s.GetEnvironmentProfile(ctx, id)
	if err != nil {
		return nil, err
	}
	if values == nil {
		values = map[string]string{}
	}
	valuesJSON, err := json.Marshal(values)
	if err != nil {
		return nil, fmt.Errorf("api: encoding profile values: %w", err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE environment_profiles SET variables = ? WHERE id = ?`, string(valuesJSON), id); err != nil {
		return nil, fmt.Errorf("api: updating environment profile: %w", err)
	}
	p.Values = values
	return p, nil
}

func (s *Store) DeleteEnvironmentProfile(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM environment_profiles WHERE id = ?`, id)
	return err
}

func scanEnvironmentProfile(row rowScanner) (*EnvironmentProfile, error) {
	var p EnvironmentProfile
	var valuesJSON string
	if err := row.Scan(&p.ID, &p.ProjectID, &p.Name, &valuesJSON, &p.CreatedAt); err != nil {
		return nil, fmt.Errorf("api: scanning environment profile: %w", err)
	}
	if err := json.Unmarshal([]byte(valuesJSON), &p.Values); err != nil {
		return nil, fmt.Errorf("api: decoding profile values: %w", err)
	}
	return &p, nil
}

// SetFlowActiveProfile persists which environment profile a flow currently
// deploys against ("" clears it back to "no profile", meaning every env var
// must have its own default or a deploy is rejected).
func (s *Store) SetFlowActiveProfile(ctx context.Context, flowID string, profileID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE flows SET active_profile_id = ? WHERE id = ?`, nullableString(profileID), flowID)
	if err != nil {
		return fmt.Errorf("api: setting flow active profile: %w", err)
	}
	return nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// resolveEnv implements VCS-140's core promise: ff's declared env[]
// variables, each resolved as profile.Values[name] if profile is non-nil
// and has that key, else the declaration's own Default, else missing.
// Returns the resolved map and the names of any variable with neither a
// profile value nor a default — the "missing-variable check at deploy" the
// spec calls for. A nil profile is valid (every declared var must then
// carry its own default).
func resolveEnv(ff *flow.FlowFile, profile *EnvironmentProfile) (resolved map[string]string, missing []string) {
	resolved = map[string]string{}
	for _, v := range ff.Env {
		if profile != nil {
			if val, ok := profile.Values[v.Name]; ok {
				resolved[v.Name] = val
				continue
			}
		}
		if v.Default != nil {
			resolved[v.Name] = fmt.Sprint(v.Default)
			continue
		}
		missing = append(missing, v.Name)
	}
	sort.Strings(missing)
	return resolved, missing
}

// resolveEnvForDeploy loads f's active profile (if any) and resolves ff's
// declared env against it, returning a 400-worthy error listing every
// missing variable by name if the check fails.
func (h *Handlers) resolveEnvForDeploy(ctx context.Context, f *Flow, ff *flow.FlowFile) (map[string]string, error) {
	var profile *EnvironmentProfile
	if f.ActiveProfileID != nil && *f.ActiveProfileID != "" {
		p, err := h.store.GetEnvironmentProfile(ctx, *f.ActiveProfileID)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("loading active profile: %w", err)
		}
		profile = p
	}
	resolved, missing := resolveEnv(ff, profile)
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing value for env variable(s) %v: no active profile provides them and they have no default", missing)
	}
	return resolved, nil
}

// --- HTTP handlers ---

func (h *Handlers) listEnvProfiles(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleViewer) {
		return
	}
	profiles, err := h.store.ListEnvironmentProfiles(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, profiles)
}

func (h *Handlers) createEnvProfile(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleEditor) {
		return
	}
	var req struct {
		Name   string            `json:"name"`
		Values map[string]string `json:"values"`
	}
	if err := readJSON(r, &req); err != nil || req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	p, err := h.store.CreateEnvironmentProfile(r.Context(), projectID, req.Name, req.Values)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "envprofile.create", "environment_profile", p.ID, projectID, nil, p)
	writeJSON(w, http.StatusCreated, p)
}

func (h *Handlers) updateEnvProfile(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	id := r.PathValue("profileId")
	before, err := h.store.GetEnvironmentProfile(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if !requireProjectRole(w, r, h.authStore, user, before.ProjectID, auth.RoleEditor) {
		return
	}
	var req struct {
		Values map[string]string `json:"values"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	after, err := h.store.UpdateEnvironmentProfileValues(r.Context(), id, req.Values)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "envprofile.update", "environment_profile", id, before.ProjectID, before, after)
	writeJSON(w, http.StatusOK, after)
}

func (h *Handlers) deleteEnvProfile(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	id := r.PathValue("profileId")
	before, err := h.store.GetEnvironmentProfile(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if !requireProjectRole(w, r, h.authStore, user, before.ProjectID, auth.RoleEditor) {
		return
	}
	if err := h.store.DeleteEnvironmentProfile(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "envprofile.delete", "environment_profile", id, before.ProjectID, before, nil)
	w.WriteHeader(http.StatusNoContent)
}
