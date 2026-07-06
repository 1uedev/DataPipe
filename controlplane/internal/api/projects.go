package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// Project is VCS-100's organizing unit: flows, subflows, connections,
// schema references, and environment profiles per project, access-
// controlled at the project level.
type Project struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
}

func (s *Store) CreateProject(ctx context.Context, name, description string) (*Project, error) {
	p := &Project{ID: uuid.NewString(), Name: name, Description: description, CreatedAt: time.Now().UTC()}
	_, err := s.db.ExecContext(ctx, `INSERT INTO projects (id, name, description, created_at) VALUES (?, ?, ?, ?)`,
		p.ID, p.Name, p.Description, p.CreatedAt.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("api: creating project: %w", err)
	}
	return p, nil
}

func (s *Store) GetProject(ctx context.Context, id string) (*Project, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, description, created_at FROM projects WHERE id = ?`, id)
	return scanProject(row)
}

// ListProjectsForUser returns every project the user is a member of;
// System Admins see every project (SEC-110 bypass).
func (s *Store) ListProjectsForUser(ctx context.Context, user *auth.User) ([]*Project, error) {
	var rows *sql.Rows
	var err error
	if user.SystemRole == auth.SystemRoleAdmin {
		rows, err = s.db.QueryContext(ctx, `SELECT id, name, description, created_at FROM projects ORDER BY name`)
	} else {
		rows, err = s.db.QueryContext(ctx,
			`SELECT p.id, p.name, p.description, p.created_at FROM projects p
			 JOIN project_members m ON m.project_id = p.id
			 WHERE m.user_id = ? ORDER BY p.name`, user.ID)
	}
	if err != nil {
		return nil, fmt.Errorf("api: listing projects: %w", err)
	}
	defer func() { _ = rows.Close() }()

	projects := make([]*Project, 0)
	for rows.Next() {
		p, err := scanProject(rows)
		if err != nil {
			return nil, err
		}
		projects = append(projects, p)
	}
	return projects, rows.Err()
}

func (s *Store) UpdateProject(ctx context.Context, id string, name, description *string) (*Project, error) {
	p, err := s.GetProject(ctx, id)
	if err != nil {
		return nil, err
	}
	if name != nil {
		p.Name = *name
	}
	if description != nil {
		p.Description = *description
	}
	_, err = s.db.ExecContext(ctx, `UPDATE projects SET name = ?, description = ? WHERE id = ?`, p.Name, p.Description, p.ID)
	if err != nil {
		return nil, fmt.Errorf("api: updating project: %w", err)
	}
	return p, nil
}

func (s *Store) DeleteProject(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM projects WHERE id = ?`, id)
	return err
}

func scanProject(row rowScanner) (*Project, error) {
	var p Project
	var createdAt string
	if err := row.Scan(&p.ID, &p.Name, &p.Description, &createdAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning project: %w", err)
	}
	t, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("api: parsing created_at: %w", err)
	}
	p.CreatedAt = t
	return &p, nil
}

// --- HTTP handlers ---

func (h *Handlers) listProjects(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projects, err := h.store.ListProjectsForUser(r.Context(), user)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, projects)
}

func (h *Handlers) createProject(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
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

	p, err := h.store.CreateProject(r.Context(), req.Name, req.Description)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// The creator becomes Project Admin (VCS-100/SEC-110).
	if err := h.authStore.SetProjectRole(r.Context(), p.ID, user.ID, auth.RoleProjectAdmin); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.create", "project", p.ID, p.ID, nil, p)
	writeJSON(w, http.StatusCreated, p)
}

func (h *Handlers) getProject(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	id := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, id, auth.RoleViewer) {
		return
	}
	p, err := h.store.GetProject(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (h *Handlers) updateProject(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	id := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, id, auth.RoleProjectAdmin) {
		return
	}
	before, err := h.store.GetProject(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}

	var req struct {
		Name        *string `json:"name"`
		Description *string `json:"description"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	after, err := h.store.UpdateProject(r.Context(), id, req.Name, req.Description)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.update", "project", id, id, before, after)
	writeJSON(w, http.StatusOK, after)
}

func (h *Handlers) deleteProject(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	id := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, id, auth.RoleProjectAdmin) {
		return
	}
	before, _ := h.store.GetProject(r.Context(), id)
	if err := h.store.DeleteProject(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.delete", "project", id, id, before, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) setProjectMember(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	targetUserID := r.PathValue("userId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleProjectAdmin) {
		return
	}
	var req struct {
		Role auth.ProjectRole `json:"role"`
	}
	if err := readJSON(r, &req); err != nil || req.Role == "" {
		writeError(w, http.StatusBadRequest, "role is required")
		return
	}
	if err := h.authStore.SetProjectRole(r.Context(), projectID, targetUserID, req.Role); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.member.set", "project_member", targetUserID, projectID, nil, req.Role)
	writeJSON(w, http.StatusOK, map[string]string{"userId": targetUserID, "role": string(req.Role)})
}

func (h *Handlers) removeProjectMember(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	targetUserID := r.PathValue("userId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleProjectAdmin) {
		return
	}
	if err := h.authStore.RemoveProjectMember(r.Context(), projectID, targetUserID); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.member.remove", "project_member", targetUserID, projectID, nil, nil)
	w.WriteHeader(http.StatusNoContent)
}
