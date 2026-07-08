// vcs_export.go implements VCS-130's portable flow/project import-export:
// flows and projects as portable JSON including subflows (a subflow's
// content just round-trips like any other flow's — PROC-160/UI-140
// subflow-call reuse itself is not implemented, per TODO.md, so there is
// no subflow-instantiation semantics to preserve here) and referenced (not
// embedded) connection definitions. Secrets are never exported: a
// connection's credentialId is dropped entirely, replaced by a boolean
// flag so the importing user knows a credential needs (re)attaching.
// Imports remap a bundle's connection references onto the target
// project's existing connections when name+type match, or create a new
// (credential-less) connection otherwise — this is the "remap or prompt"
// VCS-130 calls for; "prompt" is a UI-side follow-up once ConnectionsCreated
// comes back non-empty (see docs/User-Guide.md).
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/engine/flow"
)

const exportFormatVersion = 1

// ExportedConnection is a connection's portable, secret-free shape: same
// name/type/config an admin would enter by hand, plus whether the
// original had a credential attached (so the importing UI can prompt for
// one — the credential value itself never leaves the vault).
type ExportedConnection struct {
	Ref           string          `json:"ref"`
	Name          string          `json:"name"`
	Type          string          `json:"type"`
	Config        json.RawMessage `json:"config"`
	HasCredential bool            `json:"hasCredential"`
}

// ExportedFlow carries a flow's canonical content with every node's
// `connection` field rewritten from a project-local connection id to a
// bundle-local ExportedConnection.Ref token, so the bundle is meaningful
// outside the project (or control plane) it came from.
type ExportedFlow struct {
	Name    string          `json:"name"`
	Content json.RawMessage `json:"content"`
}

// FlowExportBundle is VCS-130's portable JSON document: one or more flows
// plus the connections they reference.
type FlowExportBundle struct {
	FormatVersion int                  `json:"formatVersion"`
	ExportedAt    time.Time            `json:"exportedAt"`
	ProjectName   string               `json:"projectName,omitempty"`
	Flows         []ExportedFlow       `json:"flows"`
	Connections   []ExportedConnection `json:"connections"`
}

// ImportResult reports what an import actually did, so the UI can show
// "N connections need a credential attached" for ConnectionsCreated.
type ImportResult struct {
	Flows              []*Flow       `json:"flows"`
	ConnectionsCreated []*Connection `json:"connectionsCreated"`
	ConnectionsMatched []*Connection `json:"connectionsMatched"`
}

// buildExportBundle rewrites each flow's node connection ids to bundle-
// local refs, collecting the referenced connections' portable definitions
// along the way. A node referencing a connection id that no longer exists
// (already deleted) is left with an empty connection field in the export —
// deploy-time validation catches that same way it would for any other
// missing connection.
func buildExportBundle(ctx context.Context, store *Store, projectName string, flows []*Flow) (*FlowExportBundle, error) {
	bundle := &FlowExportBundle{FormatVersion: exportFormatVersion, ExportedAt: time.Now().UTC(), ProjectName: projectName}
	refByConnectionID := map[string]string{}

	for _, f := range flows {
		ff, err := flow.Parse(f.Content)
		if err != nil {
			return nil, fmt.Errorf("api: parsing flow %q for export: %w", f.Name, err)
		}
		for i := range ff.Graph.Nodes {
			n := &ff.Graph.Nodes[i]
			if n.Connection == "" {
				continue
			}
			ref, ok := refByConnectionID[n.Connection]
			if !ok {
				conn, err := store.GetConnection(ctx, n.Connection)
				if err != nil {
					n.Connection = ""
					continue
				}
				ref = fmt.Sprintf("ref:%d", len(bundle.Connections))
				refByConnectionID[n.Connection] = ref
				bundle.Connections = append(bundle.Connections, ExportedConnection{
					Ref: ref, Name: conn.Name, Type: conn.Type, Config: conn.Config, HasCredential: conn.CredentialID != nil,
				})
			}
			n.Connection = ref
		}
		content, err := ff.MarshalCanonical()
		if err != nil {
			return nil, fmt.Errorf("api: serializing flow %q for export: %w", f.Name, err)
		}
		bundle.Flows = append(bundle.Flows, ExportedFlow{Name: f.Name, Content: content})
	}
	return bundle, nil
}

// importBundle creates every bundle flow as a new draft flow in
// projectID, remapping connection refs onto an existing same-name-and-
// type connection in the target project when one exists, or creating a
// new (credential-less) one otherwise.
func importBundle(ctx context.Context, store *Store, projectID string, bundle *FlowExportBundle) (*ImportResult, error) {
	existing, err := store.ListConnections(ctx, projectID)
	if err != nil {
		return nil, fmt.Errorf("api: listing target connections: %w", err)
	}
	type nameType struct{ name, connType string }
	byNameType := make(map[nameType]*Connection, len(existing))
	for _, c := range existing {
		byNameType[nameType{c.Name, c.Type}] = c
	}

	result := &ImportResult{}
	idByRef := make(map[string]string, len(bundle.Connections))
	for _, ec := range bundle.Connections {
		if match, ok := byNameType[nameType{ec.Name, ec.Type}]; ok {
			idByRef[ec.Ref] = match.ID
			result.ConnectionsMatched = append(result.ConnectionsMatched, match)
			continue
		}
		created, err := store.CreateConnection(ctx, projectID, ec.Name, ec.Type, ec.Config, nil)
		if err != nil {
			return nil, fmt.Errorf("api: creating connection %q on import: %w", ec.Name, err)
		}
		idByRef[ec.Ref] = created.ID
		result.ConnectionsCreated = append(result.ConnectionsCreated, created)
	}

	for _, ef := range bundle.Flows {
		ff, err := flow.Parse(ef.Content)
		if err != nil {
			return nil, fmt.Errorf("api: parsing bundle flow %q: %w", ef.Name, err)
		}
		for i := range ff.Graph.Nodes {
			n := &ff.Graph.Nodes[i]
			if id, ok := idByRef[n.Connection]; ok {
				n.Connection = id
			}
		}
		content, err := ff.MarshalCanonical()
		if err != nil {
			return nil, fmt.Errorf("api: serializing bundle flow %q: %w", ef.Name, err)
		}
		created, err := store.CreateFlow(ctx, projectID, ef.Name, content)
		if err != nil {
			return nil, fmt.Errorf("api: creating imported flow %q: %w", ef.Name, err)
		}
		result.Flows = append(result.Flows, created)
	}
	return result, nil
}

// --- HTTP handlers ---

func (h *Handlers) exportFlow(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	bundle, err := buildExportBundle(r.Context(), h.store, "", []*Flow{f})
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "flow.export", "flow", f.ID, f.ProjectID, nil, nil)
	w.Header().Set("Content-Disposition", `attachment; filename="`+f.Name+`.flow.json"`)
	writeJSON(w, http.StatusOK, bundle)
}

func (h *Handlers) exportProject(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleViewer) {
		return
	}
	project, err := h.store.GetProject(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	flows, err := h.store.ListFlows(r.Context(), projectID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	bundle, err := buildExportBundle(r.Context(), h.store, project.Name, flows)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.export", "project", projectID, projectID, nil, nil)
	w.Header().Set("Content-Disposition", `attachment; filename="`+project.Name+`.project.json"`)
	writeJSON(w, http.StatusOK, bundle)
}

func (h *Handlers) importProject(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	projectID := r.PathValue("projectId")
	if !requireProjectRole(w, r, h.authStore, user, projectID, auth.RoleEditor) {
		return
	}
	var bundle FlowExportBundle
	if err := readJSON(r, &bundle); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if len(bundle.Flows) == 0 {
		writeError(w, http.StatusBadRequest, "bundle has no flows")
		return
	}
	result, err := importBundle(r.Context(), h.store, projectID, &bundle)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "project.import", "project", projectID, projectID, nil, result)
	writeJSON(w, http.StatusCreated, result)
}
