package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/1uedev/DataPipe/controlplane/internal/audit"
	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
	"github.com/1uedev/DataPipe/controlplane/internal/db"
)

// Store is the plain CRUD layer over the SQL database; RBAC and audit
// logging live one level up, in the HTTP handlers, so Store stays a dumb
// persistence layer.
type Store struct {
	db *db.DB
}

func NewStore(d *db.DB) *Store {
	return &Store{db: d}
}

type rowScanner interface {
	Scan(dest ...any) error
}

// Deployer pushes a flow version to whichever runtime it's assigned to
// (Increment 3's deploy orchestration); implemented by
// controlplane/internal/registry.
type Deployer interface {
	DeployFlow(ctx context.Context, flowID string, version int64, flowJSON string) error
}

// RuntimeInfo is the read-only fleet view (GET /runtimes).
type RuntimeInfo struct {
	RuntimeID string    `json:"runtimeId"`
	Kind      string    `json:"kind"`
	Version   string    `json:"version"`
	LastSeen  time.Time `json:"lastSeen"`
}

// RuntimeLister backs GET /runtimes; implemented by
// controlplane/internal/registry.
type RuntimeLister interface {
	ListRuntimes() []RuntimeInfo
}

// Handlers implements every route in docs/api/openapi.yaml.
type Handlers struct {
	store     *Store
	authStore *auth.Store
	vault     *crypto.Vault
	auditLog  *audit.Log
	deployer  Deployer
	runtimes  RuntimeLister
	logger    *slog.Logger
}

func NewHandlers(store *Store, authStore *auth.Store, vault *crypto.Vault, auditLog *audit.Log, deployer Deployer, runtimes RuntimeLister, logger *slog.Logger) *Handlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handlers{store: store, authStore: authStore, vault: vault, auditLog: auditLog, deployer: deployer, runtimes: runtimes, logger: logger}
}

// audit records a SEC-140 entry; a failure here is logged but never fails
// the request that triggered it (the action already happened).
func (h *Handlers) audit(r *http.Request, actorUserID, action, objectType, objectID, projectID string, before, after any) {
	if _, err := h.auditLog.Append(r.Context(), actorUserID, action, objectType, objectID, projectID, before, after); err != nil {
		h.logger.Error("audit append failed", "error", err, "action", action)
	}
}

// Routes builds the full route table. Callers should mount it under the
// OpenAPI "servers" prefix (/api/v1), e.g.:
//
//	http.Handle("/api/v1/", http.StripPrefix("/api/v1", handlers.Routes()))
func (h *Handlers) Routes() http.Handler {
	protected := http.NewServeMux()
	protected.HandleFunc("POST /auth/logout", h.logout)
	protected.HandleFunc("GET /auth/me", h.me)

	protected.HandleFunc("GET /users", h.listUsers)
	protected.HandleFunc("POST /users", h.createUser)

	protected.HandleFunc("GET /projects", h.listProjects)
	protected.HandleFunc("POST /projects", h.createProject)
	protected.HandleFunc("GET /projects/{projectId}", h.getProject)
	protected.HandleFunc("PATCH /projects/{projectId}", h.updateProject)
	protected.HandleFunc("DELETE /projects/{projectId}", h.deleteProject)
	protected.HandleFunc("PUT /projects/{projectId}/members/{userId}", h.setProjectMember)
	protected.HandleFunc("DELETE /projects/{projectId}/members/{userId}", h.removeProjectMember)

	protected.HandleFunc("GET /projects/{projectId}/flows", h.listFlows)
	protected.HandleFunc("POST /projects/{projectId}/flows", h.createFlow)
	protected.HandleFunc("GET /flows/{flowId}", h.getFlow)
	protected.HandleFunc("PATCH /flows/{flowId}", h.updateFlow)
	protected.HandleFunc("DELETE /flows/{flowId}", h.deleteFlow)
	protected.HandleFunc("POST /flows/{flowId}/deploy", h.deployFlow)
	protected.HandleFunc("GET /flows/{flowId}/versions", h.listFlowVersions)
	protected.HandleFunc("GET /flows/{flowId}/versions/{version}", h.getFlowVersion)
	protected.HandleFunc("POST /flows/{flowId}/versions/{version}/rollback", h.rollbackFlow)

	protected.HandleFunc("GET /projects/{projectId}/connections", h.listConnections)
	protected.HandleFunc("POST /projects/{projectId}/connections", h.createConnection)
	protected.HandleFunc("PATCH /connections/{connectionId}", h.updateConnection)
	protected.HandleFunc("DELETE /connections/{connectionId}", h.deleteConnection)

	protected.HandleFunc("GET /projects/{projectId}/credentials", h.listCredentials)
	protected.HandleFunc("POST /projects/{projectId}/credentials", h.createCredential)
	protected.HandleFunc("DELETE /credentials/{credentialId}", h.deleteCredential)

	protected.HandleFunc("GET /runtimes", h.listRuntimes)
	protected.HandleFunc("GET /audit-log", h.listAuditLog)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/login", h.login)
	mux.Handle("/", h.authStore.Middleware(protected))
	return mux
}
