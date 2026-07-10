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
	"github.com/1uedev/DataPipe/controlplane/internal/debughub"
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
// controlplane/internal/registry. defaultErrorFlow is the owning project's
// ERR-120 fallback error-handler flow id (Increment 8), "" if none.
// targetGroup is the flow's runtimeAssignment.group (Increment 9, UI-220),
// "" for "every connected runtime". logLevel is OBS-120's per-flow log
// level ("" meaning "info").
type Deployer interface {
	DeployFlow(ctx context.Context, flowID string, version int64, flowJSON, defaultErrorFlow, targetGroup, logLevel string, resolvedEnv map[string]string) error
}

// ExecutionCommander issues runtime-bound commands for triggered
// executions (Increment 8, ENG-130/DBG-140/ERR-130); implemented by
// controlplane/internal/registry.
type ExecutionCommander interface {
	RunExecution(ctx context.Context, flowID, from, nodeID, port, datagramJSON, reRunOf string) error
	CancelExecution(ctx context.Context, executionID string) error
	ReinjectDeadLetter(ctx context.Context, flowID, nodeID, port, datagramJSON string) error
}

// RuntimeInfo is the read-only fleet view (GET /runtimes), combining live
// registry state (kind/version/lastSeen/online/health) with admin-
// configured fleet metadata (displayName/group/enrolled) — Increment 9,
// EDGE-120 "inventory with health (online, CPU, memory, flow status,
// versions)".
type RuntimeInfo struct {
	RuntimeID   string    `json:"runtimeId"`
	Kind        string    `json:"kind"`
	Version     string    `json:"version"`
	LastSeen    time.Time `json:"lastSeen"`
	Online      bool      `json:"online"`
	CPUPercent  *float64  `json:"cpuPercent"`
	MemoryBytes *int64    `json:"memoryBytes"`
	FlowCount   int       `json:"flowCount"`
	DisplayName *string   `json:"displayName"`
	Group       *string   `json:"group"`
	Enrolled    bool      `json:"enrolled"`
}

// RuntimeLister backs GET /runtimes; implemented by
// controlplane/internal/registry.
type RuntimeLister interface {
	ListRuntimes(ctx context.Context) []RuntimeInfo
}

// Handlers implements every route in docs/api/openapi.yaml.
type Handlers struct {
	store     *Store
	authStore *auth.Store
	vault     *crypto.Vault
	auditLog  *audit.Log
	deployer  Deployer
	runtimes  RuntimeLister
	debugHub  *debughub.Hub
	commander ExecutionCommander
	logger    *slog.Logger
}

func NewHandlers(store *Store, authStore *auth.Store, vault *crypto.Vault, auditLog *audit.Log, deployer Deployer, runtimes RuntimeLister, debugHub *debughub.Hub, commander ExecutionCommander, logger *slog.Logger) *Handlers {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handlers{store: store, authStore: authStore, vault: vault, auditLog: auditLog, deployer: deployer, runtimes: runtimes, debugHub: debugHub, commander: commander, logger: logger}
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
	protected.HandleFunc("PATCH /flows/{flowId}/log-level", h.setFlowLogLevel)
	protected.HandleFunc("GET /flows/{flowId}/versions", h.listFlowVersions)
	protected.HandleFunc("GET /flows/{flowId}/versions/{version}", h.getFlowVersion)
	protected.HandleFunc("POST /flows/{flowId}/versions/{version}/rollback", h.rollbackFlow)
	protected.HandleFunc("GET /flows/{flowId}/export", h.exportFlow)
	protected.HandleFunc("GET /projects/{projectId}/export", h.exportProject)
	protected.HandleFunc("POST /projects/{projectId}/import", h.importProject)

	protected.HandleFunc("GET /projects/{projectId}/profiles", h.listEnvProfiles)
	protected.HandleFunc("POST /projects/{projectId}/profiles", h.createEnvProfile)
	protected.HandleFunc("PATCH /profiles/{profileId}", h.updateEnvProfile)
	protected.HandleFunc("DELETE /profiles/{profileId}", h.deleteEnvProfile)

	protected.HandleFunc("GET /projects/{projectId}/connections", h.listConnections)
	protected.HandleFunc("POST /projects/{projectId}/connections", h.createConnection)
	protected.HandleFunc("PATCH /connections/{connectionId}", h.updateConnection)
	protected.HandleFunc("DELETE /connections/{connectionId}", h.deleteConnection)
	protected.HandleFunc("POST /connections/{connectionId}/test", h.testConnection)
	protected.HandleFunc("POST /connections/{connectionId}/secsgem-browse", h.secsgemBrowse)

	protected.HandleFunc("GET /projects/{projectId}/credentials", h.listCredentials)
	protected.HandleFunc("POST /projects/{projectId}/credentials", h.createCredential)
	protected.HandleFunc("DELETE /credentials/{credentialId}", h.deleteCredential)

	protected.HandleFunc("GET /runtimes", h.listRuntimes)
	protected.HandleFunc("PATCH /runtimes/{runtimeId}", h.updateRuntime)
	protected.HandleFunc("GET /runtime-groups", h.listRuntimeGroups)
	protected.HandleFunc("POST /runtime-groups", h.createRuntimeGroup)
	protected.HandleFunc("DELETE /runtime-groups/{name}", h.deleteRuntimeGroup)
	protected.HandleFunc("GET /runtime-enroll-tokens", h.listEnrollTokens)
	protected.HandleFunc("POST /runtime-enroll-tokens", h.createEnrollToken)
	protected.HandleFunc("DELETE /runtime-enroll-tokens/{tokenId}", h.deleteEnrollToken)
	protected.HandleFunc("GET /audit-log", h.listAuditLog)
	protected.HandleFunc("GET /node-types", h.listNodeTypes)

	protected.HandleFunc("GET /alert-rules", h.listAlertRules)
	protected.HandleFunc("POST /alert-rules", h.createAlertRule)
	protected.HandleFunc("DELETE /alert-rules/{ruleId}", h.deleteAlertRule)
	protected.HandleFunc("GET /alerts", h.listAlerts)

	protected.HandleFunc("GET /backup", h.exportBackup)
	protected.HandleFunc("POST /backup/restore", h.restoreBackup)

	protected.HandleFunc("POST /flows/{flowId}/nodes/{nodeId}/execute", h.executeNode)
	protected.HandleFunc("POST /flows/{flowId}/nodes/{nodeId}/preview", h.previewNode)
	protected.HandleFunc("GET /flows/{flowId}/debug/pins", h.listPins)
	protected.HandleFunc("PUT /flows/{flowId}/nodes/{nodeId}/pins/{port}", h.upsertPin)
	protected.HandleFunc("DELETE /flows/{flowId}/nodes/{nodeId}/pins/{port}", h.deletePin)
	protected.HandleFunc("GET /flows/{flowId}/debug/events/{eventId}", h.loadFullDebugEvent)

	protected.HandleFunc("GET /flows/{flowId}/executions", h.listExecutions)
	protected.HandleFunc("GET /executions/{executionId}", h.getExecution)
	protected.HandleFunc("POST /executions/{executionId}/rerun", h.rerunExecution)
	protected.HandleFunc("POST /executions/{executionId}/cancel", h.cancelExecution)
	protected.HandleFunc("GET /flows/{flowId}/dead-letters", h.listDeadLetters)
	protected.HandleFunc("DELETE /dead-letters/{deadLetterId}", h.deleteDeadLetter)
	protected.HandleFunc("POST /dead-letters/{deadLetterId}/reinject", h.reinjectDeadLetter)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/login", h.login)
	// /ws/debug authenticates via a query-param token (docs/api/debug-websocket.md),
	// not the Authorization header, so it is mounted outside auth.Middleware.
	mux.HandleFunc("GET /ws/debug", h.debugWebSocket)
	mux.Handle("/", h.authStore.Middleware(protected))
	return mux
}
