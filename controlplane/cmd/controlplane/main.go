// Command controlplane is the DataPipe control-plane API server: gRPC
// runtime registration + deploy-command push (ARC-210/ADR-007), and the
// REST API of docs/api/openapi.yaml (Development-Plan Increment 3) —
// projects, flows CRUD + deploy + immutable versions (VCS-110),
// connections + write-only credentials (SEC-120), local auth + RBAC
// (SEC-100/110), audit log (SEC-140).
package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"

	"github.com/1uedev/DataPipe/controlplane/internal/alerting"
	"github.com/1uedev/DataPipe/controlplane/internal/api"
	"github.com/1uedev/DataPipe/controlplane/internal/audit"
	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
	"github.com/1uedev/DataPipe/controlplane/internal/db"
	"github.com/1uedev/DataPipe/controlplane/internal/health"
	"github.com/1uedev/DataPipe/controlplane/internal/registry"

	_ "github.com/1uedev/DataPipe/engine/nodes/busin"
	_ "github.com/1uedev/DataPipe/engine/nodes/busout"
	_ "github.com/1uedev/DataPipe/engine/nodes/calculator"
	_ "github.com/1uedev/DataPipe/engine/nodes/convert"
	_ "github.com/1uedev/DataPipe/engine/nodes/debuglog"
	_ "github.com/1uedev/DataPipe/engine/nodes/delay"
	_ "github.com/1uedev/DataPipe/engine/nodes/errortrigger"
	_ "github.com/1uedev/DataPipe/engine/nodes/filewatch"
	_ "github.com/1uedev/DataPipe/engine/nodes/filter"
	_ "github.com/1uedev/DataPipe/engine/nodes/httpin"
	_ "github.com/1uedev/DataPipe/engine/nodes/httprequest"
	_ "github.com/1uedev/DataPipe/engine/nodes/httpresponse"
	_ "github.com/1uedev/DataPipe/engine/nodes/inject"
	_ "github.com/1uedev/DataPipe/engine/nodes/kafkain"
	_ "github.com/1uedev/DataPipe/engine/nodes/kafkaout"
	_ "github.com/1uedev/DataPipe/engine/nodes/lookup"
	_ "github.com/1uedev/DataPipe/engine/nodes/loop"
	_ "github.com/1uedev/DataPipe/engine/nodes/merge"
	_ "github.com/1uedev/DataPipe/engine/nodes/modbussink"
	_ "github.com/1uedev/DataPipe/engine/nodes/modbussource"
	_ "github.com/1uedev/DataPipe/engine/nodes/mongosink"
	_ "github.com/1uedev/DataPipe/engine/nodes/mongosource"
	_ "github.com/1uedev/DataPipe/engine/nodes/mqttin"
	_ "github.com/1uedev/DataPipe/engine/nodes/mqttout"
	_ "github.com/1uedev/DataPipe/engine/nodes/opcuasink"
	_ "github.com/1uedev/DataPipe/engine/nodes/opcuasource"
	_ "github.com/1uedev/DataPipe/engine/nodes/redissink"
	_ "github.com/1uedev/DataPipe/engine/nodes/redissource"
	_ "github.com/1uedev/DataPipe/engine/nodes/s3sink"
	_ "github.com/1uedev/DataPipe/engine/nodes/s3source"
	_ "github.com/1uedev/DataPipe/engine/nodes/schedule"
	_ "github.com/1uedev/DataPipe/engine/nodes/script"
	_ "github.com/1uedev/DataPipe/engine/nodes/secsgemhost"
	_ "github.com/1uedev/DataPipe/engine/nodes/serialin"
	_ "github.com/1uedev/DataPipe/engine/nodes/serialout"
	_ "github.com/1uedev/DataPipe/engine/nodes/set"
	_ "github.com/1uedev/DataPipe/engine/nodes/splitbatch"
	_ "github.com/1uedev/DataPipe/engine/nodes/sqlsink"
	_ "github.com/1uedev/DataPipe/engine/nodes/sqlsource"
	_ "github.com/1uedev/DataPipe/engine/nodes/state"
	_ "github.com/1uedev/DataPipe/engine/nodes/stoperror"
	_ "github.com/1uedev/DataPipe/engine/nodes/switchroute"
	_ "github.com/1uedev/DataPipe/engine/nodes/tcpin"
	_ "github.com/1uedev/DataPipe/engine/nodes/tcpout"
	_ "github.com/1uedev/DataPipe/engine/nodes/template"
	_ "github.com/1uedev/DataPipe/engine/nodes/trycatch"
	_ "github.com/1uedev/DataPipe/engine/nodes/udpin"
	_ "github.com/1uedev/DataPipe/engine/nodes/udpout"
	_ "github.com/1uedev/DataPipe/engine/nodes/websocketin"
	_ "github.com/1uedev/DataPipe/engine/nodes/websocketout"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// OBS-120: structured JSON logs, correlated by whatever key/value pairs
	// each call site already attaches (flowId/nodeId/executionId/runtimeId
	// etc — slog's attrs become JSON fields directly, no separate
	// correlation mechanism needed).
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	httpAddr := envOr("CONTROLPLANE_HTTP_ADDR", ":8080")
	grpcAddr := envOr("CONTROLPLANE_GRPC_ADDR", ":9090")
	dsn := envOr("DATABASE_URL", "postgres://datapipe:datapipe@localhost:5432/datapipe?sslmode=disable")

	database, err := db.Open(dsn)
	if err != nil {
		slog.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer func() { _ = database.Close() }()

	if err := database.Migrate(ctx); err != nil {
		slog.Error("failed to migrate database", "error", err)
		os.Exit(1)
	}

	vault, err := newVaultFromEnv()
	if err != nil {
		slog.Error("failed to initialize credential vault", "error", err)
		os.Exit(1)
	}

	authStore := auth.NewStore(database)
	auditLog := audit.NewLog(database)
	apiStore := api.NewStore(database)
	reg := registry.NewService()
	reg.SetConnectionResolver(connectionResolverAdapter{api.NewConnectionResolver(apiStore, vault)})
	reg.SetExecutionStore(executionStoreAdapter{apiStore})
	reg.SetDeployedFlowsLister(deployedFlowsListerAdapter{apiStore})
	reg.SetDeviceStore(deviceStoreAdapter{apiStore})

	// OBS-140: alerting hooks — periodically evaluate rules against live
	// runtime state and fire/resolve alerts (webhook delivery included).
	alertEvaluator := alerting.NewEvaluator(alertStoreAdapter{apiStore}, alertRuntimeListerAdapter{reg})
	go alertEvaluator.Run(ctx)

	if err := bootstrapAdmin(ctx, authStore); err != nil {
		slog.Error("failed to bootstrap admin user", "error", err)
		os.Exit(1)
	}

	handlers := api.NewHandlers(apiStore, authStore, vault, auditLog, reg, runtimeLister{reg}, reg.DebugHub(), reg, slog.Default())

	healthSrv := health.NewServer(func() error {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return database.PingContext(pingCtx)
	})

	mux := http.NewServeMux()
	mux.Handle("/healthz", healthSrv.Handler())
	mux.Handle("/api/v1/", http.StripPrefix("/api/v1", handlers.Routes()))
	httpServer := &http.Server{Addr: httpAddr, Handler: mux}

	grpcServer := grpc.NewServer()
	runtimev1.RegisterRuntimeRegistryServiceServer(grpcServer, reg)

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("failed to listen for gRPC", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("control plane HTTP endpoint listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http server failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		slog.Info("control plane gRPC endpoint listening", "addr", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down control plane")
	grpcServer.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
}

// newVaultFromEnv reads the SEC-120 master key from DATAPIPE_MASTER_KEY
// (base64-encoded, 32 raw bytes). There is no safe default: an
// envelope-encryption vault without an explicit key would either fail
// silently or use a predictable key, so this is a hard startup requirement.
func newVaultFromEnv() (*crypto.Vault, error) {
	encoded := os.Getenv("DATAPIPE_MASTER_KEY")
	if encoded == "" {
		return nil, fmt.Errorf("DATAPIPE_MASTER_KEY is required (base64-encoded 32-byte AES-256 key)")
	}
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("DATAPIPE_MASTER_KEY: %w", err)
	}
	return crypto.NewVault(key)
}

// bootstrapAdmin creates the first System Admin account from
// DATAPIPE_ADMIN_USERNAME/DATAPIPE_ADMIN_PASSWORD if no users exist yet.
// There is deliberately no unauthenticated "create the first user" API
// endpoint, so some out-of-band bootstrap is unavoidable; this is that
// bootstrap, gated on the users table actually being empty.
func bootstrapAdmin(ctx context.Context, authStore *auth.Store) error {
	users, err := authStore.ListUsers(ctx)
	if err != nil {
		return err
	}
	if len(users) > 0 {
		return nil
	}

	username := envOr("DATAPIPE_ADMIN_USERNAME", "admin")
	password := os.Getenv("DATAPIPE_ADMIN_PASSWORD")
	if password == "" {
		return fmt.Errorf("no users exist yet; set DATAPIPE_ADMIN_PASSWORD to bootstrap the initial system_admin account %q", username)
	}
	if _, err := authStore.CreateUser(ctx, username, password, auth.SystemRoleAdmin); err != nil {
		return err
	}
	slog.Info("bootstrapped initial system_admin account", "username", username)
	return nil
}

// runtimeLister adapts registry.Service's own RuntimeSnapshot type to
// api.RuntimeLister without making the lower-level registry package depend
// on the api package's types.
type runtimeLister struct{ reg *registry.Service }

func (a runtimeLister) ListRuntimes(ctx context.Context) []api.RuntimeInfo {
	snaps := a.reg.ListRuntimes(ctx)
	out := make([]api.RuntimeInfo, len(snaps))
	for i, s := range snaps {
		info := api.RuntimeInfo{
			RuntimeID: s.RuntimeID, Kind: s.Kind, Version: s.Version, LastSeen: s.LastSeen,
			Online: s.Online, FlowCount: s.FlowCount, Enrolled: s.Enrolled,
		}
		if s.CPUPercent != nil {
			info.CPUPercent = s.CPUPercent
		}
		if s.MemoryBytes != nil {
			mem := int64(*s.MemoryBytes)
			info.MemoryBytes = &mem
		}
		if s.DisplayName != "" {
			info.DisplayName = &s.DisplayName
		}
		if s.Group != "" {
			info.Group = &s.Group
		}
		out[i] = info
	}
	return out
}

// deviceStoreAdapter adapts api.Store to registry.DeviceStore (Increment
// 9, EDGE-120/ARC-210), the same no-lower-package-depends-on-a-higher-one
// pattern as the adapters above (DeviceInfo is a distinct named struct in
// each package, so — unlike api.ExecutionCommander — this needs a real
// adapter, not direct satisfaction).
type deviceStoreAdapter struct{ store *api.Store }

func (a deviceStoreAdapter) Authenticate(ctx context.Context, runtimeID, kind, enrollmentToken string) error {
	return a.store.Authenticate(ctx, runtimeID, kind, enrollmentToken)
}

func (a deviceStoreAdapter) GroupOf(ctx context.Context, runtimeID string) (string, error) {
	return a.store.GroupOf(ctx, runtimeID)
}

func (a deviceStoreAdapter) DeviceInfo(ctx context.Context, runtimeID string) (registry.DeviceInfo, error) {
	info, err := a.store.DeviceInfo(ctx, runtimeID)
	if err != nil {
		return registry.DeviceInfo{}, err
	}
	return registry.DeviceInfo{Kind: info.Kind, DisplayName: info.DisplayName, GroupName: info.GroupName, Enrolled: info.Enrolled}, nil
}

// connectionResolverAdapter adapts api.ConnectionResolver's own
// ConnectionInfo type to registry.ConnectionInfo, the same
// no-lower-package-depends-on-a-higher-one pattern as runtimeLister above.
type connectionResolverAdapter struct{ inner *api.ConnectionResolver }

func (a connectionResolverAdapter) ResolveConnection(ctx context.Context, connectionID string) (registry.ConnectionInfo, error) {
	info, err := a.inner.ResolveConnection(ctx, connectionID)
	if err != nil {
		return registry.ConnectionInfo{}, err
	}
	return registry.ConnectionInfo{Type: info.Type, ConfigJSON: info.ConfigJSON, CredentialJSON: info.CredentialJSON}, nil
}

// executionStoreAdapter adapts api.Store to registry.ExecutionStore
// (Increment 8), the same no-lower-package-depends-on-a-higher-one pattern
// as the adapters above.
type executionStoreAdapter struct{ store *api.Store }

func (a executionStoreAdapter) RecordExecutionEvent(ctx context.Context, runtimeID string, ev registry.ExecutionEvent) error {
	return a.store.RecordExecutionEvent(ctx, runtimeID, api.ExecutionEventInput{
		ExecutionID: ev.ExecutionID, FlowID: ev.FlowID, Phase: ev.Phase, TimeUnixMs: ev.TimeUnixMs,
		TriggerNodeID: ev.TriggerNodeID, TriggerKind: ev.TriggerKind, ReRunOf: ev.ReRunOf, SeedDatagramJSON: ev.SeedDatagramJSON,
		NodeID: ev.NodeID, Port: ev.Port, Attempt: ev.Attempt, DurationUs: ev.DurationUs,
		InputJSON: ev.InputJSON, OutputsJSON: ev.OutputsJSON,
		ErrorMessage: ev.ErrorMessage, ErrorCode: ev.ErrorCode, ErrorStack: ev.ErrorStack,
		Status: ev.Status, Reason: ev.Reason,
	})
}

func (a executionStoreAdapter) RecordDeadLetter(ctx context.Context, runtimeID string, ev registry.DeadLetterEvent) error {
	return a.store.RecordDeadLetter(ctx, runtimeID, api.DeadLetterEventInput{
		FlowID: ev.FlowID, NodeID: ev.NodeID, Port: ev.Port, Reason: ev.Reason, DatagramJSON: ev.DatagramJSON, TimeUnixMs: ev.TimeUnixMs,
	})
}

func (a executionStoreAdapter) MarkRuntimeExecutionsCrashed(ctx context.Context, runtimeID string) error {
	return a.store.MarkRuntimeExecutionsCrashed(ctx, runtimeID)
}

// deployedFlowsListerAdapter adapts api.Store to registry.
// DeployedFlowsLister (Increment 8, ERR-150).
type deployedFlowsListerAdapter struct{ store *api.Store }

func (a deployedFlowsListerAdapter) ListDeployedFlows(ctx context.Context) ([]registry.DeployedFlowInfo, error) {
	flows, err := a.store.ListDeployedFlows(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]registry.DeployedFlowInfo, len(flows))
	for i, f := range flows {
		out[i] = registry.DeployedFlowInfo{FlowID: f.FlowID, Version: f.Version, ContentJSON: f.ContentJSON, DefaultErrorFlow: f.DefaultErrorFlow, TargetGroup: f.TargetGroup, LogLevel: f.LogLevel, ResolvedEnv: f.ResolvedEnv}
	}
	return out, nil
}

// alertStoreAdapter adapts api.Store to alerting.Store — the same
// no-lower-package-depends-on-a-higher-one pattern as the adapters above.
type alertStoreAdapter struct{ store *api.Store }

func (a alertStoreAdapter) ListEnabledRules(ctx context.Context) ([]alerting.Rule, error) {
	rules, err := a.store.ListAlertRules(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]alerting.Rule, 0, len(rules))
	for _, r := range rules {
		if !r.Enabled {
			continue
		}
		out = append(out, alerting.Rule{ID: r.ID, Name: r.Name, Metric: alerting.Metric(r.Metric), TargetRuntimeID: r.TargetRuntimeID, WebhookURL: r.WebhookURL})
	}
	return out, nil
}

func (a alertStoreAdapter) OpenAlert(ctx context.Context, ruleID string) (string, bool, error) {
	return a.store.OpenAlert(ctx, ruleID)
}

func (a alertStoreAdapter) CreateAlert(ctx context.Context, ruleID, message string) error {
	return a.store.CreateAlert(ctx, ruleID, message)
}

func (a alertStoreAdapter) ResolveAlert(ctx context.Context, alertID string) error {
	return a.store.ResolveAlert(ctx, alertID)
}

// alertRuntimeListerAdapter adapts registry.Service to alerting.RuntimeLister.
type alertRuntimeListerAdapter struct{ reg *registry.Service }

func (a alertRuntimeListerAdapter) ListRuntimeStatuses(ctx context.Context) ([]alerting.RuntimeStatus, error) {
	snaps := a.reg.ListRuntimes(ctx)
	out := make([]alerting.RuntimeStatus, len(snaps))
	for i, s := range snaps {
		out[i] = alerting.RuntimeStatus{RuntimeID: s.RuntimeID, Online: s.Online}
	}
	return out, nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
