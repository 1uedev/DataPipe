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

	"github.com/1uedev/DataPipe/controlplane/internal/api"
	"github.com/1uedev/DataPipe/controlplane/internal/audit"
	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
	"github.com/1uedev/DataPipe/controlplane/internal/db"
	"github.com/1uedev/DataPipe/controlplane/internal/health"
	"github.com/1uedev/DataPipe/controlplane/internal/registry"

	_ "github.com/1uedev/DataPipe/engine/nodes/debuglog"
	_ "github.com/1uedev/DataPipe/engine/nodes/inject"
	_ "github.com/1uedev/DataPipe/engine/nodes/set"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	if err := bootstrapAdmin(ctx, authStore); err != nil {
		slog.Error("failed to bootstrap admin user", "error", err)
		os.Exit(1)
	}

	handlers := api.NewHandlers(apiStore, authStore, vault, auditLog, reg, runtimeLister{reg}, reg.DebugHub(), slog.Default())

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

func (a runtimeLister) ListRuntimes() []api.RuntimeInfo {
	snaps := a.reg.ListRuntimes()
	out := make([]api.RuntimeInfo, len(snaps))
	for i, s := range snaps {
		out[i] = api.RuntimeInfo{RuntimeID: s.RuntimeID, Kind: s.Kind, Version: s.Version, LastSeen: s.LastSeen}
	}
	return out
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
