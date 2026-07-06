// Command controlplane is the DataPipe control-plane API server
// (Development-Plan Increment 0 walking skeleton): it accepts runtime
// registrations over gRPC and exposes a Postgres-backed health endpoint.
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"

	"github.com/1uedev/DataPipe/controlplane/internal/health"
	"github.com/1uedev/DataPipe/controlplane/internal/registry"
	"github.com/1uedev/DataPipe/controlplane/internal/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpAddr := envOr("CONTROLPLANE_HTTP_ADDR", ":8080")
	grpcAddr := envOr("CONTROLPLANE_GRPC_ADDR", ":9090")
	dsn := envOr("DATABASE_URL", "postgres://datapipe:datapipe@localhost:5432/datapipe?sslmode=disable")

	db, err := store.Open(dsn)
	if err != nil {
		slog.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	healthSrv := health.NewServer(func() error {
		pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return db.Ping(pingCtx)
	})
	httpServer := &http.Server{Addr: httpAddr, Handler: healthSrv.Handler()}

	grpcServer := grpc.NewServer()
	runtimev1.RegisterRuntimeRegistryServiceServer(grpcServer, registry.NewService())

	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		slog.Error("failed to listen for gRPC", "error", err)
		os.Exit(1)
	}

	go func() {
		slog.Info("control plane health endpoint listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server failed", "error", err)
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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
