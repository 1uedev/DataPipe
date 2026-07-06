// Command runtime is the DataPipe server runtime process (Development-Plan
// Increment 0 walking skeleton): it exposes a health endpoint and keeps
// itself registered with the control plane over gRPC.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/engine/internal/health"
	"github.com/1uedev/DataPipe/engine/internal/runtimeclient"
)

const version = "0.0.0-dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpAddr := envOr("RUNTIME_HTTP_ADDR", ":8081")
	controlPlaneAddr := envOr("CONTROLPLANE_GRPC_ADDR", "localhost:9090")
	runtimeID := envOr("RUNTIME_ID", uuid.NewString())

	healthSrv := health.NewServer()

	httpServer := &http.Server{Addr: httpAddr, Handler: healthSrv.Handler()}
	go func() {
		slog.Info("runtime health endpoint listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		err := runtimeclient.Run(ctx, controlPlaneAddr, runtimeID, version, healthSrv.SetReady)
		if err != nil && ctx.Err() == nil {
			slog.Error("runtime client stopped unexpectedly", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down runtime")
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
