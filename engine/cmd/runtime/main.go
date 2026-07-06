// Command runtime is the DataPipe server runtime process: it exposes a
// health endpoint, keeps itself registered with the control plane over
// gRPC, and applies flow deployments pushed down the DeployStream
// (Increment 3 deploy orchestration) via its local flow.Deployment.
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

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/health"
	"github.com/1uedev/DataPipe/engine/internal/runtimeclient"

	_ "github.com/1uedev/DataPipe/engine/nodes/debuglog"
	_ "github.com/1uedev/DataPipe/engine/nodes/inject"
	_ "github.com/1uedev/DataPipe/engine/nodes/set"
)

const version = "0.0.0-dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	httpAddr := envOr("RUNTIME_HTTP_ADDR", ":8081")
	controlPlaneAddr := envOr("CONTROLPLANE_GRPC_ADDR", "localhost:9090")
	runtimeID := envOr("RUNTIME_ID", uuid.NewString())

	healthSrv := health.NewServer()
	deployment := flow.NewDeployment(slog.Default())
	defer deployment.Stop()

	debugSink := runtimeclient.NewDebugSink()
	deployment.SetDebugSink(debugSink)

	httpServer := &http.Server{Addr: httpAddr, Handler: healthSrv.Handler()}
	go func() {
		slog.Info("runtime health endpoint listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		err := runtimeclient.Run(ctx, controlPlaneAddr, runtimeID, version, healthSrv.SetReady, applyDeploy(deployment), debugSink, deployment)
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

// applyDeploy parses and applies one pushed flow onto deployment, hot-
// swapping only the affected nodes (ENG-140). Errors are logged, not
// fatal — a bad deploy push must never take the runtime down (ARC-150).
func applyDeploy(deployment *flow.Deployment) runtimeclient.DeployHandler {
	return func(ctx context.Context, flowID string, ver int64, flowJSON string) {
		ff, err := flow.Parse([]byte(flowJSON))
		if err != nil {
			slog.Error("received undeployable flow: parse failed", "flowId", flowID, "version", ver, "error", err)
			return
		}
		if err := deployment.Deploy(ctx, ff); err != nil {
			slog.Error("deploy failed", "flowId", flowID, "version", ver, "error", err)
			return
		}
		slog.Info("flow deployed", "flowId", flowID, "version", ver)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
