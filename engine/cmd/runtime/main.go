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
	"github.com/1uedev/DataPipe/engine/internal/obsmetrics"
	"github.com/1uedev/DataPipe/engine/internal/procstats"
	"github.com/1uedev/DataPipe/engine/internal/runtimeclient"
	"github.com/1uedev/DataPipe/engine/webhook"

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
	_ "github.com/1uedev/DataPipe/engine/nodes/secsgemaction"
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

const version = "0.0.0-dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// OBS-120: structured JSON logs, dynamically re-levelable per flow at
	// runtime (logLevelVar, set from applyDeploy) without a redeploy —
	// slog.LevelVar is exactly the "already-constructed handler whose
	// verbosity can change later" primitive this needs.
	logLevelVar := new(slog.LevelVar)
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevelVar})))

	httpAddr := envOr("RUNTIME_HTTP_ADDR", ":8081")
	webhookAddr := envOr("RUNTIME_WEBHOOK_ADDR", ":8090")
	controlPlaneAddr := envOr("CONTROLPLANE_GRPC_ADDR", "localhost:9090")
	runtimeID := envOr("RUNTIME_ID", uuid.NewString())
	enrollmentToken := envOr("RUNTIME_ENROLL_TOKEN", "")
	dataDir := envOr("RUNTIME_DATA_DIR", "./data")

	healthSrv := health.NewServer()
	deployment := flow.NewDeployment(slog.Default())
	defer deployment.Stop()
	deployment.SetDataDir(dataDir)

	cpuSampler := procstats.NewSampler()
	healthProvider := func() runtimeclient.HealthSnapshot {
		snap := runtimeclient.HealthSnapshot{
			CPUPercent:  cpuSampler.CPUPercent(),
			MemoryBytes: procstats.MemoryBytes(),
		}
		if id := deployment.FlowID(); id != "" {
			snap.FlowStatuses = []runtimeclient.FlowStatus{{FlowID: id, Status: "running"}}
		}
		return snap
	}

	debugSink := runtimeclient.NewDebugSink()
	deployment.SetDebugSink(debugSink)

	connResolver := runtimeclient.NewConnectionResolver()
	deployment.SetConnectionResolver(connResolver)

	eventSink := runtimeclient.NewEventSink()
	deployment.SetExecutionSink(eventSink)
	deployment.SetDeadLetterSink(eventSink)

	// A separate Sampler from the one the heartbeat loop uses below: each
	// Sampler tracks CPU time since its own last call, so sharing one
	// between two independent periodic callers (heartbeat vs. whoever
	// scrapes /metrics, on their own schedule) would corrupt both deltas.
	metricsCPUSampler := procstats.NewSampler()
	mux := http.NewServeMux()
	mux.Handle("/", healthSrv.Handler())
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		rt := obsmetrics.RuntimeInfo{RuntimeID: runtimeID, CPUPercent: metricsCPUSampler.CPUPercent(), MemoryBytes: procstats.MemoryBytes()}
		_, _ = w.Write([]byte(obsmetrics.FormatPrometheus(deployment.MetricsSnapshot(), rt)))
	})
	httpServer := &http.Server{Addr: httpAddr, Handler: mux}
	go func() {
		slog.Info("runtime health endpoint listening", "addr", httpAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("health server failed", "error", err)
			os.Exit(1)
		}
	}()

	// Shared across every "http-in" node (CON-300): one runtime-wide
	// listener rather than one per node.
	webhookServer := &http.Server{Addr: webhookAddr, Handler: webhook.DefaultRegistry}
	go func() {
		slog.Info("runtime webhook endpoint listening", "addr", webhookAddr)
		if err := webhookServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("webhook server failed", "error", err)
			os.Exit(1)
		}
	}()

	go func() {
		err := runtimeclient.Run(ctx, controlPlaneAddr, runtimeID, version, enrollmentToken, healthSrv.SetReady, applyDeploy(deployment, logLevelVar), debugSink, deployment, connResolver, eventSink, deployment, healthProvider)
		if err != nil && ctx.Err() == nil {
			slog.Error("runtime client stopped unexpectedly", "error", err)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down runtime")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpServer.Shutdown(shutdownCtx)
	_ = webhookServer.Shutdown(shutdownCtx)
}

// applyDeploy parses and applies one pushed flow onto deployment, hot-
// swapping only the affected nodes (ENG-140). Errors are logged, not
// fatal — a bad deploy push must never take the runtime down (ARC-150).
// logLevelVar is OBS-120's per-flow log level: resending the SAME
// flowId/version/flowJSON with a different logLevel string re-applies
// deployment.Deploy (a no-op restart-wise, per ENG-140 fingerprinting)
// purely to pick up the new verbosity.
func applyDeploy(deployment *flow.Deployment, logLevelVar *slog.LevelVar) runtimeclient.DeployHandler {
	return func(ctx context.Context, flowID string, ver int64, flowJSON, defaultErrorFlow, logLevel string, resolvedEnv map[string]string) {
		ff, err := flow.Parse([]byte(flowJSON))
		if err != nil {
			slog.Error("received undeployable flow: parse failed", "flowId", flowID, "version", ver, "error", err)
			return
		}
		// The control plane is the authority on "which flow this is" for
		// every REST route and WebSocket subscription (its own row id, not
		// necessarily the flow file's own "id" field, which is author-chosen
		// and has no reason to match). Deployment tags every debug/execution
		// event it reports with FlowFile.ID, so that has to be the
		// control-plane id too, or DBG-100/DBG-140 subscriptions and
		// GET .../executions would key against a value nothing ever reports.
		ff.ID = flowID
		deployment.SetDefaultErrorFlow(defaultErrorFlow)
		deployment.SetResolvedEnv(resolvedEnv)
		logLevelVar.Set(parseLogLevel(logLevel))
		if err := deployment.Deploy(ctx, ff); err != nil {
			slog.Error("deploy failed", "flowId", flowID, "version", ver, "error", err)
			return
		}
		slog.Info("flow deployed", "flowId", flowID, "version", ver, "logLevel", logLevel)
	}
}

// parseLogLevel maps OBS-120's per-flow log-level string onto slog.Level,
// defaulting to Info for "" or anything unrecognized rather than failing a
// deploy over a cosmetic setting.
func parseLogLevel(s string) slog.Level {
	switch s {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
