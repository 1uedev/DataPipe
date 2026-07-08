// Package runtimeclient implements the runtime side of the registration
// protocol (ARC-210, ADR-007): the runtime dials the control plane, keeps
// itself registered for as long as the process runs, and applies deploy
// commands pushed down the DeployStream (Increment 3 deploy orchestration).
package runtimeclient

import (
	"context"
	"log/slog"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

const heartbeatInterval = 10 * time.Second
const deployStreamRetryDelay = 2 * time.Second

// DeployHandler applies one pushed flow deployment. defaultErrorFlow is the
// owning project's ERR-120 fallback error-handler flow id (Increment 8),
// "" if none is configured. logLevel is OBS-120's per-flow log level
// ("debug"|"info"|"warn"|"error", "" meaning "info").
type DeployHandler func(ctx context.Context, flowID string, version int64, flowJSON, defaultErrorFlow, logLevel string)

// FlowStatus is one deployed flow's coarse health, reported on every
// Heartbeat (Increment 9, EDGE-120 "flow status").
type FlowStatus struct {
	FlowID string
	Status string // "running" | "error"
}

// HealthSnapshot is this runtime's fleet health at one point in time
// (Increment 9, EDGE-120 "inventory with health (online, CPU, memory, flow
// status, versions)"), sampled fresh on every Heartbeat.
type HealthSnapshot struct {
	CPUPercent   float64
	MemoryBytes  uint64
	FlowStatuses []FlowStatus
}

// HealthProvider samples the current HealthSnapshot. May be nil to send
// heartbeats with no health payload (kind, id, and liveness are still
// tracked control-plane-side either way).
type HealthProvider func() HealthSnapshot

// Run dials addr and keeps the runtime registered until ctx is cancelled.
// onRegistered is invoked (with the current registration state) every time
// registration succeeds or is lost, so callers can drive a health endpoint;
// onDeploy is invoked for every DeployCommand received while registered.
// debugSink and rb may be nil to opt out of the live-debugging channel
// entirely (Increment 5, DBG-100/110/120/170); connResolver may be nil to
// opt out of connection resolution (Increment 6, CON-110) — nodes that
// reference a connection will simply fail to resolve it. eventSink and
// target may be nil to opt out of the execution/dead-letter channel
// entirely (Increment 8, ENG-130/DBG-140/ERR-130) — nothing is tracked or
// re-runnable in that case. enrollmentToken authenticates this runtime as
// a managed fleet device (Increment 9, EDGE-120/ARC-210) — "" for the
// walking-skeleton no-token local/dev setup. healthProvider may be nil to
// send heartbeats with no health payload.
func Run(ctx context.Context, addr, runtimeID, version, enrollmentToken string, onRegistered func(bool), onDeploy DeployHandler, debugSink *DebugSink, rb RingBufferSource, connResolver *ConnectionResolver, eventSink *EventSink, target DeploymentTarget, healthProvider HealthProvider) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	client := runtimev1.NewRuntimeRegistryServiceClient(conn)
	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		sessionToken, err := register(ctx, client, runtimeID, version, enrollmentToken)
		if err != nil {
			onRegistered(false)
			slog.Warn("runtime registration failed, retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(bo.Next()):
				continue
			}
		}
		bo.Reset()
		onRegistered(true)
		slog.Info("runtime registered", "runtime_id", runtimeID)
		if connResolver != nil {
			connResolver.attach(client, runtimeID, sessionToken)
		}

		streamCtx, cancelStreams := context.WithCancel(ctx)
		go deployStreamLoop(streamCtx, client, runtimeID, sessionToken, onDeploy)
		if debugSink != nil {
			go debugChannelLoop(streamCtx, client, runtimeID, sessionToken, debugSink, rb)
		}
		if eventSink != nil {
			go eventChannelLoop(streamCtx, client, runtimeID, sessionToken, eventSink, target)
		}

		if err := heartbeatLoop(ctx, client, runtimeID, sessionToken, healthProvider); err != nil {
			onRegistered(false)
			slog.Warn("heartbeat loop ended, will re-register", "error", err)
		}
		cancelStreams()
	}
}

func register(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, version, enrollmentToken string) (string, error) {
	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{
		RuntimeId:       runtimeID,
		Kind:            runtimev1.RuntimeKind_RUNTIME_KIND_SERVER,
		Version:         version,
		EnrollmentToken: enrollmentToken,
	})
	if err != nil {
		return "", err
	}
	return resp.GetSessionToken(), nil
}

func heartbeatLoop(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, sessionToken string, healthProvider HealthProvider) error {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			req := &runtimev1.HeartbeatRequest{
				RuntimeId:    runtimeID,
				SessionToken: sessionToken,
			}
			if healthProvider != nil {
				snap := healthProvider()
				req.CpuPercent = snap.CPUPercent
				req.MemoryBytes = snap.MemoryBytes
				for _, fs := range snap.FlowStatuses {
					req.FlowStatuses = append(req.FlowStatuses, &runtimev1.FlowStatus{FlowId: fs.FlowID, Status: fs.Status})
				}
			}
			if _, err := client.Heartbeat(ctx, req); err != nil {
				return err
			}
		}
	}
}

// deployStreamLoop keeps a DeployStream open, reconnecting on failure,
// until ctx is cancelled (which happens when the heartbeat loop needs to
// re-register).
func deployStreamLoop(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, sessionToken string, onDeploy DeployHandler) {
	for ctx.Err() == nil {
		stream, err := client.DeployStream(ctx, &runtimev1.DeployStreamRequest{RuntimeId: runtimeID, SessionToken: sessionToken})
		if err != nil {
			slog.Warn("opening deploy stream failed, retrying", "error", err)
			if !sleepOrDone(ctx, deployStreamRetryDelay) {
				return
			}
			continue
		}

		for {
			cmd, err := stream.Recv()
			if err != nil {
				if ctx.Err() == nil {
					slog.Warn("deploy stream ended, reconnecting", "error", err)
				}
				break
			}
			if onDeploy != nil {
				onDeploy(ctx, cmd.GetFlowId(), cmd.GetVersion(), cmd.GetFlowJson(), cmd.GetDefaultErrorFlow(), cmd.GetLogLevel())
			}
		}

		if !sleepOrDone(ctx, deployStreamRetryDelay) {
			return
		}
	}
}

// sleepOrDone waits for d or ctx cancellation, returning false in the
// latter case so callers can stop looping.
func sleepOrDone(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
