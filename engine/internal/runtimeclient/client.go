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

// DeployHandler applies one pushed flow deployment.
type DeployHandler func(ctx context.Context, flowID string, version int64, flowJSON string)

// Run dials addr and keeps the runtime registered until ctx is cancelled.
// onRegistered is invoked (with the current registration state) every time
// registration succeeds or is lost, so callers can drive a health endpoint;
// onDeploy is invoked for every DeployCommand received while registered.
func Run(ctx context.Context, addr, runtimeID, version string, onRegistered func(bool), onDeploy DeployHandler) error {
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

		sessionToken, err := register(ctx, client, runtimeID, version)
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

		deployCtx, cancelDeploy := context.WithCancel(ctx)
		go deployStreamLoop(deployCtx, client, runtimeID, sessionToken, onDeploy)

		if err := heartbeatLoop(ctx, client, runtimeID, sessionToken); err != nil {
			onRegistered(false)
			slog.Warn("heartbeat loop ended, will re-register", "error", err)
		}
		cancelDeploy()
	}
}

func register(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, version string) (string, error) {
	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{
		RuntimeId: runtimeID,
		Kind:      runtimev1.RuntimeKind_RUNTIME_KIND_SERVER,
		Version:   version,
	})
	if err != nil {
		return "", err
	}
	return resp.GetSessionToken(), nil
}

func heartbeatLoop(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, sessionToken string) error {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := client.Heartbeat(ctx, &runtimev1.HeartbeatRequest{
				RuntimeId:    runtimeID,
				SessionToken: sessionToken,
			}); err != nil {
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
				onDeploy(ctx, cmd.GetFlowId(), cmd.GetVersion(), cmd.GetFlowJson())
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
