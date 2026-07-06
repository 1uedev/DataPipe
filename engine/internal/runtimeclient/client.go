// Package runtimeclient implements the runtime side of the registration
// protocol (ARC-210, ADR-007): the runtime dials the control plane and
// keeps itself registered for as long as the process runs.
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

// Run dials addr and keeps the runtime registered until ctx is cancelled.
// onRegistered is invoked (with the current registration state) every time
// registration succeeds or is lost, so callers can drive a health endpoint.
func Run(ctx context.Context, addr, runtimeID, version string, onRegistered func(bool)) error {
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

		if err := heartbeatLoop(ctx, client, runtimeID, sessionToken); err != nil {
			onRegistered(false)
			slog.Warn("heartbeat loop ended, will re-register", "error", err)
		}
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
