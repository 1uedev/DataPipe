package registry

import (
	"context"
	"net"
	"testing"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestServer(t *testing.T) (runtimev1.RuntimeRegistryServiceClient, *Service, func()) {
	t.Helper()
	svc := NewService()
	grpcServer := grpc.NewServer()
	runtimev1.RegisterRuntimeRegistryServiceServer(grpcServer, svc)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = grpcServer.Serve(lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	client := runtimev1.NewRuntimeRegistryServiceClient(conn)

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
	}
	return client, svc, cleanup
}

func TestARC210_RegisterAndHeartbeat(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	ctx := context.Background()

	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1", Kind: runtimev1.RuntimeKind_RUNTIME_KIND_SERVER, Version: "0.0.1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if !resp.GetAccepted() || resp.GetSessionToken() == "" {
		t.Fatalf("Register response = %+v, want accepted with a session token", resp)
	}

	hb, err := client.Heartbeat(ctx, &runtimev1.HeartbeatRequest{RuntimeId: "rt-1", SessionToken: resp.GetSessionToken()})
	if err != nil || !hb.GetOk() {
		t.Fatalf("Heartbeat = %+v, %v, want ok", hb, err)
	}

	if svc.Count() != 1 {
		t.Errorf("Count() = %d, want 1", svc.Count())
	}
}

func TestARC210_HeartbeatRejectsWrongSessionToken(t *testing.T) {
	client, _, cleanup := startTestServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := client.Heartbeat(ctx, &runtimev1.HeartbeatRequest{RuntimeId: "rt-1", SessionToken: "wrong"}); err == nil {
		t.Error("Heartbeat with wrong session token should fail")
	}
}

func TestDeployFlow_NoRuntimeConnectedReturnsError(t *testing.T) {
	_, svc, cleanup := startTestServer(t)
	defer cleanup()

	if err := svc.DeployFlow(context.Background(), "flow-1", 1, "{}", "", ""); err == nil {
		t.Error("DeployFlow with no connected runtime should return an error")
	}
}

func TestDeployFlow_PushesToOpenDeployStream(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1", Version: "0.0.1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	stream, err := client.DeployStream(ctx, &runtimev1.DeployStreamRequest{RuntimeId: "rt-1", SessionToken: resp.GetSessionToken()})
	if err != nil {
		t.Fatalf("DeployStream: %v", err)
	}

	// Give the server a moment to register the stream before pushing.
	deployed := make(chan error, 1)
	go func() {
		time.Sleep(100 * time.Millisecond)
		deployed <- svc.DeployFlow(ctx, "flow-1", 3, `{"id":"flow-1"}`, "", "")
	}()

	cmd, err := stream.Recv()
	if err != nil {
		t.Fatalf("stream.Recv: %v", err)
	}
	if cmd.GetFlowId() != "flow-1" || cmd.GetVersion() != 3 || cmd.GetFlowJson() != `{"id":"flow-1"}` {
		t.Errorf("received command = %+v, want flow-1/3/{\"id\":\"flow-1\"}", cmd)
	}
	if err := <-deployed; err != nil {
		t.Errorf("DeployFlow: %v", err)
	}
}

func TestListRuntimes(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	ctx := context.Background()

	if _, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1", Kind: runtimev1.RuntimeKind_RUNTIME_KIND_EDGE, Version: "1.2.3"}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	snaps := svc.ListRuntimes(ctx)
	if len(snaps) != 1 {
		t.Fatalf("ListRuntimes() = %d entries, want 1", len(snaps))
	}
	if snaps[0].RuntimeID != "rt-1" || snaps[0].Kind != "edge" || snaps[0].Version != "1.2.3" {
		t.Errorf("snapshot = %+v, want rt-1/edge/1.2.3", snaps[0])
	}
}
