package runtimeclient

import (
	"context"
	"net"
	"testing"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type fakeResolveServer struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer
	lastReq *runtimev1.ResolveConnectionRequest
	resp    *runtimev1.ResolveConnectionResponse
	err     error
}

func (s *fakeResolveServer) ResolveConnection(_ context.Context, req *runtimev1.ResolveConnectionRequest) (*runtimev1.ResolveConnectionResponse, error) {
	s.lastReq = req
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

func startFakeResolveServer(t *testing.T, srv *fakeResolveServer) (runtimev1.RuntimeRegistryServiceClient, func()) {
	t.Helper()
	grpcServer := grpc.NewServer()
	runtimev1.RegisterRuntimeRegistryServiceServer(grpcServer, srv)

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
	return client, func() {
		_ = conn.Close()
		grpcServer.Stop()
	}
}

func TestCON110_ConnectionResolverNotAttachedErrors(t *testing.T) {
	r := NewConnectionResolver()
	if _, err := r.ResolveConnection(context.Background(), "conn-1"); err == nil {
		t.Fatal("expected an error before attach() has ever run")
	}
}

func TestCON110_ConnectionResolverCallsResolveConnectionRPC(t *testing.T) {
	srv := &fakeResolveServer{resp: &runtimev1.ResolveConnectionResponse{
		Type:           "mqtt",
		ConfigJson:     `{"broker":"tcp://localhost:1883"}`,
		CredentialJson: `{"username":"u","password":"p"}`,
	}}
	client, cleanup := startFakeResolveServer(t, srv)
	defer cleanup()

	r := NewConnectionResolver()
	r.attach(client, "rt-1", "tok-1")

	info, err := r.ResolveConnection(context.Background(), "conn-42")
	if err != nil {
		t.Fatalf("ResolveConnection: %v", err)
	}
	if info.Type != "mqtt" {
		t.Errorf("Type = %q, want mqtt", info.Type)
	}
	if string(info.Config) != `{"broker":"tcp://localhost:1883"}` {
		t.Errorf("Config = %s", info.Config)
	}
	if string(info.CredentialJSON) != `{"username":"u","password":"p"}` {
		t.Errorf("CredentialJSON = %s", info.CredentialJSON)
	}
	if srv.lastReq.GetRuntimeId() != "rt-1" || srv.lastReq.GetSessionToken() != "tok-1" || srv.lastReq.GetConnectionId() != "conn-42" {
		t.Errorf("request sent = %+v", srv.lastReq)
	}
}

func TestCON110_ConnectionResolverNoCredentialLeavesNilJSON(t *testing.T) {
	srv := &fakeResolveServer{resp: &runtimev1.ResolveConnectionResponse{
		Type: "schedule", ConfigJson: `{}`, CredentialJson: "",
	}}
	client, cleanup := startFakeResolveServer(t, srv)
	defer cleanup()

	r := NewConnectionResolver()
	r.attach(client, "rt-1", "tok-1")

	info, err := r.ResolveConnection(context.Background(), "conn-no-cred")
	if err != nil {
		t.Fatalf("ResolveConnection: %v", err)
	}
	if info.CredentialJSON != nil {
		t.Errorf("CredentialJSON = %q, want nil", info.CredentialJSON)
	}
}

func TestCON110_ConnectionResolverPropagatesRPCError(t *testing.T) {
	srv := &fakeResolveServer{err: context.DeadlineExceeded}
	client, cleanup := startFakeResolveServer(t, srv)
	defer cleanup()

	r := NewConnectionResolver()
	r.attach(client, "rt-1", "tok-1")

	if _, err := r.ResolveConnection(context.Background(), "conn-1"); err == nil {
		t.Fatal("expected the RPC error to propagate")
	}
}
