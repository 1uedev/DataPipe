package registry

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
)

type fakeConnResolver struct {
	got  string
	info ConnectionInfo
	err  error
}

func (f *fakeConnResolver) ResolveConnection(_ context.Context, connectionID string) (ConnectionInfo, error) {
	f.got = connectionID
	return f.info, f.err
}

func TestCON110_ResolveConnectionRPCRequiresValidSession(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	svc.SetConnectionResolver(&fakeConnResolver{})

	ctx := context.Background()
	reg, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	if _, err := client.ResolveConnection(ctx, &runtimev1.ResolveConnectionRequest{
		RuntimeId: "rt-1", SessionToken: "wrong-token", ConnectionId: "conn-1",
	}); err == nil {
		t.Fatal("expected an error for a wrong session token")
	}

	if _, err := client.ResolveConnection(ctx, &runtimev1.ResolveConnectionRequest{
		RuntimeId: "rt-1", SessionToken: reg.GetSessionToken(), ConnectionId: "conn-1",
	}); err != nil {
		t.Fatalf("ResolveConnection with a valid session: %v", err)
	}
}

func TestCON110_ResolveConnectionRPCCallsThroughToResolverAndBack(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()

	resolver := &fakeConnResolver{info: ConnectionInfo{
		Type:           "mqtt",
		ConfigJSON:     json.RawMessage(`{"broker":"tcp://localhost:1883"}`),
		CredentialJSON: json.RawMessage(`{"username":"u"}`),
	}}
	svc.SetConnectionResolver(resolver)

	ctx := context.Background()
	reg, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}

	resp, err := client.ResolveConnection(ctx, &runtimev1.ResolveConnectionRequest{
		RuntimeId: "rt-1", SessionToken: reg.GetSessionToken(), ConnectionId: "conn-42",
	})
	if err != nil {
		t.Fatalf("ResolveConnection: %v", err)
	}
	if resolver.got != "conn-42" {
		t.Errorf("resolver was asked for %q, want conn-42", resolver.got)
	}
	if resp.GetType() != "mqtt" {
		t.Errorf("Type = %q, want mqtt", resp.GetType())
	}
	if resp.GetConfigJson() != `{"broker":"tcp://localhost:1883"}` {
		t.Errorf("ConfigJson = %s", resp.GetConfigJson())
	}
	if resp.GetCredentialJson() != `{"username":"u"}` {
		t.Errorf("CredentialJson = %s", resp.GetCredentialJson())
	}
}

func TestCON110_ResolveConnectionRPCWithoutResolverConfiguredErrors(t *testing.T) {
	client, _, cleanup := startTestServer(t)
	defer cleanup()

	ctx := context.Background()
	reg, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := client.ResolveConnection(ctx, &runtimev1.ResolveConnectionRequest{
		RuntimeId: "rt-1", SessionToken: reg.GetSessionToken(), ConnectionId: "conn-1",
	}); err == nil {
		t.Fatal("expected an error when no resolver is configured")
	}
}

func TestCON110_ResolveConnectionRPCPropagatesResolverError(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	svc.SetConnectionResolver(&fakeConnResolver{err: errors.New("connection not found")})

	ctx := context.Background()
	reg, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: "rt-1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if _, err := client.ResolveConnection(ctx, &runtimev1.ResolveConnectionRequest{
		RuntimeId: "rt-1", SessionToken: reg.GetSessionToken(), ConnectionId: "conn-missing",
	}); err == nil {
		t.Fatal("expected the resolver's error to propagate")
	}
}
