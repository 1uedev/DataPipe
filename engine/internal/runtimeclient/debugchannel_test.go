package runtimeclient

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/1uedev/DataPipe/engine/flow"
)

// fakeDebugServer implements just enough of RuntimeRegistryServiceServer to
// exercise the runtime-side DebugChannel loop: it immediately subscribes
// the client to "f1" (simulating a browser opening f1's inspector), and
// records every DebugChannelRequest it receives.
type fakeDebugServer struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer

	mu       sync.Mutex
	received []*runtimev1.DebugChannelRequest
}

func (s *fakeDebugServer) DebugChannel(stream runtimev1.RuntimeRegistryService_DebugChannelServer) error {
	if err := stream.Send(&runtimev1.DebugChannelResponse{
		Payload: &runtimev1.DebugChannelResponse_Subscribe{Subscribe: &runtimev1.SubscribeFlow{FlowId: "f1"}},
	}); err != nil {
		return err
	}
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		s.mu.Lock()
		s.received = append(s.received, req)
		s.mu.Unlock()
	}
}

func (s *fakeDebugServer) snapshot() []*runtimev1.DebugChannelRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*runtimev1.DebugChannelRequest(nil), s.received...)
}

type fakeRingBufferSource struct{ events map[string][]flow.DebugEvent }

func (f fakeRingBufferSource) FlowDebugSnapshot(flowID string) []flow.DebugEvent {
	return f.events[flowID]
}

func startFakeDebugServer(t *testing.T) (runtimev1.RuntimeRegistryServiceClient, *fakeDebugServer, func()) {
	t.Helper()
	srv := &fakeDebugServer{}
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

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
	}
	return client, srv, cleanup
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

// TestDBG100_170_DebugChannelReplaysRingBufferOnSubscribeAndGatesLiveEvents
// proves: (1) a Subscribe downlink immediately triggers a ring-buffer replay
// (DBG-100: "inspection works without redeploy"), and (2) live Capture calls
// only reach the wire for flows that are actually subscribed (DBG-170:
// "sampled/rate-limited to protect the runtime" — here, gated entirely).
func TestDBG100_170_DebugChannelReplaysRingBufferOnSubscribeAndGatesLiveEvents(t *testing.T) {
	client, srv, cleanup := startFakeDebugServer(t)
	defer cleanup()

	rb := fakeRingBufferSource{events: map[string][]flow.DebugEvent{
		"f1": {{ID: "hist-1", FlowID: "f1", NodeID: "n1", Direction: flow.DirOut}},
	}}
	sink := NewDebugSink()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go debugChannelLoop(ctx, client, "rt-1", "tok-1", sink, rb)

	// The runtime announces itself (an empty "hello", so the hub has
	// someone to send Subscribe/Unsubscribe downlinks to even before any
	// other uplink traffic exists) as its very first message, then the fake
	// server's immediate Subscribe("f1") triggers a replay of "f1"'s
	// ring-buffer history.
	waitFor(t, 2*time.Second, func() bool { return len(srv.snapshot()) >= 2 })
	hello := srv.snapshot()[0]
	if hello.GetEvent() != nil || hello.GetWireMetrics() != nil {
		t.Fatalf("expected an empty hello message first, got %+v", hello)
	}
	replayed := srv.snapshot()[1].GetEvent()
	if replayed.GetId() != "hist-1" {
		t.Fatalf("expected the replayed ring-buffer event second, got %+v", replayed)
	}

	// A live event on the subscribed flow reaches the server.
	sink.Capture(flow.DebugEvent{ID: "live-1", FlowID: "f1", NodeID: "n1"})
	waitFor(t, 2*time.Second, func() bool { return len(srv.snapshot()) >= 3 })

	// A live event on an *unsubscribed* flow never reaches the server
	// (DBG-170's runtime-side gating, not just client-side filtering).
	sink.Capture(flow.DebugEvent{ID: "live-2", FlowID: "f2", NodeID: "n2"})
	time.Sleep(200 * time.Millisecond)
	for _, req := range srv.snapshot() {
		if req.GetEvent().GetId() == "live-2" {
			t.Fatal("event for an unsubscribed flow reached the wire")
		}
	}
}
