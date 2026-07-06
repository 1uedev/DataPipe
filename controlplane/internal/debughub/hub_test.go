package debughub

import (
	"context"
	"net"
	"testing"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// fakeRegistryServer wires a Hub up as the DebugChannel handler of a real
// gRPC server, so tests exercise the actual wire protocol rather than
// calling Hub methods directly against an in-process fake stream.
type fakeRegistryServer struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer
	hub *Hub
}

func (s *fakeRegistryServer) DebugChannel(stream runtimev1.RuntimeRegistryService_DebugChannelServer) error {
	return s.hub.Serve(stream)
}

func startTestHub(t *testing.T, validate func(string, string) bool) (*Hub, runtimev1.RuntimeRegistryServiceClient, func()) {
	t.Helper()
	hub := New(validate)
	grpcServer := grpc.NewServer()
	runtimev1.RegisterRuntimeRegistryServiceServer(grpcServer, &fakeRegistryServer{hub: hub})

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
	return hub, client, cleanup
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

func TestDBG170_HubOnlySubscribesRuntimeAfterFirstBrowserSubscriber(t *testing.T) {
	hub, client, cleanup := startTestHub(t, func(string, string) bool { return true })
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.DebugChannel(ctx)
	if err != nil {
		t.Fatalf("open debug channel: %v", err)
	}

	downlinks := make(chan *runtimev1.DebugChannelResponse, 16)
	go func() {
		for {
			resp, err := stream.Recv()
			if err != nil {
				close(downlinks)
				return
			}
			downlinks <- resp
		}
	}()

	// Send a first uplink to establish this stream's runtime identity.
	if err := stream.Send(&runtimev1.DebugChannelRequest{RuntimeId: "rt-1", SessionToken: "tok"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// Nobody has subscribed yet: no Subscribe downlink should arrive.
	select {
	case resp := <-downlinks:
		t.Fatalf("unexpected downlink before any browser subscribed: %+v", resp)
	case <-time.After(200 * time.Millisecond):
	}

	events, cancelSub := hub.Subscribe("flow-1")
	defer cancelSub()

	var got *runtimev1.DebugChannelResponse
	select {
	case got = <-downlinks:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a Subscribe downlink after the first browser subscriber")
	}
	sub := got.GetSubscribe()
	if sub == nil || sub.GetFlowId() != "flow-1" {
		t.Fatalf("expected Subscribe{flow-1}, got %+v", got)
	}

	// Now simulate the runtime forwarding a live event for that flow.
	if err := stream.Send(&runtimev1.DebugChannelRequest{
		RuntimeId: "rt-1", SessionToken: "tok",
		Payload: &runtimev1.DebugChannelRequest_Event{Event: &runtimev1.DebugEvent{
			Id: "e1", FlowId: "flow-1", NodeId: "n1", Direction: "out", ValueJson: `{"v":1}`,
		}},
	}); err != nil {
		t.Fatalf("send event: %v", err)
	}

	waitFor(t, 2*time.Second, func() bool {
		select {
		case item := <-events:
			return item.Event != nil && item.Event.ID == "e1"
		default:
			return false
		}
	})

	// Cancelling the last subscriber should trigger Unsubscribe.
	cancelSub()
	select {
	case got = <-downlinks:
	case <-time.After(2 * time.Second):
		t.Fatal("expected an Unsubscribe downlink after the last browser subscriber left")
	}
	if got.GetUnsubscribe() == nil || got.GetUnsubscribe().GetFlowId() != "flow-1" {
		t.Fatalf("expected Unsubscribe{flow-1}, got %+v", got)
	}
}

func TestDBG100_HubReplaysCacheToLateSubscriber(t *testing.T) {
	hub, client, cleanup := startTestHub(t, func(string, string) bool { return true })
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.DebugChannel(ctx)
	if err != nil {
		t.Fatalf("open debug channel: %v", err)
	}
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()
	if err := stream.Send(&runtimev1.DebugChannelRequest{RuntimeId: "rt-1", SessionToken: "tok"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	// First subscriber joins and receives a live event.
	firstEvents, cancelFirst := hub.Subscribe("flow-2")
	defer cancelFirst()
	if err := stream.Send(&runtimev1.DebugChannelRequest{
		RuntimeId: "rt-1", SessionToken: "tok",
		Payload: &runtimev1.DebugChannelRequest_Event{Event: &runtimev1.DebugEvent{
			Id: "hist-1", FlowId: "flow-2", NodeId: "n1", ValueJson: `1`,
		}},
	}); err != nil {
		t.Fatalf("send event: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		select {
		case item := <-firstEvents:
			return item.Event != nil && item.Event.ID == "hist-1"
		default:
			return false
		}
	})

	// A second, late-joining subscriber (no new runtime traffic in between)
	// must still see that history via the hub's own cache.
	lateEvents, cancelLate := hub.Subscribe("flow-2")
	defer cancelLate()
	select {
	case item := <-lateEvents:
		if item.Event == nil || item.Event.ID != "hist-1" {
			t.Fatalf("expected replayed cached event, got %+v", item)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("late subscriber did not receive cached history")
	}
}

func TestDBG110_HubTruncatesLargePayloadAndLoadFullReturnsOriginal(t *testing.T) {
	hub, client, cleanup := startTestHub(t, func(string, string) bool { return true })
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.DebugChannel(ctx)
	if err != nil {
		t.Fatalf("open debug channel: %v", err)
	}
	go func() {
		for {
			if _, err := stream.Recv(); err != nil {
				return
			}
		}
	}()
	if err := stream.Send(&runtimev1.DebugChannelRequest{RuntimeId: "rt-1", SessionToken: "tok"}); err != nil {
		t.Fatalf("send: %v", err)
	}

	events, cancelSub := hub.Subscribe("flow-3")
	defer cancelSub()

	big := make([]byte, MaxInlinePayloadBytes+500)
	for i := range big {
		big[i] = 'a'
	}
	if err := stream.Send(&runtimev1.DebugChannelRequest{
		RuntimeId: "rt-1", SessionToken: "tok",
		Payload: &runtimev1.DebugChannelRequest_Event{Event: &runtimev1.DebugEvent{
			Id: "big-1", FlowId: "flow-3", NodeId: "n1", ValueJson: string(big),
		}},
	}); err != nil {
		t.Fatalf("send event: %v", err)
	}

	var ev *Event
	waitFor(t, 2*time.Second, func() bool {
		select {
		case item := <-events:
			ev = item.Event
			return ev != nil && ev.ID == "big-1"
		default:
			return false
		}
	})
	if !ev.Truncated {
		t.Error("expected the inline event to be marked truncated")
	}
	if len(ev.ValueJSON) != MaxInlinePayloadBytes {
		t.Errorf("truncated ValueJSON length = %d, want %d", len(ev.ValueJSON), MaxInlinePayloadBytes)
	}
	if ev.FullLength != len(big) {
		t.Errorf("FullLength = %d, want %d", ev.FullLength, len(big))
	}

	full, ok := hub.LoadFull("flow-3", "big-1")
	if !ok {
		t.Fatal("LoadFull: not found")
	}
	if full != string(big) {
		t.Errorf("LoadFull returned truncated or wrong content (len=%d, want %d)", len(full), len(big))
	}

	if _, ok := hub.LoadFull("flow-3", "does-not-exist"); ok {
		t.Error("LoadFull should report false for an unknown event id")
	}
}

func TestDBG170_HubRejectsUnvalidatedRuntime(t *testing.T) {
	_, client, cleanup := startTestHub(t, func(runtimeID, token string) bool { return token == "good" })
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.DebugChannel(ctx)
	if err != nil {
		t.Fatalf("open debug channel: %v", err)
	}
	if err := stream.Send(&runtimev1.DebugChannelRequest{RuntimeId: "rt-1", SessionToken: "bad"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("expected the stream to be rejected for an invalid session")
	}
}
