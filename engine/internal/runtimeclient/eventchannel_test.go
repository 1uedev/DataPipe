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

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

// fakeEventServer implements just enough of RuntimeRegistryServiceServer to
// exercise the runtime-side EventChannel loop: it records every uplink
// request and, if downlink is set, pushes it down immediately after the
// hello message.
type fakeEventServer struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer

	downlink *runtimev1.EventChannelResponse

	mu       sync.Mutex
	received []*runtimev1.EventChannelRequest
}

func (s *fakeEventServer) EventChannel(stream runtimev1.RuntimeRegistryService_EventChannelServer) error {
	if s.downlink != nil {
		if err := stream.Send(s.downlink); err != nil {
			return err
		}
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

func (s *fakeEventServer) snapshot() []*runtimev1.EventChannelRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*runtimev1.EventChannelRequest(nil), s.received...)
}

func startFakeEventServer(t *testing.T, downlink *runtimev1.EventChannelResponse) (runtimev1.RuntimeRegistryServiceClient, *fakeEventServer, func()) {
	t.Helper()
	srv := &fakeEventServer{downlink: downlink}
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

// fakeDeploymentTarget records the last call to each DeploymentTarget
// method, for asserting downlink commands were applied correctly.
type fakeDeploymentTarget struct {
	mu sync.Mutex

	replayOutputCalled bool
	replayInputCalled  bool
	nodeID, port       string
	reRunOf            string

	cancelledExecutionID string

	reinjectedNodeID, reinjectedPort string
}

func (f *fakeDeploymentTarget) ReplayOutput(ctx context.Context, nodeID, port string, seed datagram.Datagram, reRunOf string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replayOutputCalled, f.nodeID, f.port, f.reRunOf = true, nodeID, port, reRunOf
	return "new-exec-id", nil
}

func (f *fakeDeploymentTarget) ReplayInput(ctx context.Context, nodeID, port string, seed datagram.Datagram, reRunOf string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replayInputCalled, f.nodeID, f.port, f.reRunOf = true, nodeID, port, reRunOf
	return "new-exec-id", nil
}

func (f *fakeDeploymentTarget) ReinjectDeadLetter(ctx context.Context, nodeID, port string, d datagram.Datagram) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reinjectedNodeID, f.reinjectedPort = nodeID, port
	return nil
}

func (f *fakeDeploymentTarget) CancelExecution(executionID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelledExecutionID = executionID
	return true
}

// deploymentTargetSnapshot is a plain-value copy of fakeDeploymentTarget's
// fields, safe to return from snapshot() without copying the mutex itself
// (copying a locked sync.Mutex is a bug the race detector — correctly —
// flags).
type deploymentTargetSnapshot struct {
	replayOutputCalled, replayInputCalled bool
	nodeID, port, reRunOf                 string
	cancelledExecutionID                  string
	reinjectedNodeID, reinjectedPort      string
}

func (f *fakeDeploymentTarget) snapshot() deploymentTargetSnapshot {
	f.mu.Lock()
	defer f.mu.Unlock()
	return deploymentTargetSnapshot{
		replayOutputCalled: f.replayOutputCalled, replayInputCalled: f.replayInputCalled,
		nodeID: f.nodeID, port: f.port, reRunOf: f.reRunOf,
		cancelledExecutionID: f.cancelledExecutionID,
		reinjectedNodeID:     f.reinjectedNodeID, reinjectedPort: f.reinjectedPort,
	}
}

func TestENG130_EventChannelForwardsExecutionAndDeadLetterEvents(t *testing.T) {
	client, srv, cleanup := startFakeEventServer(t, nil)
	defer cleanup()

	sink := NewEventSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eventChannelLoop(ctx, client, "rt-1", "tok-1", sink, nil)

	waitFor(t, 2*time.Second, func() bool { return len(srv.snapshot()) >= 1 })

	seed := datagram.New(datagram.Source{NodeID: "trig"}, datagram.Payload{Value: 1})
	sink.Started("flow-1", "exec-1", "trig", "webhook", "", time.Now(), seed)
	sink.NodeEvent("flow-1", "exec-1", flow.NodeIO{NodeID: "n1", Input: seed})
	sink.Finished("flow-1", "exec-1", flow.ExecutionSuccess, time.Now(), "")
	sink.Capture("flow-1", "n1", "in", "node_error", seed, time.Now())

	waitFor(t, 2*time.Second, func() bool { return len(srv.snapshot()) >= 5 })

	reqs := srv.snapshot()
	// reqs[0] is the hello.
	if reqs[1].GetExecutionEvent().GetPhase() != "started" {
		t.Fatalf("reqs[1] phase = %q, want \"started\"", reqs[1].GetExecutionEvent().GetPhase())
	}
	if reqs[2].GetExecutionEvent().GetPhase() != "node" {
		t.Fatalf("reqs[2] phase = %q, want \"node\"", reqs[2].GetExecutionEvent().GetPhase())
	}
	if reqs[3].GetExecutionEvent().GetPhase() != "finished" {
		t.Fatalf("reqs[3] phase = %q, want \"finished\"", reqs[3].GetExecutionEvent().GetPhase())
	}
	if reqs[4].GetDeadLetterEvent() == nil {
		t.Fatalf("reqs[4] = %+v, want a dead-letter event", reqs[4])
	}
}

func TestENG130_EventChannelAppliesRunExecutionDownlink(t *testing.T) {
	seedJSON := `{"header":{"id":"x","correlationId":"x"},"payload":{"value":1}}`
	downlink := &runtimev1.EventChannelResponse{
		Payload: &runtimev1.EventChannelResponse_RunExecution{RunExecution: &runtimev1.RunExecution{
			FlowId: "flow-1", From: "node", NodeId: "n1", Port: "in", DatagramJson: seedJSON, ReRunOf: "exec-orig",
		}},
	}
	client, _, cleanup := startFakeEventServer(t, downlink)
	defer cleanup()

	target := &fakeDeploymentTarget{}
	sink := NewEventSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eventChannelLoop(ctx, client, "rt-1", "tok-1", sink, target)

	waitFor(t, 2*time.Second, func() bool { return target.snapshot().replayInputCalled })
	got := target.snapshot()
	if got.nodeID != "n1" || got.port != "in" || got.reRunOf != "exec-orig" {
		t.Fatalf("target = %+v, want nodeID=n1 port=in reRunOf=exec-orig", got)
	}
}

func TestENG130_EventChannelAppliesCancelExecutionDownlink(t *testing.T) {
	downlink := &runtimev1.EventChannelResponse{
		Payload: &runtimev1.EventChannelResponse_CancelExecution{CancelExecution: &runtimev1.CancelExecution{ExecutionId: "exec-1"}},
	}
	client, _, cleanup := startFakeEventServer(t, downlink)
	defer cleanup()

	target := &fakeDeploymentTarget{}
	sink := NewEventSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eventChannelLoop(ctx, client, "rt-1", "tok-1", sink, target)

	waitFor(t, 2*time.Second, func() bool { return target.snapshot().cancelledExecutionID == "exec-1" })
}

func TestERR130_EventChannelAppliesReinjectDeadLetterDownlink(t *testing.T) {
	downlink := &runtimev1.EventChannelResponse{
		Payload: &runtimev1.EventChannelResponse_ReinjectDeadLetter{ReinjectDeadLetter: &runtimev1.ReinjectDeadLetter{
			FlowId: "flow-1", NodeId: "n2", Port: "in", DatagramJson: `{"header":{"id":"x","correlationId":"x"},"payload":{"value":1}}`,
		}},
	}
	client, _, cleanup := startFakeEventServer(t, downlink)
	defer cleanup()

	target := &fakeDeploymentTarget{}
	sink := NewEventSink()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go eventChannelLoop(ctx, client, "rt-1", "tok-1", sink, target)

	waitFor(t, 2*time.Second, func() bool { return target.snapshot().reinjectedNodeID == "n2" })
}
