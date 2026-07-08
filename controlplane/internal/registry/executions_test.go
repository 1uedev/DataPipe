package registry

import (
	"context"
	"sync"
	"testing"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
)

// fakeExecutionStore records every call, for asserting the EventChannel
// handler translates uplink proto messages correctly and marks a
// disconnected runtime's executions crashed (ERR-150).
type fakeExecutionStore struct {
	mu               sync.Mutex
	events           []ExecutionEvent
	deadLetters      []DeadLetterEvent
	crashedRuntimeID string
	crashedCalls     int
}

func (f *fakeExecutionStore) RecordExecutionEvent(ctx context.Context, runtimeID string, ev ExecutionEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	return nil
}

func (f *fakeExecutionStore) RecordDeadLetter(ctx context.Context, runtimeID string, ev DeadLetterEvent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deadLetters = append(f.deadLetters, ev)
	return nil
}

func (f *fakeExecutionStore) MarkRuntimeExecutionsCrashed(ctx context.Context, runtimeID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.crashedRuntimeID = runtimeID
	f.crashedCalls++
	return nil
}

func (f *fakeExecutionStore) snapshot() (events []ExecutionEvent, deadLetters []DeadLetterEvent, crashedRuntimeID string, crashedCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ExecutionEvent(nil), f.events...), append([]DeadLetterEvent(nil), f.deadLetters...), f.crashedRuntimeID, f.crashedCalls
}

func registerAndOpenEventChannel(t *testing.T, client runtimev1.RuntimeRegistryServiceClient, svc *Service, runtimeID string) (runtimev1.RuntimeRegistryService_EventChannelClient, string) {
	t.Helper()
	ctx := context.Background()
	resp, err := client.Register(ctx, &runtimev1.RegisterRequest{RuntimeId: runtimeID, Version: "0.0.1"})
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	stream, err := client.EventChannel(ctx)
	if err != nil {
		t.Fatalf("EventChannel: %v", err)
	}
	if err := stream.Send(&runtimev1.EventChannelRequest{RuntimeId: runtimeID, SessionToken: resp.GetSessionToken()}); err != nil {
		t.Fatalf("hello: %v", err)
	}
	// The client's Send returning only means the hello was handed to the
	// transport, not that the server's EventChannel RPC goroutine has
	// actually processed it and registered rt.eventCh yet — without this
	// wait, a caller proceeding straight to RunExecution/CancelExecution/
	// ReinjectDeadLetter races the server's own registration and
	// intermittently (near-certainly under -race's slower scheduling)
	// sees "no runtime currently connected".
	waitFor(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		rt, ok := svc.runtimes[runtimeID]
		return ok && rt.eventCh != nil
	}, 2*time.Second)
	return stream, resp.GetSessionToken()
}

func TestENG130_EventChannelPersistsExecutionEventViaStore(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	store := &fakeExecutionStore{}
	svc.SetExecutionStore(store)

	stream, _ := registerAndOpenEventChannel(t, client, svc, "rt-1")
	if err := stream.Send(&runtimev1.EventChannelRequest{
		RuntimeId: "rt-1",
		Payload: &runtimev1.EventChannelRequest_ExecutionEvent{ExecutionEvent: &runtimev1.ExecutionEvent{
			ExecutionId: "exec-1", FlowId: "flow-1", Phase: "started", TriggerNodeId: "trig",
		}},
	}); err != nil {
		t.Fatalf("send execution event: %v", err)
	}

	waitFor(t, func() bool {
		events, _, _, _ := store.snapshot()
		return len(events) == 1
	}, 2*time.Second)
	events, _, _, _ := store.snapshot()
	if events[0].ExecutionID != "exec-1" || events[0].TriggerNodeID != "trig" {
		t.Fatalf("recorded event = %+v, want exec-1/trig", events[0])
	}
}

func TestERR130_EventChannelPersistsDeadLetterViaStore(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	store := &fakeExecutionStore{}
	svc.SetExecutionStore(store)

	stream, _ := registerAndOpenEventChannel(t, client, svc, "rt-1")
	if err := stream.Send(&runtimev1.EventChannelRequest{
		RuntimeId: "rt-1",
		Payload: &runtimev1.EventChannelRequest_DeadLetterEvent{DeadLetterEvent: &runtimev1.DeadLetterEvent{
			FlowId: "flow-1", NodeId: "n1", Reason: "node_error",
		}},
	}); err != nil {
		t.Fatalf("send dead letter event: %v", err)
	}

	waitFor(t, func() bool {
		_, dls, _, _ := store.snapshot()
		return len(dls) == 1
	}, 2*time.Second)
	_, dls, _, _ := store.snapshot()
	if dls[0].NodeID != "n1" || dls[0].Reason != "node_error" {
		t.Fatalf("recorded dead letter = %+v, want n1/node_error", dls[0])
	}
}

func TestERR150_EventChannelDisconnectMarksRuntimeExecutionsCrashed(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()
	store := &fakeExecutionStore{}
	svc.SetExecutionStore(store)

	stream, _ := registerAndOpenEventChannel(t, client, svc, "rt-1")
	// Send one message so the server has attached the runtime, then close
	// the stream to simulate a crash/disconnect.
	if err := stream.Send(&runtimev1.EventChannelRequest{
		RuntimeId: "rt-1",
		Payload: &runtimev1.EventChannelRequest_ExecutionEvent{ExecutionEvent: &runtimev1.ExecutionEvent{
			ExecutionId: "exec-1", FlowId: "flow-1", Phase: "started",
		}},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	waitFor(t, func() bool {
		events, _, _, _ := store.snapshot()
		return len(events) == 1
	}, 2*time.Second)

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	waitFor(t, func() bool {
		_, _, _, calls := store.snapshot()
		return calls == 1
	}, 2*time.Second)
	_, _, crashedRuntimeID, _ := store.snapshot()
	if crashedRuntimeID != "rt-1" {
		t.Fatalf("crashedRuntimeID = %q, want rt-1", crashedRuntimeID)
	}
}

func TestENG130_RunExecutionCancelExecutionReinjectDeadLetterBroadcast(t *testing.T) {
	client, svc, cleanup := startTestServer(t)
	defer cleanup()

	stream, _ := registerAndOpenEventChannel(t, client, svc, "rt-1")

	if err := svc.RunExecution(context.Background(), "flow-1", "start", "trig", "out", `{"payload":{"value":1}}`, ""); err != nil {
		t.Fatalf("RunExecution: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	re := resp.GetRunExecution()
	if re == nil || re.GetFlowId() != "flow-1" || re.GetNodeId() != "trig" {
		t.Fatalf("received = %+v, want a RunExecution for flow-1/trig", resp)
	}

	if err := svc.CancelExecution(context.Background(), "exec-1"); err != nil {
		t.Fatalf("CancelExecution: %v", err)
	}
	resp, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if resp.GetCancelExecution().GetExecutionId() != "exec-1" {
		t.Fatalf("received = %+v, want CancelExecution for exec-1", resp)
	}

	if err := svc.ReinjectDeadLetter(context.Background(), "flow-1", "n1", "in", `{"payload":{}}`); err != nil {
		t.Fatalf("ReinjectDeadLetter: %v", err)
	}
	resp, err = stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if resp.GetReinjectDeadLetter().GetNodeId() != "n1" {
		t.Fatalf("received = %+v, want ReinjectDeadLetter for n1", resp)
	}
}

func TestENG130_ExecutionCommandsFailWithNoRuntimeConnected(t *testing.T) {
	_, svc, cleanup := startTestServer(t)
	defer cleanup()

	if err := svc.RunExecution(context.Background(), "flow-1", "start", "trig", "out", "{}", ""); err == nil {
		t.Error("RunExecution with no connected runtime should return an error")
	}
	if err := svc.CancelExecution(context.Background(), "exec-1"); err == nil {
		t.Error("CancelExecution with no connected runtime should return an error")
	}
	if err := svc.ReinjectDeadLetter(context.Background(), "flow-1", "n1", "in", "{}"); err == nil {
		t.Error("ReinjectDeadLetter with no connected runtime should return an error")
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatal("condition not met within timeout")
	}
}
