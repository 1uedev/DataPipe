package flow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// recordingExecutionSink records every call for assertion, safe for
// concurrent use by Tracker.
type recordingExecutionSink struct {
	mu       sync.Mutex
	waiting  []string
	started  []string
	nodes    []NodeIO
	finished []finishedCall
}

type finishedCall struct {
	ExecutionID string
	Status      ExecutionStatus
	Reason      string
}

func (s *recordingExecutionSink) Waiting(flowID, executionID, triggerNodeID string, at time.Time, seed datagram.Datagram) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.waiting = append(s.waiting, executionID)
}

func (s *recordingExecutionSink) Started(flowID, executionID, triggerNodeID, triggerKind, reRunOf string, at time.Time, seed datagram.Datagram) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = append(s.started, executionID)
}

func (s *recordingExecutionSink) NodeEvent(flowID, executionID string, ev NodeIO) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes = append(s.nodes, ev)
}

func (s *recordingExecutionSink) Finished(flowID, executionID string, status ExecutionStatus, at time.Time, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finished = append(s.finished, finishedCall{executionID, status, reason})
}

func (s *recordingExecutionSink) snapshot() (waiting, started []string, nodes []NodeIO, finished []finishedCall) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.waiting...), append([]string(nil), s.started...), append([]NodeIO(nil), s.nodes...), append([]finishedCall(nil), s.finished...)
}

func rootDgm(v int) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "trigger"}, datagram.Payload{Value: v})
}

func TestENG130_StartAndSingleNodeFinishSucceeds(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(0, false, 0, sink)

	seed := rootDgm(1)
	execID, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if execID != seed.Header.CorrelationID {
		t.Fatalf("execution id = %q, want the seed's correlation id %q", execID, seed.Header.CorrelationID)
	}
	if !tracker.Tracking(execID) {
		t.Fatal("expected execution to be tracked after Start")
	}

	// A terminal node with zero outputs finishes the execution.
	tracker.NodeEvent("flow-1", seed, NodeIO{NodeID: "sink", Input: seed}, "")

	if tracker.Tracking(execID) {
		t.Fatal("expected execution to no longer be tracked once pending reaches zero")
	}
	_, started, _, finished := sink.snapshot()
	if len(started) != 1 || started[0] != execID {
		t.Fatalf("started = %v, want [%s]", started, execID)
	}
	if len(finished) != 1 || finished[0].Status != ExecutionSuccess {
		t.Fatalf("finished = %+v, want one success", finished)
	}
}

func TestENG130_FanOutKeepsExecutionRunningUntilAllBranchesFinish(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(0, false, 0, sink)

	seed := rootDgm(1)
	execID, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	child1 := datagram.NewCaused(seed, datagram.Source{}, datagram.Payload{Value: "a"})
	child2 := datagram.NewCaused(seed, datagram.Source{}, datagram.Payload{Value: "b"})
	// The trigger's own datagram fans out to two downstream results.
	tracker.NodeEvent("flow-1", seed, NodeIO{NodeID: "split", Outputs: []PortDatagram{{Port: "out", Datagram: child1}, {Port: "out", Datagram: child2}}}, "")
	if !tracker.Tracking(execID) {
		t.Fatal("execution should still be running: two branches are outstanding")
	}

	tracker.NodeEvent("flow-1", child1, NodeIO{NodeID: "sink"}, "")
	if !tracker.Tracking(execID) {
		t.Fatal("execution should still be running: one branch is still outstanding")
	}

	tracker.NodeEvent("flow-1", child2, NodeIO{NodeID: "sink"}, "")
	if tracker.Tracking(execID) {
		t.Fatal("execution should have finished once both branches completed")
	}
	_, _, _, finished := sink.snapshot()
	if len(finished) != 1 || finished[0].Status != ExecutionSuccess {
		t.Fatalf("finished = %+v, want one success", finished)
	}
}

func TestENG130_UnhandledFailMarksExecutionFailed(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(0, false, 0, sink)

	seed := rootDgm(1)
	if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tracker.NodeEvent("flow-1", seed, NodeIO{NodeID: "n1", Err: &NodeError{Message: "boom"}}, "fail")

	_, _, _, finished := sink.snapshot()
	if len(finished) != 1 || finished[0].Status != ExecutionFailed || finished[0].Reason != "boom" {
		t.Fatalf("finished = %+v, want one failed with reason \"boom\"", finished)
	}
}

func TestENG130_DiscardedErrorDoesNotFailExecution(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(0, false, 0, sink)

	seed := rootDgm(1)
	if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed); err != nil {
		t.Fatalf("Start: %v", err)
	}
	tracker.NodeEvent("flow-1", seed, NodeIO{NodeID: "n1", Err: &NodeError{Message: "meh"}}, "discard")

	_, _, _, finished := sink.snapshot()
	if len(finished) != 1 || finished[0].Status != ExecutionSuccess {
		t.Fatalf("finished = %+v, want one success (discard is a deliberate, acknowledged policy)", finished)
	}
}

func TestENG130_UntrackedDatagramIsNoop(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(0, false, 0, sink)

	// No Start call: this correlation id was never tracked (e.g. an
	// ordinary streaming-flow datagram).
	tracker.NodeEvent("flow-1", rootDgm(1), NodeIO{NodeID: "n1"}, "")

	_, _, nodes, finished := sink.snapshot()
	if len(nodes) != 0 || len(finished) != 0 {
		t.Fatalf("expected no sink activity for an untracked datagram, got nodes=%v finished=%v", nodes, finished)
	}
}

func TestENG130_ConcurrencyRejectPolicyRejectsOverLimit(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(1, true, 0, sink)

	seed1 := rootDgm(1)
	if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed1); err != nil {
		t.Fatalf("first Start: %v", err)
	}

	seed2 := rootDgm(2)
	_, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed2)
	if err != ErrConcurrencyRejected {
		t.Fatalf("second Start error = %v, want ErrConcurrencyRejected", err)
	}
}

func TestENG130_ConcurrencyQueuePolicyBlocksThenAdmitsOnceSlotFrees(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(1, false, 0, sink)

	seed1 := rootDgm(1)
	execID1, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed1)
	if err != nil {
		t.Fatalf("first Start: %v", err)
	}

	seed2 := rootDgm(2)
	started2 := make(chan struct{})
	go func() {
		if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed2); err != nil {
			t.Errorf("second Start: %v", err)
		}
		close(started2)
	}()

	select {
	case <-started2:
		t.Fatal("second Start should have blocked while the first execution is still running")
	case <-time.After(100 * time.Millisecond):
	}

	// Finish the first execution, freeing its slot.
	tracker.NodeEvent("flow-1", seed1, NodeIO{NodeID: "sink"}, "")
	if !waitFor(t, func() bool { return tracker.Tracking(execID1) == false }, time.Second) {
		t.Fatal("first execution never finished")
	}

	select {
	case <-started2:
	case <-time.After(2 * time.Second):
		t.Fatal("second Start never unblocked after the first execution finished")
	}

	waiting, _, _, _ := sink.snapshot()
	if len(waiting) != 1 {
		t.Fatalf("waiting events = %v, want exactly one (the queued second execution)", waiting)
	}
}

func TestENG150_TimeoutMarksExecutionFailedAndFreesSlot(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(1, true, 30*time.Millisecond, sink)

	seed1 := rootDgm(1)
	if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed1); err != nil {
		t.Fatalf("first Start: %v", err)
	}

	if !waitFor(t, func() bool {
		_, _, _, finished := sink.snapshot()
		return len(finished) == 1
	}, time.Second) {
		t.Fatal("execution never timed out")
	}
	_, _, _, finished := sink.snapshot()
	if finished[0].Status != ExecutionFailed || finished[0].Reason != "timeout" {
		t.Fatalf("finished = %+v, want failed/timeout", finished)
	}

	// The timed-out execution's slot must be free for a new one.
	seed2 := rootDgm(2)
	if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed2); err != nil {
		t.Fatalf("Start after timeout freed the slot: %v", err)
	}
}

func TestENG130_CancelFreesSlotAndReportsFinished(t *testing.T) {
	sink := &recordingExecutionSink{}
	tracker := NewTracker(1, true, 0, sink)

	seed := rootDgm(1)
	execID, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if !tracker.Cancel("flow-1", execID) {
		t.Fatal("Cancel on a tracked execution should report true")
	}
	if tracker.Cancel("flow-1", execID) {
		t.Fatal("Cancel on an already-finished execution should report false")
	}

	_, _, _, finished := sink.snapshot()
	if len(finished) != 1 || finished[0].Status != ExecutionCancelled {
		t.Fatalf("finished = %+v, want one cancelled", finished)
	}

	seed2 := rootDgm(2)
	if _, err := tracker.Start(context.Background(), "flow-1", "trig", "webhook", "", seed2); err != nil {
		t.Fatalf("Start after cancel freed the slot: %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}
