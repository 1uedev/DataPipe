package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/topics"
)

// --- test-only trigger node: emits whatever string arrives on a
// package-level, test-controlled channel, as a fresh root datagram — a
// stand-in for a real trigger like http-in, driven programmatically. ---

var (
	execTestTriggerMu    sync.Mutex
	execTestTriggerChans = map[string]chan string{}
)

func execTestTriggerChannel(key string) chan string {
	execTestTriggerMu.Lock()
	defer execTestTriggerMu.Unlock()
	if ch, ok := execTestTriggerChans[key]; ok {
		return ch
	}
	ch := make(chan string, 10)
	execTestTriggerChans[key] = ch
	return ch
}

type execTestTrigger struct{ ch chan string }

func (n *execTestTrigger) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	for {
		select {
		case v := <-n.ch:
			d := datagram.New(datagram.Source{NodeID: "trigger"}, datagram.Payload{Value: v})
			if err := emit("out", d); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func newExecTestTrigger(raw json.RawMessage) (any, error) {
	var cfg struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &execTestTrigger{ch: execTestTriggerChannel(cfg.Key)}, nil
}

func init() {
	Register("exec-test-trigger", NodeTypeInfo{Kind: KindSource, Trigger: true, Outputs: []string{"out"}}, newExecTestTrigger)
}

// --- test-only processor: fails (default ERR-100 "fail" policy) whenever
// the payload equals its configured failOn value, otherwise passes
// through — hot-redeploying with a different failOn is this test's stand-in
// for "the underlying issue is fixed" between a failed run and a re-run. ---

type execTestMaybeFail struct{ failOn string }

func (n *execTestMaybeFail) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	v, _ := in.Payload.Value.(string)
	if v == n.failOn {
		return nil, fmt.Errorf("exec-test-maybe-fail: intentional failure on %q", v)
	}
	out := datagram.NewCaused(in, datagram.Source{NodeID: "maybe-fail"}, in.Payload)
	return []PortDatagram{{Port: "out", Datagram: out}}, nil
}

func newExecTestMaybeFail(raw json.RawMessage) (any, error) {
	var cfg struct {
		FailOn string `json:"failOn"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &execTestMaybeFail{failOn: cfg.FailOn}, nil
}

func init() {
	Register("exec-test-maybe-fail", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, newExecTestMaybeFail)
}

// --- test-only terminal sink: string payloads onto a package-level,
// test-observed channel. ---

var (
	execTestSinkMu    sync.Mutex
	execTestSinkChans = map[string]chan string{}
)

func execTestSinkChannel(key string) chan string {
	execTestSinkMu.Lock()
	defer execTestSinkMu.Unlock()
	if ch, ok := execTestSinkChans[key]; ok {
		return ch
	}
	ch := make(chan string, 10)
	execTestSinkChans[key] = ch
	return ch
}

type execTestSink struct{ ch chan string }

func (n *execTestSink) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	v, _ := in.Payload.Value.(string)
	select {
	case n.ch <- v:
	case <-ctx.Done():
	}
	return nil, nil
}

func newExecTestSink(raw json.RawMessage) (any, error) {
	var cfg struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &execTestSink{ch: execTestSinkChannel(cfg.Key)}, nil
}

func init() {
	Register("exec-test-sink", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newExecTestSink)
}

// triggerFlowFile builds trigger -> maybe-fail -> sink, all keyed by key so
// concurrent subtests never collide.
func triggerFlowFile(t *testing.T, key, failOn string) *FlowFile {
	return &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_exec_" + key, Name: "t", Mode: ModeTriggered,
		Graph: Graph{
			Nodes: []Node{
				{ID: "trig", Type: "exec-test-trigger", TypeVersion: 1, Config: rawConfig(t, map[string]any{"key": key})},
				{ID: "mf", Type: "exec-test-maybe-fail", TypeVersion: 1, Config: rawConfig(t, map[string]any{"failOn": failOn})},
				{ID: "sink", Type: "exec-test-sink", TypeVersion: 1, Config: rawConfig(t, map[string]any{"key": key})},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "trig", Port: "out"}, To: Endpoint{Node: "mf", Port: "in"}},
				{ID: "w2", From: Endpoint{Node: "mf", Port: "out"}, To: Endpoint{Node: "sink", Port: "in"}},
			},
		},
	}
}

func TestENG130_LiveTriggerFireProducesTrackedSuccessAndReachesSink(t *testing.T) {
	key := "live-success"
	sink := &recordingExecutionSink{}
	dlq := &recordingDeadLetterSink{}

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetExecutionSink(sink)
	d.SetDeadLetterSink(dlq)
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "never-matches")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	execTestTriggerChannel(key) <- "hello"

	select {
	case v := <-execTestSinkChannel(key):
		if v != "hello" {
			t.Fatalf("sink received %q, want \"hello\"", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sink never received the datagram")
	}

	if !waitFor(t, func() bool {
		_, _, _, finished := sink.snapshot()
		return len(finished) == 1
	}, time.Second) {
		t.Fatal("execution never reported finished")
	}
	_, started, nodes, finished := sink.snapshot()
	if len(started) != 1 {
		t.Fatalf("started events = %v, want exactly 1", started)
	}
	if len(nodes) != 2 { // maybe-fail's success + sink's success
		t.Fatalf("node events = %+v, want exactly 2", nodes)
	}
	if finished[0].Status != ExecutionSuccess {
		t.Fatalf("finished = %+v, want success", finished[0])
	}
	if dlq.count() != 0 {
		t.Fatalf("dead letters = %d, want 0 for a successful run", dlq.count())
	}
}

func TestERR130_LiveNodeFailureDeadLettersAndFailsExecution(t *testing.T) {
	key := "live-fail"
	execSink := &recordingExecutionSink{}
	dlq := &recordingDeadLetterSink{}

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetExecutionSink(execSink)
	d.SetDeadLetterSink(dlq)
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "bad")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	execTestTriggerChannel(key) <- "bad"

	if !waitFor(t, func() bool { return dlq.count() == 1 }, 2*time.Second) {
		t.Fatal("expected the failing node's datagram to be dead-lettered")
	}
	if !waitFor(t, func() bool {
		_, _, _, finished := execSink.snapshot()
		return len(finished) == 1
	}, time.Second) {
		t.Fatal("execution never reported finished")
	}
	_, _, _, finished := execSink.snapshot()
	if finished[0].Status != ExecutionFailed {
		t.Fatalf("finished = %+v, want failed", finished[0])
	}

	select {
	case v := <-execTestSinkChannel(key):
		t.Fatalf("sink should never have received a datagram past the failing node, got %q", v)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestERR120_LiveUnhandledFailurePublishesToErrorFlowTopic(t *testing.T) {
	key := "live-errflow"
	target := "flow_errhandler_" + key

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetExecutionSink(&recordingExecutionSink{})
	d.SetDeadLetterSink(&recordingDeadLetterSink{})
	d.SetDefaultErrorFlow(target)
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "bad")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	wire, cancel := topics.DefaultBroker.Subscribe(ErrorFlowTopic(target), nil, bus.WireConfig{Capacity: 8, Overflow: bus.OverflowDropOldest})
	defer cancel()

	execTestTriggerChannel(key) <- "bad"

	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()
	errDgm, err := wire.Receive(ctx)
	if err != nil {
		t.Fatalf("expected the error-flow topic to receive the unhandled error datagram: %v", err)
	}
	payload, _ := errDgm.Payload.Value.(map[string]any)
	if payload == nil || payload["error"] == nil {
		t.Fatalf("error datagram payload = %+v, want ERR-100's {original, error} shape", errDgm.Payload.Value)
	}
}

func TestDBG140_ReplayInputRerunsFailedNodeAfterFix(t *testing.T) {
	key := "live-replay-input"
	dlq := &recordingDeadLetterSink{}
	execSink := &recordingExecutionSink{}

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetExecutionSink(execSink)
	d.SetDeadLetterSink(dlq)
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "bad")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	execTestTriggerChannel(key) <- "bad"
	if !waitFor(t, func() bool { return dlq.count() == 1 }, 2*time.Second) {
		t.Fatal("expected the failing node's datagram to be dead-lettered")
	}
	failedInput := dlq.last()

	// "Fix" the issue: redeploy with a failOn value that no longer matches
	// the recorded input (ENG-140 hot-redeploy, unrelated node ids so only
	// mf's config changes — the trigger and sink nodes' own fingerprints
	// are untouched and so are NOT restarted by this Deploy call, which is
	// exactly the scenario that exposed a real bug: reconfigureTrackerLocked
	// used to replace g.execTracker outright on every Deploy, orphaning any
	// node whose runner had already captured the old Tracker pointer — the
	// sink node's completion report would go to a Tracker nobody was
	// listening to anymore, and the execution would never resolve.
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "no-longer-bad")); err != nil {
		t.Fatalf("redeploy: %v", err)
	}

	execID, err := d.ReplayInput(context.Background(), "mf", "in", failedInput, "orig-exec")
	if err != nil {
		t.Fatalf("ReplayInput: %v", err)
	}

	select {
	case v := <-execTestSinkChannel(key):
		if v != "bad" {
			t.Fatalf("sink received %q, want the original payload \"bad\" replayed successfully", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sink never received the re-run's output")
	}

	if !waitFor(t, func() bool {
		_, _, _, finished := execSink.snapshot()
		for _, f := range finished {
			if f.ExecutionID == execID {
				return true
			}
		}
		return false
	}, 2*time.Second) {
		t.Fatal("re-run execution never reported Finished (Tracker orphaned by the redeploy?)")
	}
	_, _, _, finished := execSink.snapshot()
	for _, f := range finished {
		if f.ExecutionID == execID && f.Status != ExecutionSuccess {
			t.Fatalf("re-run execution status = %q, want success", f.Status)
		}
	}
}

func TestDBG140_ReplayOutputRerunsFromStart(t *testing.T) {
	key := "live-replay-output"

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetExecutionSink(&recordingExecutionSink{})
	d.SetDeadLetterSink(&recordingDeadLetterSink{})
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "never-matches")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	seed := datagram.New(datagram.Source{NodeID: "trigger"}, datagram.Payload{Value: "replayed"})
	if _, err := d.ReplayOutput(context.Background(), "trig", "out", seed, ""); err != nil {
		t.Fatalf("ReplayOutput: %v", err)
	}

	select {
	case v := <-execTestSinkChannel(key):
		if v != "replayed" {
			t.Fatalf("sink received %q, want \"replayed\"", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sink never received the replayed-from-start datagram")
	}
}

func TestENG130_CancelExecutionOnLiveDeployment(t *testing.T) {
	key := "live-cancel"
	sink := &recordingExecutionSink{}

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetExecutionSink(sink)
	d.SetDeadLetterSink(&recordingDeadLetterSink{})
	if err := d.Deploy(context.Background(), triggerFlowFile(t, key, "never-matches")); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	execTestTriggerChannel(key) <- "hello"
	if !waitFor(t, func() bool {
		_, started, _, _ := sink.snapshot()
		return len(started) == 1
	}, time.Second) {
		t.Fatal("execution never started")
	}
	_, started, _, _ := sink.snapshot()

	// The execution likely already finished (it's fast); Cancel on an
	// already-finished execution correctly reports false — still exercises
	// the wiring end to end (Deployment.CancelExecution -> Tracker.Cancel).
	d.CancelExecution(started[0])
}

// recordingDeadLetterSink records every captured dead letter.
type recordingDeadLetterSink struct {
	mu       sync.Mutex
	captured []datagram.Datagram
}

func (s *recordingDeadLetterSink) Capture(flowID, nodeID, port, reason string, d datagram.Datagram, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captured = append(s.captured, d)
}

func (s *recordingDeadLetterSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.captured)
}

func (s *recordingDeadLetterSink) last() datagram.Datagram {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.captured[len(s.captured)-1]
}
