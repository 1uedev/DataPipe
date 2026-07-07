package flow

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func newTestOutput(capacity int) (*bus.Wire, *bus.FanOut) {
	w := bus.NewWire(bus.WireConfig{Capacity: capacity, Overflow: bus.OverflowBlock})
	return w, bus.NewFanOut(datagram.DefaultBinaryRefThreshold, w)
}

func testDgm(v int) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: v})
}

// panickyProcessor panics on every call.
type panickyProcessor struct{ calls atomic.Int32 }

func (p *panickyProcessor) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	p.calls.Add(1)
	panic("boom")
}

func TestARC150_PanicInProcessorRecoveredAndNodeKeepsRunning(t *testing.T) {
	inbox := bus.NewWire(bus.WireConfig{Capacity: 4, Overflow: bus.OverflowBlock})
	r := &nodeRunner{id: "n1", logger: testLogger(), metrics: &NodeMetrics{}, outputs: map[string]*bus.FanOut{}}
	proc := &panickyProcessor{}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		r.runProcessor(ctx, proc, inbox)
		close(done)
	}()

	for i := 0; i < 3; i++ {
		if _, err := inbox.Send(context.Background(), testDgm(i)); err != nil {
			t.Fatalf("Send: %v", err)
		}
	}
	// Give the runner time to process (and panic-recover from) all three.
	time.Sleep(50 * time.Millisecond)

	if got := proc.calls.Load(); got != 3 {
		t.Fatalf("processor called %d times, want 3 (a panic must not stop the run loop)", got)
	}
	if got := r.metrics.Errors.Load(); got != 3 {
		t.Errorf("Errors metric = %d, want 3", got)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runProcessor did not exit after context cancellation")
	}
}

type panickySource struct{}

func (panickySource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	panic("source boom")
}

func TestARC150_PanicInSourceRecovered(t *testing.T) {
	r := &nodeRunner{id: "n1", logger: testLogger(), metrics: &NodeMetrics{}, outputs: map[string]*bus.FanOut{}}
	done := make(chan struct{})
	go func() {
		r.runSource(context.Background(), panickySource{})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runSource did not return after the source panicked")
	}
}

// flakyProcessor fails failCount times, then succeeds.
type flakyProcessor struct {
	failCount int
	calls     atomic.Int32
}

func (p *flakyProcessor) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	n := p.calls.Add(1)
	if int(n) <= p.failCount {
		return nil, errors.New("transient failure")
	}
	return []PortDatagram{{Port: "out", Datagram: in}}, nil
}

func TestERR100_RetrySucceedsWithinMaxAttempts(t *testing.T) {
	outWire, outFanout := newTestOutput(4)
	r := &nodeRunner{
		id:          "n1",
		logger:      testLogger(),
		metrics:     &NodeMetrics{},
		outputs:     map[string]*bus.FanOut{"out": outFanout},
		errorPolicy: &ErrorPolicy{OnError: "retry", Retry: &RetryPolicy{Max: 3, BackoffMs: 1, MaxBackoffMs: 5}},
	}
	proc := &flakyProcessor{failCount: 2}

	r.handle(context.Background(), testDgm(1), proc.Process)

	if got := proc.calls.Load(); got != 3 {
		t.Fatalf("processor called %d times, want 3 (2 failures + 1 success)", got)
	}
	if got := r.metrics.Retries.Load(); got != 2 {
		t.Errorf("Retries = %d, want 2", got)
	}
	select {
	case out := <-drainOne(t, outWire):
		if v, _ := out.Payload.Value.(int); v != 1 {
			t.Errorf("output value = %v, want 1", out.Payload.Value)
		}
	case <-time.After(time.Second):
		t.Fatal("no output delivered after eventual success")
	}
}

func TestERR100_RetryGivesUpAfterMax(t *testing.T) {
	r := &nodeRunner{
		id:          "n1",
		logger:      testLogger(),
		metrics:     &NodeMetrics{},
		outputs:     map[string]*bus.FanOut{},
		errorPolicy: &ErrorPolicy{OnError: "retry", Retry: &RetryPolicy{Max: 2, BackoffMs: 1, MaxBackoffMs: 5}},
	}
	proc := &flakyProcessor{failCount: 100}

	r.handle(context.Background(), testDgm(1), proc.Process)

	if got := proc.calls.Load(); got != 3 { // 1 initial + 2 retries
		t.Fatalf("processor called %d times, want 3 (initial + max 2 retries)", got)
	}
	if got := r.metrics.Retries.Load(); got != 2 {
		t.Errorf("Retries = %d, want 2", got)
	}
}

type alwaysFailProcessor struct{}

func (alwaysFailProcessor) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	return nil, errors.New("nope")
}

func TestERR100_ErrorPortRoutesOriginalAndErrorInfo(t *testing.T) {
	errWire, errFanout := newTestOutput(4)
	r := &nodeRunner{
		id:          "n1",
		logger:      testLogger(),
		metrics:     &NodeMetrics{},
		outputs:     map[string]*bus.FanOut{"error": errFanout},
		errorPolicy: &ErrorPolicy{OnError: "errorPort"},
	}

	in := testDgm(7)
	r.handle(context.Background(), in, alwaysFailProcessor{}.Process)

	out, err := errWire.Receive(context.Background())
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if out.Header.CausationID != in.Header.ID {
		t.Errorf("error datagram causation id = %q, want %q", out.Header.CausationID, in.Header.ID)
	}
	if out.Header.Quality != datagram.QualityBad {
		t.Errorf("error datagram quality = %v, want BAD", out.Header.Quality)
	}
	payload, ok := out.Payload.Value.(map[string]any)
	if !ok {
		t.Fatalf("error payload = %T, want map[string]any", out.Payload.Value)
	}
	errInfo, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("error payload[\"error\"] = %T, want map[string]any", payload["error"])
	}
	if errInfo["message"] != "nope" {
		t.Errorf("error message = %v, want %q", errInfo["message"], "nope")
	}
	if errInfo["node"] != "n1" {
		t.Errorf("error node = %v, want %q", errInfo["node"], "n1")
	}
}

func TestERR100_DiscardDropsSilentlyWithoutOutput(t *testing.T) {
	r := &nodeRunner{
		id:          "n1",
		logger:      testLogger(),
		metrics:     &NodeMetrics{},
		outputs:     map[string]*bus.FanOut{},
		errorPolicy: &ErrorPolicy{OnError: "discard"},
	}
	r.handle(context.Background(), testDgm(1), alwaysFailProcessor{}.Process)
	if got := r.metrics.Errors.Load(); got != 1 {
		t.Errorf("Errors = %d, want 1", got)
	}
}

func TestERR100_DefaultPolicyIsFail(t *testing.T) {
	r := &nodeRunner{id: "n1", logger: testLogger(), metrics: &NodeMetrics{}, outputs: map[string]*bus.FanOut{}}
	if got := r.policy(); got.OnError != "fail" {
		t.Errorf("default policy = %q, want %q", got.OnError, "fail")
	}
}

func drainOne(t *testing.T, w *bus.Wire) chan datagram.Datagram {
	t.Helper()
	ch := make(chan datagram.Datagram, 1)
	go func() {
		d, err := w.Receive(context.Background())
		if err == nil {
			ch <- d
		}
	}()
	return ch
}
