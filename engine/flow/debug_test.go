package flow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestDBG100_RingBufferBoundedAndOldestFirst(t *testing.T) {
	rb := newRingBuffer(3)
	for i := 0; i < 5; i++ {
		rb.push(DebugEvent{DatagramID: string(rune('a' + i))})
	}
	got := rb.snapshot()
	if len(got) != 3 {
		t.Fatalf("expected ring buffer bounded at 3, got %d", len(got))
	}
	want := []string{"c", "d", "e"} // oldest of the last 3 pushed, oldest-first
	for i, e := range got {
		if e.DatagramID != want[i] {
			t.Fatalf("index %d: got %q, want %q (full: %+v)", i, e.DatagramID, want[i], got)
		}
	}
}

func TestDBG100_RingBufferSnapshotBeforeFull(t *testing.T) {
	rb := newRingBuffer(5)
	rb.push(DebugEvent{DatagramID: "a"})
	rb.push(DebugEvent{DatagramID: "b"})
	got := rb.snapshot()
	if len(got) != 2 || got[0].DatagramID != "a" || got[1].DatagramID != "b" {
		t.Fatalf("unexpected snapshot before buffer is full: %+v", got)
	}
}

func TestDBG170_RateLimiterCapsThroughput(t *testing.T) {
	rl := newRateLimiter(10)
	allowed := 0
	for i := 0; i < 1000; i++ {
		if rl.allow() {
			allowed++
		}
	}
	// A burst of 1000 calls with no elapsed time must be capped at the
	// bucket size (10), not let everything through.
	if allowed > 10 {
		t.Fatalf("rate limiter let %d/1000 immediate calls through, want <= 10", allowed)
	}
	if allowed == 0 {
		t.Fatalf("rate limiter let nothing through on a fresh bucket")
	}
}

// --- a busy-loop test source that emits as fast as possible (no ticker),
// used to approximate the "10k dgm/s" DBG-170 done-when criterion. ---

type debugTestFastSource struct{ n int }

func (s *debugTestFastSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	for i := 0; i < s.n; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		d := datagram.New(datagram.Source{NodeID: "fast"}, datagram.Payload{Value: float64(i)})
		if err := emit("out", d); err != nil {
			return err
		}
	}
	return nil
}

func newDebugTestFastSource(raw json.RawMessage) (any, error) {
	var cfg struct {
		N int `json:"n"`
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
	}
	if cfg.N <= 0 {
		cfg.N = 20000
	}
	return &debugTestFastSource{n: cfg.N}, nil
}

type countingDebugSink struct {
	mu           sync.Mutex
	events       int
	wireMetrics  int
	lastWireStat WireMetricsSample
}

func (s *countingDebugSink) Capture(DebugEvent) {
	s.mu.Lock()
	s.events++
	s.mu.Unlock()
}

func (s *countingDebugSink) WireMetrics(m WireMetricsSample) {
	s.mu.Lock()
	s.wireMetrics++
	s.lastWireStat = m
	s.mu.Unlock()
}

func (s *countingDebugSink) snapshot() (events, wireMetrics int, last WireMetricsSample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.events, s.wireMetrics, s.lastWireStat
}

// debugTestDrainSink just counts what it receives, without blocking, so a
// high-throughput producer can never stall on it (unlike graph-test-sink's
// bounded, undrained channel).
type debugTestDrainSink struct{}

func (debugTestDrainSink) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	return nil, nil
}

func newDebugTestDrainSink(json.RawMessage) (any, error) { return debugTestDrainSink{}, nil }

func init() {
	Register("debug-test-fast-source", NodeTypeInfo{Kind: KindSource, Outputs: []string{"out"}}, newDebugTestFastSource)
	Register("debug-test-drain-sink", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newDebugTestDrainSink)
}

// TestDBG100_170_HighThroughputStaysBounded proves the Increment 5 "done
// when" criterion: at a throughput far above what a browser could consume
// per datagram, the ring buffer (DBG-100) stays capped at its configured
// size and the live-forwarded event count (DBG-170) stays near the rate
// limit rather than growing with actual traffic — while wire metrics still
// accurately report the real delivered count.
func TestDBG100_170_HighThroughputStaysBounded(t *testing.T) {
	const total = 20000 // approximates a >>10k dgm/s burst condensed into one deploy

	cfg, err := json.Marshal(map[string]int{"n": total})
	if err != nil {
		t.Fatal(err)
	}
	f := &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "debug-throughput-flow", Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "src", Type: "debug-test-fast-source", Config: cfg},
				{ID: "sink", Type: "debug-test-drain-sink"},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "src", Port: "out"}, To: Endpoint{Node: "sink", Port: "in"}},
			},
		},
	}

	dep := NewDeployment(nil)
	defer dep.Stop()

	sink := &countingDebugSink{}
	dep.SetDebugSink(sink)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := dep.Deploy(ctx, f); err != nil {
		t.Fatalf("deploy failed: %v", err)
	}

	// Wait for the fast source to finish emitting all `total` datagrams by
	// polling the sink's own processed counter instead of a fixed sleep.
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		if stats, ok := dep.NodeStats("sink"); ok && stats.Metrics.Processed >= uint64(total) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	stats, ok := dep.NodeStats("sink")
	if !ok || stats.Metrics.Processed < uint64(total) {
		t.Fatalf("sink did not process all %d datagrams in time (processed=%d)", total, stats.Metrics.Processed)
	}

	// Ring buffer: bounded at the configured default regardless of total
	// throughput (DBG-100).
	rb := dep.NodeDebugSnapshot("sink")
	if len(rb) != DefaultRingBufferSize {
		t.Fatalf("ring buffer size = %d, want %d (total datagrams = %d)", len(rb), DefaultRingBufferSize, total)
	}

	// Live-forwarded events: capped near the rate limit, nowhere close to
	// the real traffic volume (DBG-170 "sampled/rate-limited to protect the
	// runtime"). Generous slack for the loop's real wall-clock duration.
	events, _, _ := sink.snapshot()
	maxExpected := DefaultDebugRateLimit * 3 // two nodes (src out, sink in) x limiter burst slack
	if events > maxExpected {
		t.Fatalf("live-forwarded events = %d, want <= ~%d (total datagrams = %d); rate limiting is not protecting the runtime", events, maxExpected, total)
	}

	// Wire metrics still accurately reflect the real, un-sampled volume.
	deadline = time.Now().Add(3 * time.Second)
	var lastStat WireMetricsSample
	for time.Now().Before(deadline) {
		_, wm, stat := sink.snapshot()
		if wm > 0 && stat.Delivered >= uint64(total) {
			lastStat = stat
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastStat.Delivered < uint64(total) {
		t.Fatalf("wire metrics delivered = %d, want >= %d (metrics must not be sampled, only the per-event stream is)", lastStat.Delivered, total)
	}
}
