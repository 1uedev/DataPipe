// Live debugging support (Increment 5): a bounded per-node ring buffer of
// recent datagrams (DBG-100, "held runtime-side; inspection works without
// redeploy") plus a rate-limited live forwarding path to an attached
// DebugSink (DBG-170, "sampled/rate-limited to protect the runtime"). The
// ring buffer is always populated at native speed regardless of whether
// anything is subscribed; only the live-forward path is throttled, since
// that is the one that turns into network traffic.
package flow

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/1uedev/DataPipe/engine/datagram"
)

var (
	debugIDMu      sync.Mutex
	debugIDEntropy = ulid.Monotonic(rand.Reader, 0)
)

func newDebugEventID() string {
	debugIDMu.Lock()
	defer debugIDMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), debugIDEntropy).String()
}

// DefaultRingBufferSize is DBG-100's configurable default of 50 datagrams.
const DefaultRingBufferSize = 50

// DefaultDebugRateLimit bounds how many live events per node per second are
// forwarded to a DebugSink (DBG-170); the ring buffer itself is unaffected.
const DefaultDebugRateLimit = 20

// DebugDirection classifies where in a node's lifecycle an event was
// observed.
type DebugDirection string

const (
	DirIn      DebugDirection = "in"
	DirOut     DebugDirection = "out"
	DirSidebar DebugDirection = "sidebar"
)

// DebugEvent is one observed datagram, ready to be shown in an inspector or
// forwarded off-process.
type DebugEvent struct {
	ID            string
	FlowID        string
	NodeID        string
	Port          string
	Direction     DebugDirection
	Label         string
	Time          time.Time
	DatagramID    string
	CorrelationID string
	CausationID   string
	Quality       string
	Value         any
}

// WireMetricsSample is a periodic snapshot of one wire's cumulative
// delivered/dropped counters (DBG-120's live counters/rates).
type WireMetricsSample struct {
	FlowID    string
	FromNode  string
	FromPort  string
	ToNode    string
	ToPort    string
	Delivered uint64
	Dropped   uint64
}

// DebugSink receives live debug events and wire-metrics samples. Deployment
// calls it unconditionally (through a rate limiter for events); a real sink
// is expected to cheaply no-op when nobody is subscribed to the relevant
// flow (DBG-170).
type DebugSink interface {
	Capture(DebugEvent)
	WireMetrics(WireMetricsSample)
}

type noopDebugSink struct{}

func (noopDebugSink) Capture(DebugEvent)            {}
func (noopDebugSink) WireMetrics(WireMetricsSample) {}

// NoopDebugSink is the default sink: capturing costs nothing beyond the
// ring-buffer write.
var NoopDebugSink DebugSink = noopDebugSink{}

// ringBuffer is a fixed-capacity, thread-safe circular buffer of DebugEvent,
// oldest-first on Snapshot.
type ringBuffer struct {
	mu     sync.Mutex
	events []DebugEvent
	cap    int
	next   int
	filled bool
}

func newRingBuffer(capacity int) *ringBuffer {
	if capacity <= 0 {
		capacity = DefaultRingBufferSize
	}
	return &ringBuffer{events: make([]DebugEvent, capacity), cap: capacity}
}

func (b *ringBuffer) push(e DebugEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.events[b.next] = e
	b.next = (b.next + 1) % b.cap
	if b.next == 0 {
		b.filled = true
	}
}

func (b *ringBuffer) snapshot() []DebugEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.filled {
		out := make([]DebugEvent, b.next)
		copy(out, b.events[:b.next])
		return out
	}
	out := make([]DebugEvent, b.cap)
	copy(out, b.events[b.next:])
	copy(out[b.cap-b.next:], b.events[:b.next])
	return out
}

// rateLimiter is a minimal per-key token bucket refilled at a fixed rate,
// used to cap how many events per second are handed to a DebugSink per node
// (DBG-170).
type rateLimiter struct {
	mu       sync.Mutex
	perSec   int
	tokens   float64
	lastFill time.Time
}

func newRateLimiter(perSec int) *rateLimiter {
	if perSec <= 0 {
		perSec = DefaultDebugRateLimit
	}
	return &rateLimiter{perSec: perSec, tokens: float64(perSec), lastFill: time.Now()}
}

func (r *rateLimiter) allow() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(r.lastFill).Seconds()
	r.lastFill = now
	r.tokens += elapsed * float64(r.perSec)
	if r.tokens > float64(r.perSec) {
		r.tokens = float64(r.perSec)
	}
	if r.tokens < 1 {
		return false
	}
	r.tokens--
	return true
}

// debugValue converts a datagram payload into a JSON-friendly snapshot for
// the inspector. Binary payloads are represented by size rather than raw
// bytes so large blobs never get copied into debug events (DGM-120's
// by-reference rationale applies here too).
func debugValue(p datagram.Payload) any {
	if p.IsBinary() {
		return map[string]any{"binary": true, "bytes": p.Size()}
	}
	return p.Value
}

func newDebugEvent(flowID, nodeID, port string, dir DebugDirection, label string, d datagram.Datagram) DebugEvent {
	return DebugEvent{
		ID:            newDebugEventID(),
		FlowID:        flowID,
		NodeID:        nodeID,
		Port:          port,
		Direction:     dir,
		Label:         label,
		Time:          time.Now().UTC(),
		DatagramID:    d.Header.ID,
		CorrelationID: d.Header.CorrelationID,
		CausationID:   d.Header.CausationID,
		Quality:       string(d.Header.Quality),
		Value:         debugValue(d.Payload),
	}
}
