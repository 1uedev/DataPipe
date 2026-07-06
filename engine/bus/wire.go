// Package bus implements the internal data bus (Requirements §9,
// BUS-100/110/140): bounded, per-wire queues between nodes with
// configurable overflow (backpressure) policies, plus fan-out/fan-in.
package bus

import (
	"context"
	"errors"
	"sync"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// ErrClosed is returned by Send/Receive once the wire has been closed.
var ErrClosed = errors.New("bus: wire closed")

// OverflowPolicy governs what happens when a wire's bounded queue is full
// and a new datagram arrives (BUS-110).
type OverflowPolicy int

const (
	// OverflowBlock makes Send wait for room; the default for durable
	// sources (Kafka/SQL) that can tolerate being paused.
	OverflowBlock OverflowPolicy = iota
	// OverflowDropOldest evicts the head of the queue to make room.
	OverflowDropOldest
	// OverflowDropNewest discards the incoming datagram, keeping the queue
	// unchanged.
	OverflowDropNewest
	// OverflowSample keeps only every SampleEvery-th datagram while the
	// queue is full, dropping the rest.
	OverflowSample
)

// WireConfig configures a Wire's bounded queue and overflow behavior.
type WireConfig struct {
	Capacity    int
	Overflow    OverflowPolicy
	SampleEvery int // only used by OverflowSample; must be >= 1
}

// Metrics exposes the counters BUS-110 requires to be "counted and exposed
// as metrics and canvas warnings".
type Metrics struct {
	Delivered uint64
	Dropped   uint64
}

// Wire is a bounded, at-most-once, in-order channel between one output port
// and one input port (BUS-100), backed by a fixed-size ring buffer.
type Wire struct {
	cfg WireConfig

	mu       sync.Mutex
	notEmpty *sync.Cond
	notFull  *sync.Cond
	buf      []datagram.Datagram
	head     int
	count    int
	closed   bool
	sampleN  int
	metrics  Metrics
}

// NewWire creates a wire with the given bounded-queue configuration.
func NewWire(cfg WireConfig) *Wire {
	if cfg.Capacity <= 0 {
		cfg.Capacity = 1
	}
	if cfg.Overflow == OverflowSample && cfg.SampleEvery < 1 {
		cfg.SampleEvery = 1
	}
	w := &Wire{cfg: cfg, buf: make([]datagram.Datagram, cfg.Capacity)}
	w.notEmpty = sync.NewCond(&w.mu)
	w.notFull = sync.NewCond(&w.mu)
	return w
}

func (w *Wire) full() bool { return w.count >= w.cfg.Capacity }

// pushLocked appends dgm to the ring buffer; caller must ensure there is
// room (count < capacity) and hold w.mu.
func (w *Wire) pushLocked(dgm datagram.Datagram) {
	tail := (w.head + w.count) % len(w.buf)
	w.buf[tail] = dgm
	w.count++
}

// popOldestLocked evicts the head of the queue to make room; caller must
// hold w.mu and ensure the queue is non-empty.
func (w *Wire) popOldestLocked() {
	w.buf[w.head] = datagram.Datagram{} // release references for GC
	w.head = (w.head + 1) % len(w.buf)
	w.count--
}

// Send delivers dgm according to the wire's overflow policy. delivered is
// false when the policy dropped the datagram rather than enqueuing it; a
// non-nil error only occurs on cancellation or if the wire is closed.
func (w *Wire) Send(ctx context.Context, dgm datagram.Datagram) (delivered bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return false, ErrClosed
	}

	if w.full() {
		switch w.cfg.Overflow {
		case OverflowBlock:
			if err := waitLocked(ctx, &w.mu, w.notFull, func() bool {
				return w.full() && !w.closed
			}); err != nil {
				return false, err
			}
			if w.closed {
				return false, ErrClosed
			}
		case OverflowDropOldest:
			w.popOldestLocked()
			w.metrics.Dropped++
		case OverflowDropNewest:
			w.metrics.Dropped++
			return false, nil
		case OverflowSample:
			w.sampleN++
			if w.sampleN%w.cfg.SampleEvery != 0 {
				w.metrics.Dropped++
				return false, nil
			}
			w.popOldestLocked()
		}
	}

	w.pushLocked(dgm)
	w.metrics.Delivered++
	w.notEmpty.Signal()
	return true, nil
}

// Receive blocks until a datagram is available, ctx is cancelled, or the
// wire is closed and drained.
func (w *Wire) Receive(ctx context.Context) (datagram.Datagram, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := waitLocked(ctx, &w.mu, w.notEmpty, func() bool {
		return w.count == 0 && !w.closed
	}); err != nil {
		return datagram.Datagram{}, err
	}
	if w.count == 0 {
		return datagram.Datagram{}, ErrClosed
	}

	dgm := w.buf[w.head]
	w.popOldestLocked()
	w.notFull.Signal()
	return dgm, nil
}

// Close marks the wire closed and wakes any blocked Send/Receive callers.
func (w *Wire) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.notEmpty.Broadcast()
	w.notFull.Broadcast()
}

// Metrics returns a snapshot of the wire's delivered/dropped counters.
func (w *Wire) Metrics() Metrics {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.metrics
}

// Depth returns the current queue length.
func (w *Wire) Depth() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.count
}

// waitLocked blocks on cond while predicate() is true, waking early and
// returning ctx.Err() if ctx is cancelled. mu must already be held; cond
// must be built on mu.
func waitLocked(ctx context.Context, mu *sync.Mutex, cond *sync.Cond, predicate func() bool) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !predicate() {
		return nil
	}

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			mu.Lock()
			cond.Broadcast()
			mu.Unlock()
		case <-stop:
		}
	}()

	for predicate() {
		cond.Wait()
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
	return nil
}
