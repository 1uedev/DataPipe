package bus

import (
	"context"
	"testing"
	"time"
)

func TestBUS140_FanOutDeliversIndependentCopies(t *testing.T) {
	ctx := context.Background()
	w1 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	w2 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	fanout := NewFanOut(1024, w1, w2)

	if err := fanout.Send(ctx, dgmWith(1)); err != nil {
		t.Fatalf("Send: %v", err)
	}

	got1, err := w1.Receive(ctx)
	if err != nil {
		t.Fatalf("w1.Receive: %v", err)
	}
	got2, err := w2.Receive(ctx)
	if err != nil {
		t.Fatalf("w2.Receive: %v", err)
	}

	// Fan-out delivers the same logical event to every branch, so both
	// copies keep the original datagram's identity.
	if got1.Header.ID != got2.Header.ID {
		t.Error("fan-out copies must keep the same header id (same logical event delivered twice)")
	}
	if got1.Header.CorrelationID != got2.Header.CorrelationID {
		t.Error("fan-out copies must share correlation id (same logical event)")
	}

	// Mutating one branch's header must not affect the other (independent
	// copies, BUS-140).
	got1.Header.Tags = map[string]string{"branch": "one"}
	if got2.Header.Tags != nil {
		t.Error("mutating one fan-out branch's tags leaked into the other")
	}
}

func TestBUS140_FanOutStopsOnFirstHardError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	w1 := NewWire(WireConfig{Capacity: 1, Overflow: OverflowBlock})
	// fill w1 so a Block-policy send would need to wait, then hit cancellation
	if _, err := w1.Send(context.Background(), dgmWith(0)); err != nil {
		t.Fatalf("prime w1: %v", err)
	}
	w2 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	fanout := NewFanOut(1024, w1, w2)

	if err := fanout.Send(ctx, dgmWith(1)); err == nil {
		t.Error("expected fan-out Send to fail on an already-cancelled context")
	}
	if depth := w2.Depth(); depth != 0 {
		t.Errorf("w2 depth = %d, want 0 (fan-out should stop before reaching later wires)", depth)
	}
}

func TestBUS140_FanInInterleavesArrivalOrder(t *testing.T) {
	ctx := context.Background()
	w1 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	w2 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	fanin := NewFanIn(ctx, 8, w1, w2)
	defer fanin.Close()

	// Send strictly in the order w1(0), w2(1), w1(2), w2(3), pacing sends so
	// arrival order is deterministic.
	send := func(w *Wire, v int) {
		if _, err := w.Send(ctx, dgmWith(v)); err != nil {
			t.Fatalf("Send(%d): %v", v, err)
		}
		time.Sleep(10 * time.Millisecond) // let the fan-in goroutine drain it
	}
	send(w1, 0)
	send(w2, 1)
	send(w1, 2)
	send(w2, 3)

	for _, want := range []int{0, 1, 2, 3} {
		got, err := fanin.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if v := valueOf(t, got); v != want {
			t.Errorf("fan-in order: got %d want %d", v, want)
		}
	}
}

func TestBUS140_FanInMergesAllSources(t *testing.T) {
	ctx := context.Background()
	w1 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	w2 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	w3 := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	fanin := NewFanIn(ctx, 16, w1, w2, w3)
	defer fanin.Close()

	total := 0
	for _, w := range []*Wire{w1, w2, w3} {
		for i := 0; i < 3; i++ {
			if _, err := w.Send(ctx, dgmWith(i)); err != nil {
				t.Fatalf("Send: %v", err)
			}
			total++
		}
	}

	recvCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	got := 0
	for got < total {
		if _, err := fanin.Receive(recvCtx); err != nil {
			t.Fatalf("Receive: %v", err)
		}
		got++
	}
	if got != total {
		t.Errorf("received %d datagrams, want %d", got, total)
	}
}
