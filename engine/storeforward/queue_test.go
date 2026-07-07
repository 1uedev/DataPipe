package storeforward

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestEDGE130_EnqueueThenPeekReturnsFIFOOrder(t *testing.T) {
	q, err := Open(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue([]byte("a"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue([]byte("b"), time.Now()); err != nil {
		t.Fatal(err)
	}
	e, ok := q.Peek()
	if !ok || string(e.Payload) != "a" {
		t.Fatalf("expected head 'a', got %q ok=%v", e.Payload, ok)
	}
	q.Remove(e.ID)
	e, ok = q.Peek()
	if !ok || string(e.Payload) != "b" {
		t.Fatalf("expected head 'b' after remove, got %q ok=%v", e.Payload, ok)
	}
}

func TestEDGE130_SizeBoundDropsOldestEntries(t *testing.T) {
	probe, err := Open(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := probe.Enqueue([]byte("first"), time.Now()); err != nil {
		t.Fatal(err)
	}
	oneEntrySize := probe.SizeBytes() // bound that fits exactly one entry of this size

	q, err := Open(t.TempDir(), oneEntrySize, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue([]byte("first"), time.Now()); err != nil {
		t.Fatal(err)
	}
	dropped, err := q.Enqueue([]byte("second"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if dropped == 0 {
		t.Fatalf("expected at least one drop once the size bound was exceeded")
	}
	if q.Len() != 1 {
		t.Fatalf("expected exactly 1 entry surviving, got %d", q.Len())
	}
	e, _ := q.Peek()
	if string(e.Payload) != "second" {
		t.Fatalf("expected the newest entry to survive (drop-oldest), got %q", e.Payload)
	}
}

func TestEDGE130_AgeBoundDropsExpiredEntries(t *testing.T) {
	q, err := Open(t.TempDir(), 0, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-2 * time.Hour)
	if _, err := q.Enqueue([]byte("stale"), old); err != nil {
		t.Fatal(err)
	}
	dropped, err := q.Enqueue([]byte("fresh"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 1 {
		t.Fatalf("expected the stale entry to be dropped, dropped=%d", dropped)
	}
	if q.Len() != 1 {
		t.Fatalf("expected 1 surviving entry, got %d", q.Len())
	}
}

func TestEDGE130_QueueSurvivesReopenAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	q1, err := Open(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q1.Enqueue([]byte("one"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := q1.Enqueue([]byte("two"), time.Now()); err != nil {
		t.Fatal(err)
	}

	// Simulate a process restart: open a brand-new Queue over the same
	// directory and confirm it recovers exactly what was queued, in order —
	// this is what lets an edge flow survive a runtime restart without
	// losing its backlog (EDGE-130).
	q2, err := Open(dir, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if q2.Len() != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", q2.Len())
	}
	e, ok := q2.Peek()
	if !ok || string(e.Payload) != "one" {
		t.Fatalf("expected recovered FIFO head 'one', got %q ok=%v", e.Payload, ok)
	}
}

func TestEDGE130_DrainDeliversInOrderAndRemovesOnSuccess(t *testing.T) {
	q, err := Open(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"a", "b", "c"} {
		if _, err := q.Enqueue([]byte(p), time.Now()); err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	var delivered []string
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Drain(ctx, q, func(payload []byte, _ time.Time) error {
			mu.Lock()
			delivered = append(delivered, string(payload))
			mu.Unlock()
			return nil
		}, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for {
		mu.Lock()
		n := len(delivered)
		mu.Unlock()
		if n == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for drain, delivered so far: %v", delivered)
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 3 || delivered[0] != "a" || delivered[1] != "b" || delivered[2] != "c" {
		t.Fatalf("expected in-order delivery [a b c], got %v", delivered)
	}
	if q.Len() != 0 {
		t.Fatalf("expected queue empty after successful drain, len=%d", q.Len())
	}
}

func TestEDGE130_DrainRetriesHeadUntilDestinationRecovers(t *testing.T) {
	q, err := Open(t.TempDir(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue([]byte("payload"), time.Now()); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	attempts := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		Drain(ctx, q, func(payload []byte, _ time.Time) error {
			mu.Lock()
			attempts++
			n := attempts
			mu.Unlock()
			if n < 3 {
				return errUnreachable
			}
			return nil
		}, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for q.Len() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for the queue to drain after retries")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if attempts < 3 {
		t.Fatalf("expected at least 3 delivery attempts before success, got %d", attempts)
	}
}

type simulatedErr string

func (e simulatedErr) Error() string { return string(e) }

const errUnreachable = simulatedErr("destination unreachable")
