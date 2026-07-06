package bus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func dgmWith(v int) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "n"}, datagram.Payload{Value: v})
}

func valueOf(t *testing.T, d datagram.Datagram) int {
	t.Helper()
	v, ok := d.Payload.Value.(int)
	if !ok {
		t.Fatalf("payload value is %T, want int", d.Payload.Value)
	}
	return v
}

func TestBUS100_InOrderDeliveryPerWire(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 10, Overflow: OverflowBlock})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if _, err := w.Send(ctx, dgmWith(i)); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}
	for i := 0; i < 5; i++ {
		got, err := w.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive: %v", err)
		}
		if v := valueOf(t, got); v != i {
			t.Errorf("out of order: got %d want %d", v, i)
		}
	}
}

func TestBUS100_BoundedQueueRejectsBeyondCapacity(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 2, Overflow: OverflowDropNewest})
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		delivered, err := w.Send(ctx, dgmWith(i))
		if err != nil || !delivered {
			t.Fatalf("Send(%d): delivered=%v err=%v", i, delivered, err)
		}
	}
	if depth := w.Depth(); depth != 2 {
		t.Fatalf("Depth() = %d, want 2 (capacity)", depth)
	}
	delivered, err := w.Send(ctx, dgmWith(99))
	if err != nil {
		t.Fatalf("Send beyond capacity: unexpected error %v", err)
	}
	if delivered {
		t.Error("Send beyond capacity with DropNewest must report delivered=false")
	}
	if depth := w.Depth(); depth != 2 {
		t.Errorf("Depth() after overflow = %d, want unchanged 2", depth)
	}
}

func TestBUS110_OverflowBlockWaitsForRoom(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 1, Overflow: OverflowBlock})
	ctx := context.Background()

	if _, err := w.Send(ctx, dgmWith(0)); err != nil {
		t.Fatalf("first send: %v", err)
	}

	sendDone := make(chan error, 1)
	go func() {
		_, err := w.Send(ctx, dgmWith(1))
		sendDone <- err
	}()

	select {
	case <-sendDone:
		t.Fatal("Send should have blocked while the queue is full")
	case <-time.After(50 * time.Millisecond):
	}

	if _, err := w.Receive(ctx); err != nil {
		t.Fatalf("Receive: %v", err)
	}

	select {
	case err := <-sendDone:
		if err != nil {
			t.Fatalf("blocked Send returned error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Send did not unblock after Receive freed room")
	}
}

func TestBUS110_OverflowBlockRespectsContextCancellation(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 1, Overflow: OverflowBlock})
	ctx := context.Background()
	if _, err := w.Send(ctx, dgmWith(0)); err != nil {
		t.Fatalf("first send: %v", err)
	}

	cancelCtx, cancel := context.WithCancel(context.Background())
	sendDone := make(chan error, 1)
	go func() {
		_, err := w.Send(cancelCtx, dgmWith(1))
		sendDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-sendDone:
		if err != context.Canceled {
			t.Fatalf("Send error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Send did not return after context cancellation")
	}
}

func TestBUS110_OverflowDropOldest(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 2, Overflow: OverflowDropOldest})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := w.Send(ctx, dgmWith(i)); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}

	got1, _ := w.Receive(ctx)
	got2, _ := w.Receive(ctx)
	if v := valueOf(t, got1); v != 1 {
		t.Errorf("oldest survivor = %d, want 1 (0 should have been dropped)", v)
	}
	if v := valueOf(t, got2); v != 2 {
		t.Errorf("second survivor = %d, want 2", v)
	}
	if dropped := w.Metrics().Dropped; dropped != 1 {
		t.Errorf("Dropped = %d, want 1", dropped)
	}
}

func TestBUS110_OverflowDropNewest(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 2, Overflow: OverflowDropNewest})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := w.Send(ctx, dgmWith(i)); err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
	}

	got1, _ := w.Receive(ctx)
	got2, _ := w.Receive(ctx)
	if v := valueOf(t, got1); v != 0 {
		t.Errorf("first survivor = %d, want 0", v)
	}
	if v := valueOf(t, got2); v != 1 {
		t.Errorf("second survivor = %d, want 1 (2 should have been dropped)", v)
	}
	if dropped := w.Metrics().Dropped; dropped != 1 {
		t.Errorf("Dropped = %d, want 1", dropped)
	}
}

func TestBUS110_OverflowSampleKeepsEveryNth(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 1, Overflow: OverflowSample, SampleEvery: 3})
	ctx := context.Background()

	// Fill the single slot, then push 6 more while full: with SampleEvery=3
	// only the 3rd and 6th of those should survive (replacing the slot).
	if _, err := w.Send(ctx, dgmWith(0)); err != nil {
		t.Fatalf("Send(0): %v", err)
	}
	lastKept := -1
	for i := 1; i <= 6; i++ {
		delivered, err := w.Send(ctx, dgmWith(i))
		if err != nil {
			t.Fatalf("Send(%d): %v", i, err)
		}
		if delivered {
			lastKept = i
		}
	}
	if lastKept != 6 {
		t.Fatalf("expected the 6th overflow send (i=6) to be kept, lastKept=%d", lastKept)
	}

	got, err := w.Receive(ctx)
	if err != nil {
		t.Fatalf("Receive: %v", err)
	}
	if v := valueOf(t, got); v != 6 {
		t.Errorf("surviving datagram = %d, want 6", v)
	}
	// Of the 6 overflow sends (i=1..6), only i=3 and i=6 are kept (every
	// 3rd); the other 4 are dropped. i=3 itself is later evicted by i=6
	// reusing the single slot, but eviction isn't counted as a drop.
	if dropped := w.Metrics().Dropped; dropped != 4 {
		t.Errorf("Dropped = %d, want 4", dropped)
	}
}

func TestWire_CloseWakesBlockedReceive(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 1, Overflow: OverflowBlock})
	ctx := context.Background()

	recvDone := make(chan error, 1)
	go func() {
		_, err := w.Receive(ctx)
		recvDone <- err
	}()

	time.Sleep(20 * time.Millisecond)
	w.Close()

	select {
	case err := <-recvDone:
		if err != ErrClosed {
			t.Fatalf("Receive after Close = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Receive did not unblock after Close")
	}
}

func TestWire_ConcurrentSendReceiveNoRace(t *testing.T) {
	w := NewWire(WireConfig{Capacity: 4, Overflow: OverflowBlock})
	ctx := context.Background()
	const n = 2000

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			if _, err := w.Send(ctx, dgmWith(i)); err != nil {
				t.Errorf("Send: %v", err)
				return
			}
		}
	}()

	received := make([]int, 0, n)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			got, err := w.Receive(ctx)
			if err != nil {
				t.Errorf("Receive: %v", err)
				return
			}
			received = append(received, valueOf(t, got))
		}
	}()
	wg.Wait()

	if len(received) != n {
		t.Fatalf("received %d datagrams, want %d", len(received), n)
	}
	for i, v := range received {
		if v != i {
			t.Fatalf("order broken at index %d: got %d", i, v)
			break
		}
	}
}
