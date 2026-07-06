package inject

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestINJECT_FiresOnceWithoutRepeat(t *testing.T) {
	n, err := New(mustJSON(t, Config{Payload: "hello"}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	var count atomic.Int32
	var lastPayload any
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	err = src.Run(ctx, func(port string, d datagram.Datagram) error {
		count.Add(1)
		lastPayload = d.Payload.Value
		if port != "out" {
			t.Errorf("port = %q, want %q", port, "out")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := count.Load(); got != 1 {
		t.Errorf("emit called %d times, want exactly 1 (no repeat configured)", got)
	}
	if lastPayload != "hello" {
		t.Errorf("payload = %v, want %q", lastPayload, "hello")
	}
}

func TestINJECT_RepeatsUntilContextCancelled(t *testing.T) {
	n, err := New(mustJSON(t, Config{Payload: 1, RepeatMs: 5}))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	var count atomic.Int32
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- src.Run(ctx, func(port string, d datagram.Datagram) error {
			count.Add(1)
			return nil
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return after context timeout")
	}

	if got := count.Load(); got < 2 {
		t.Errorf("emit called %d times, want >= 2 (repeatMs=5 over 60ms)", got)
	}
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}
