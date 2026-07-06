package schedule

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestCON330_NewRejectsUnknownMode(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"bogus"}`)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
}

func TestCON330_NewRejectsMissingCronExpression(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"cron"}`)); err == nil {
		t.Fatal("expected an error when cron mode has no expression")
	}
}

func TestCON330_NewRejectsInvalidCronExpression(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"cron","cron":"not a cron expression"}`)); err == nil {
		t.Fatal("expected an error for an invalid cron expression")
	}
}

func TestCON330_NewRejectsNonPositiveInterval(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"interval","intervalMs":0}`)); err == nil {
		t.Fatal("expected an error for a non-positive interval")
	}
}

func collectEmits(t *testing.T, src flow.Source, ctx context.Context) (stop func(), values func() []any) {
	t.Helper()
	var mu sync.Mutex
	var got []any
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(runCtx, func(port string, d datagram.Datagram) error {
			if port != "out" {
				t.Errorf("emit port = %q, want out", port)
			}
			mu.Lock()
			got = append(got, d.Payload.Value)
			mu.Unlock()
			return nil
		})
	}()
	return func() { cancel(); <-done }, func() []any {
		mu.Lock()
		defer mu.Unlock()
		return append([]any(nil), got...)
	}
}

func TestCON330_IntervalModeFiresRepeatedly(t *testing.T) {
	n, err := New(json.RawMessage(`{"mode":"interval","intervalMs":20,"payload":"tick"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, values := collectEmits(t, n.(flow.Source), context.Background())
	time.Sleep(120 * time.Millisecond)
	stop()

	got := values()
	if len(got) < 3 {
		t.Fatalf("expected at least 3 ticks in 120ms at a 20ms interval, got %d: %+v", len(got), got)
	}
	for _, v := range got {
		if v != "tick" {
			t.Errorf("payload = %v, want the configured literal \"tick\"", v)
		}
	}
}

func TestCON330_CronModeFiresEverySecond(t *testing.T) {
	n, err := New(json.RawMessage(`{"mode":"cron","cron":"* * * * * *","payload":"cron-tick"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, values := collectEmits(t, n.(flow.Source), context.Background())
	time.Sleep(2200 * time.Millisecond)
	stop()

	got := values()
	if len(got) < 2 {
		t.Fatalf("expected at least 2 firings of an every-second cron in ~2.2s, got %d: %+v", len(got), got)
	}
}

func TestCON330_DefaultPayloadIsFiredAtTimestamp(t *testing.T) {
	n, err := New(json.RawMessage(`{"mode":"interval","intervalMs":10}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	stop, values := collectEmits(t, n.(flow.Source), context.Background())
	time.Sleep(40 * time.Millisecond)
	stop()

	got := values()
	if len(got) == 0 {
		t.Fatal("expected at least one firing")
	}
	m, ok := got[0].(map[string]any)
	if !ok {
		t.Fatalf("default payload = %T, want map[string]any", got[0])
	}
	if _, ok := m["firedAt"].(string); !ok {
		t.Errorf("default payload missing string firedAt: %+v", m)
	}
}

func TestCON330_ContextCancellationStopsRun(t *testing.T) {
	n, err := New(json.RawMessage(`{"mode":"interval","intervalMs":10}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- n.(flow.Source).Run(ctx, func(string, datagram.Datagram) error { return nil }) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned an error on cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not return promptly after context cancellation")
	}
}
