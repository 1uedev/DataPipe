package busin

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/topics"
)

func TestCON600_NewRequiresTopic(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error when topic is missing")
	}
}

func TestBUS120_RunEmitsPublishedDatagramsUntilContextCancelled(t *testing.T) {
	raw, err := json.Marshal(Config{Topic: "test/busin/run"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	var mu sync.Mutex
	var received []int
	emit := func(port string, d datagram.Datagram) error {
		if port != "out" {
			t.Errorf("emit port = %q, want out", port)
		}
		v, _ := d.Payload.Value.(int)
		mu.Lock()
		received = append(received, v)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := src.Run(ctx, emit); err != nil {
			t.Errorf("Run: %v", err)
		}
	}()

	// Give the subscription time to register before publishing.
	time.Sleep(50 * time.Millisecond)
	topics.DefaultBroker.Publish(context.Background(), "test/busin/run", nil, datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 7}))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 1 || received[0] != 7 {
		t.Fatalf("received = %+v, want [7]", received)
	}
}

func TestCON600_ParseOverflowDefaultsToDropOldest(t *testing.T) {
	// parseOverflow is exercised indirectly through Run/Subscribe elsewhere;
	// this only pins the mapping so a typo in the switch doesn't silently
	// change behavior.
	cases := []struct {
		spec string
		want bus.OverflowPolicy
	}{
		{"", bus.OverflowDropOldest},
		{"block", bus.OverflowBlock},
		{"dropNewest", bus.OverflowDropNewest},
		{"dropOldest", bus.OverflowDropOldest},
	}
	for _, c := range cases {
		if got := parseOverflow(c.spec); got != c.want {
			t.Errorf("parseOverflow(%q) = %v, want %v", c.spec, got, c.want)
		}
	}
}
