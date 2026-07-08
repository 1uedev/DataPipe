package tcpin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/framing"
)

func TestCON290_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{"mode":"server","framing":{"mode":"delimiter","delimiter":"\n"}}`)); err == nil {
		t.Error("expected error for missing addr in server mode")
	}
	if _, err := New([]byte(`{"mode":"client","framing":{"mode":"delimiter","delimiter":"\n"}}`)); err == nil {
		t.Error("expected error for missing host/port in client mode")
	}
	if _, err := New([]byte(`{"mode":"server","addr":":0","framing":{"mode":"bogus"}}`)); err == nil {
		t.Error("expected error for invalid framing config")
	}
}

func TestCON290_ServerModeAcceptsAndFramesDelimitedMessages(t *testing.T) {
	// Bind to an OS-assigned free port first (:0), then reuse the resolved
	// addr string for the node's own listener below.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	raw, err := json.Marshal(Config{Mode: "server", Addr: addr, Framing: framing.Config{Mode: "delimiter", Delimiter: "\\n"}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	received := make(chan any, 2)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, func(_ string, d datagram.Datagram) error {
			received <- d.Payload.Value
			return nil
		})
	}()
	time.Sleep(100 * time.Millisecond) // let the listener bind

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("hello\nworld\n")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	for i, want := range []string{"hello", "world"} {
		select {
		case v := <-received:
			s, ok := v.(string)
			if !ok {
				t.Fatalf("payload[%d] type = %T", i, v)
			}
			decoded, err := base64.StdEncoding.DecodeString(s)
			if err != nil || string(decoded) != want {
				t.Errorf("payload[%d] = %q (decoded %q), want %q", i, s, decoded, want)
			}
		case <-time.After(3 * time.Second):
			t.Fatalf("timed out waiting for frame %d", i)
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Run to return after ctx cancellation")
	}
}
