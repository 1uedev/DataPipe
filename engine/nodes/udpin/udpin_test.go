package udpin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestCON290_NewRequiresAddr(t *testing.T) {
	if _, err := New([]byte(`{}`)); err == nil {
		t.Error("expected error for missing addr")
	}
}

func TestCON290_ReceivesPacketsAsBase64Datagrams(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := pc.LocalAddr().String()
	_ = pc.Close()

	raw, err := json.Marshal(Config{Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	received := make(chan any, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, func(_ string, d datagram.Datagram) error {
			received <- d.Payload.Value
			return nil
		})
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatalf("Write: %v", err)
	}

	select {
	case v := <-received:
		s, ok := v.(string)
		if !ok {
			t.Fatalf("payload type = %T", v)
		}
		decoded, err := base64.StdEncoding.DecodeString(s)
		if err != nil || string(decoded) != "ping" {
			t.Errorf("payload = %q (decoded %q), want \"ping\"", s, decoded)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for packet")
	}
}
