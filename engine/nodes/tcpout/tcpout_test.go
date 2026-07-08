package tcpout

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/framing"
	"github.com/1uedev/DataPipe/engine/nodes/tcpin"
)

func TestCON290_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{"mode":"server"}`)); err == nil {
		t.Error("expected error for missing addr in server mode")
	}
	if _, err := New([]byte(`{"mode":"client"}`)); err == nil {
		t.Error("expected error for missing host/port in client mode")
	}
}

func TestCON290_ServerModeBroadcastsToConnectedClients(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	inRaw, err := json.Marshal(tcpin.Config{Mode: "server", Addr: addr, Framing: framing.Config{Mode: "delimiter", Delimiter: "\\n"}})
	if err != nil {
		t.Fatal(err)
	}
	inNodeAny, err := tcpin.New(inRaw)
	if err != nil {
		t.Fatalf("tcpin.New: %v", err)
	}
	inSrc := inNodeAny.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = inSrc.Run(ctx, func(string, datagram.Datagram) error { return nil })
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(100 * time.Millisecond)

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer func() { _ = conn.Close() }()
	time.Sleep(50 * time.Millisecond) // let the server accept + register in the hub

	outRaw, err := json.Marshal(Config{Mode: "server", Addr: addr})
	if err != nil {
		t.Fatal(err)
	}
	outNodeAny, err := New(outRaw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	outNode := outNodeAny.(flow.Processor)

	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "hello"})
	if _, err := outNode.Process(ctx, d); err != nil {
		t.Fatalf("Process: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	buf := make([]byte, 32)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Errorf("got %q, want %q", buf[:n], "hello")
	}
}
