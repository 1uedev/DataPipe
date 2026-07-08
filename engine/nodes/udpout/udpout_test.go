package udpout

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestCON290_NewRequiresHostAndPort(t *testing.T) {
	if _, err := New([]byte(`{}`)); err == nil {
		t.Error("expected error for missing host/port")
	}
}

func TestCON290_SendsPayloadAsUDPPacket(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	udpAddr := pc.LocalAddr().(*net.UDPAddr)

	raw, err := json.Marshal(Config{Host: "127.0.0.1", Port: udpAddr.Port})
	if err != nil {
		t.Fatal(err)
	}
	nodeAny, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := nodeAny.(*node)

	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "hello"})
	if _, err := n.Process(context.Background(), d); err != nil {
		t.Fatalf("Process: %v", err)
	}

	buf := make([]byte, 32)
	nr, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if string(buf[:nr]) != "hello" {
		t.Errorf("got %q, want %q", buf[:nr], "hello")
	}
}
