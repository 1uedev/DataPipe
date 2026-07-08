package serialout

import (
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestSNK_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{}`)); err == nil {
		t.Error("expected error for missing port")
	}
	n, err := New([]byte(`{"port":"/dev/ttyUSB0"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nd := n.(*node)
	if nd.cfg.BaudRate != 9600 || nd.cfg.DataBits != 8 {
		t.Errorf("defaults not applied: %+v", nd.cfg)
	}
}

func TestSNK_PayloadBytesDecodesBase64OrRaw(t *testing.T) {
	d1 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "aGVsbG8="})
	b, err := payloadBytes(d1)
	if err != nil || string(b) != "hello" {
		t.Errorf("payloadBytes(base64) = %q, %v", b, err)
	}
	d2 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "not-base64!!"})
	b, err = payloadBytes(d2)
	if err != nil || string(b) != "not-base64!!" {
		t.Errorf("payloadBytes(raw) = %q, %v", b, err)
	}
}
