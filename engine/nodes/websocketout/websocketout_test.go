package websocketout

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	gorilla "github.com/gorilla/websocket"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/websocketin"
	"github.com/1uedev/DataPipe/engine/webhook"
)

func TestCON320_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{"mode":"server"}`)); err == nil {
		t.Error("expected error for missing path in server mode")
	}
	if _, err := New([]byte(`{"mode":"client"}`)); err == nil {
		t.Error("expected error for missing url in client mode")
	}
	if _, err := New([]byte(`{"mode":"bogus"}`)); err == nil {
		t.Error("expected error for unknown mode")
	}
}

func TestCON320_ServerModeBroadcastsToConnectedClients(t *testing.T) {
	inRaw, err := json.Marshal(websocketin.Config{Mode: "server", Path: "/ws/test-out"})
	if err != nil {
		t.Fatal(err)
	}
	inNodeAny, err := websocketin.New(inRaw)
	if err != nil {
		t.Fatalf("websocketin.New: %v", err)
	}
	inSrc := inNodeAny.(flow.Source)

	srv := httptest.NewServer(webhook.DefaultRegistry)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = inSrc.Run(ctx, func(string, datagram.Datagram) error { return nil })
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond) // let the route register

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/test-out"
	conn, _, err := gorilla.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer func() { _ = conn.Close() }()
	time.Sleep(50 * time.Millisecond) // let the server accept + register in the hub

	outRaw, err := json.Marshal(Config{Mode: "server", Path: "/ws/test-out"})
	if err != nil {
		t.Fatal(err)
	}
	outNodeAny, err := New(outRaw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	outNode := outNodeAny.(flow.Processor)

	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"temp": 21.5}})
	if _, err := outNode.Process(ctx, d); err != nil {
		t.Fatalf("Process: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	var got map[string]any
	if err := json.Unmarshal(data, &got); err != nil || got["temp"] != 21.5 {
		t.Errorf("got = %s, err = %v", data, err)
	}
}
