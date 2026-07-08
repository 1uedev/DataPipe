package websocketin

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

func TestCON320_ServerModeAcceptsConnectionAndEmitsMessages(t *testing.T) {
	raw, err := json.Marshal(Config{Mode: "server", Path: "/ws/test-in"})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	srv := httptest.NewServer(webhook.DefaultRegistry)
	defer srv.Close()

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
	time.Sleep(50 * time.Millisecond) // let the route register

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/test-in"
	conn, _, err := gorilla.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dialing: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.WriteMessage(gorilla.TextMessage, []byte(`{"temp":21.5}`)); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	select {
	case v := <-received:
		m, ok := v.(map[string]any)
		if !ok || m["temp"] != 21.5 {
			t.Errorf("received = %+v", v)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	cancel()
	<-done
}
