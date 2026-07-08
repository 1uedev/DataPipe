// Package websocketin implements the "websocket-in" node (CON-320): server
// mode accepts inbound WebSocket connections on the shared webhook HTTP
// server (engine/webhook.DefaultRegistry, same one-process-wide-listener
// pattern as "http-in"); client mode dials out to a remote WebSocket
// server. Either mode emits one datagram per received message.
package websocketin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"

	gorilla "github.com/gorilla/websocket"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/wsshared"
	"github.com/1uedev/DataPipe/engine/webhook"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["server", "client"] },
		"path": { "type": "string", "description": "Route path on the shared webhook server, mode \"server\" (e.g. \"/ws/sensors\")." },
		"url": { "type": "string", "description": "Remote WebSocket URL to dial, mode \"client\" (e.g. \"ws://host:port/path\")." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("websocket-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "WebSocket In",
		Category:     flow.CategorySource,
		Description:  "Accept inbound WebSocket connections (server mode) or receive from a remote server (client mode) (CON-320).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "websocket-in" node's "config" object.
type Config struct {
	Mode string `json:"mode"`
	Path string `json:"path,omitempty"`
	URL  string `json:"url,omitempty"`
}

type node struct{ cfg Config }

var upgrader = gorilla.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(*http.Request) bool { return true }, // industrial LAN use case; see TODO.md for a future origin allowlist
}

// New is the flow.Factory for the "websocket-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "server":
		if cfg.Path == "" {
			return nil, fmt.Errorf("websocket-in: path is required for mode \"server\"")
		}
	case "client":
		if cfg.URL == "" {
			return nil, fmt.Errorf("websocket-in: url is required for mode \"client\"")
		}
	default:
		return nil, fmt.Errorf("websocket-in: mode must be \"server\" or \"client\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	if n.cfg.Mode == "client" {
		return n.runClient(ctx, emit)
	}
	return n.runServer(ctx, emit)
}

func (n *node) runClient(ctx context.Context, emit func(string, datagram.Datagram) error) error {
	conn, _, err := gorilla.DefaultDialer.DialContext(ctx, n.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("websocket-in: dialing %s: %w", n.cfg.URL, err)
	}
	defer func() { _ = conn.Close() }()
	go func() { <-ctx.Done(); _ = conn.Close() }()

	return n.readLoop(conn, n.cfg.URL, emit)
}

func (n *node) runServer(ctx context.Context, emit func(string, datagram.Datagram) error) error {
	var wg sync.WaitGroup
	hub := wsshared.HubFor(n.cfg.Path)
	unregister := webhook.DefaultRegistry.Register(http.MethodGet, n.cfg.Path, func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		hub.Add(conn)

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				hub.Remove(conn)
				_ = conn.Close()
			}()
			_ = n.readLoop(conn, r.RemoteAddr, emit)
		}()
	})
	defer unregister()

	<-ctx.Done()
	// Force-close every still-open accepted connection so each read loop's
	// blocking ReadMessage returns and wg.Wait() below can't hang forever
	// waiting on a peer that never sends another frame or a close handshake
	// (the same "runner must actively unblock its own goroutines on
	// shutdown" lesson as the store-and-forward drain loop, Increment 9).
	hub.CloseAll()
	wg.Wait()
	return nil
}

func (n *node) readLoop(conn *gorilla.Conn, origin string, emit func(string, datagram.Datagram) error) error {
	for {
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			return nil // connection closed (by peer, ctx cancellation closing conn, or a read error) — not an error worth failing the node over
		}
		value := decodeMessage(msgType, data)
		d := datagram.New(datagram.Source{NodeID: "websocket-in", Origin: origin}, datagram.Payload{Value: value})
		if err := emit("out", d); err != nil {
			return err
		}
	}
}

func decodeMessage(msgType int, data []byte) any {
	if msgType == gorilla.TextMessage {
		var v any
		if err := json.Unmarshal(data, &v); err == nil {
			return v
		}
		return string(data)
	}
	return base64.StdEncoding.EncodeToString(data) // binary message, same base64-text convention as the rest of the platform (e.g. sqlshared.NormalizeValue)
}
