// Package websocketout implements the "websocket-out" node (CON-320): mode
// "client" dials a remote WebSocket server and sends the payload; mode
// "server" broadcasts to every client currently connected to a
// "websocket-in" node in mode "server" on the same path (via
// engine/nodes/wsshared.Hub).
package websocketout

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	gorilla "github.com/gorilla/websocket"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/wsshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["server", "client"] },
		"path": { "type": "string", "description": "The \"websocket-in\" (mode \"server\") node's path to broadcast through, mode \"server\"." },
		"url": { "type": "string", "description": "Remote WebSocket URL to dial, mode \"client\"." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("websocket-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "WebSocket Out",
		Category:     flow.CategoryProcessor,
		Description:  "Broadcast to connected clients (server mode) or send to a remote server (client mode) (CON-320).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "websocket-out" node's "config" object.
type Config struct {
	Mode string `json:"mode"`
	Path string `json:"path,omitempty"`
	URL  string `json:"url,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	conn        *gorilla.Conn
	connectErr  error
}

// New is the flow.Factory for the "websocket-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "server":
		if cfg.Path == "" {
			return nil, fmt.Errorf("websocket-out: path is required for mode \"server\"")
		}
	case "client":
		if cfg.URL == "" {
			return nil, fmt.Errorf("websocket-out: url is required for mode \"client\"")
		}
	default:
		return nil, fmt.Errorf("websocket-out: mode must be \"server\" or \"client\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data, err := json.Marshal(in.Payload.Value)
	if err != nil {
		return nil, fmt.Errorf("websocket-out: encoding payload: %w", err)
	}

	if n.cfg.Mode == "server" {
		wsshared.HubFor(n.cfg.Path).Broadcast(gorilla.TextMessage, data)
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	}

	conn, err := n.clientConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("websocket-out: %w", err)
	}
	if err := conn.WriteMessage(gorilla.TextMessage, data); err != nil {
		return nil, fmt.Errorf("websocket-out: writing message: %w", err)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) clientConn(ctx context.Context) (*gorilla.Conn, error) {
	n.connectOnce.Do(func() {
		n.conn, _, n.connectErr = gorilla.DefaultDialer.DialContext(ctx, n.cfg.URL, nil)
	})
	return n.conn, n.connectErr
}
