// Package tcpout implements the "tcp-out" node (CON-290 companion sink):
// mode "client" dials out and writes; mode "server" broadcasts to every
// client currently connected to a "tcp-in" node in mode "server" on the
// same addr (via a shared per-addr connection registry, mirroring
// engine/nodes/wsshared's websocket Hub).
package tcpout

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/tcpshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["server", "client"] },
		"addr": { "type": "string", "description": "The \"tcp-in\" (mode \"server\") node's listen addr to broadcast through, mode \"server\"." },
		"host": { "type": "string", "description": "Remote host, mode \"client\"." },
		"port": { "type": "integer", "description": "Remote port, mode \"client\"." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("tcp-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "TCP Out",
		Category:     flow.CategoryProcessor,
		Description:  "Broadcast to connected clients (server mode) or send to a remote host (client mode) (CON-290).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "tcp-out" node's "config" object.
type Config struct {
	Mode string `json:"mode"`
	Addr string `json:"addr,omitempty"`
	Host string `json:"host,omitempty"`
	Port int    `json:"port,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	conn        net.Conn
	connectErr  error
}

// New is the flow.Factory for the "tcp-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "server":
		if cfg.Addr == "" {
			return nil, fmt.Errorf("tcp-out: addr is required for mode \"server\"")
		}
	case "client":
		if cfg.Host == "" || cfg.Port == 0 {
			return nil, fmt.Errorf("tcp-out: host and port are required for mode \"client\"")
		}
	default:
		return nil, fmt.Errorf("tcp-out: mode must be \"server\" or \"client\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	data, err := payloadBytes(in)
	if err != nil {
		return nil, fmt.Errorf("tcp-out: %w", err)
	}

	if n.cfg.Mode == "server" {
		tcpshared.HubFor(n.cfg.Addr).Broadcast(data)
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	}

	conn, err := n.clientConn(ctx)
	if err != nil {
		return nil, fmt.Errorf("tcp-out: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("tcp-out: writing: %w", err)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) clientConn(ctx context.Context) (net.Conn, error) {
	n.connectOnce.Do(func() {
		var d net.Dialer
		n.conn, n.connectErr = d.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", n.cfg.Host, n.cfg.Port))
	})
	return n.conn, n.connectErr
}

// payloadBytes interprets the payload as base64 binary (the convention
// "tcp-in"/"convert" use), falling back to the raw string bytes, or a
// JSON-encoded form for anything else.
func payloadBytes(in datagram.Datagram) ([]byte, error) {
	s, ok := in.Payload.Value.(string)
	if !ok {
		return json.Marshal(in.Payload.Value)
	}
	if decoded, err := base64.StdEncoding.DecodeString(s); err == nil {
		return decoded, nil
	}
	return []byte(s), nil
}
