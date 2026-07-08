// Package udpout implements the "udp-out" node (CON-290 companion sink):
// sends the payload as a single UDP packet to a target host:port.
package udpout

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"sync"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"host": { "type": "string" },
		"port": { "type": "integer" }
	},
	"required": ["host", "port"]
}`

func init() {
	flow.Register("udp-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "UDP Out",
		Category:     flow.CategoryProcessor,
		Description:  "Send the payload as a single UDP packet (CON-290).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "udp-out" node's "config" object.
type Config struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	conn        net.Conn
	connectErr  error
}

// New is the flow.Factory for the "udp-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Host == "" || cfg.Port == 0 {
		return nil, fmt.Errorf("udp-out: host and port are required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) connect(ctx context.Context) (net.Conn, error) {
	n.connectOnce.Do(func() {
		var d net.Dialer
		n.conn, n.connectErr = d.DialContext(ctx, "udp", fmt.Sprintf("%s:%d", n.cfg.Host, n.cfg.Port))
	})
	return n.conn, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	conn, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("udp-out: %w", err)
	}
	data, err := payloadBytes(in)
	if err != nil {
		return nil, fmt.Errorf("udp-out: %w", err)
	}
	if _, err := conn.Write(data); err != nil {
		return nil, fmt.Errorf("udp-out: writing: %w", err)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

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
