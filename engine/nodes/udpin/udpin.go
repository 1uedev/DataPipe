// Package udpin implements the "udp-in" node (CON-290): listens on a UDP
// port and emits one base64 binary payload datagram per received packet —
// UDP is inherently message-bounded, so unlike "tcp-in" there's no framing
// concept to configure.
package udpin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"addr": { "type": "string", "description": "Listen address, e.g. \":9001\"." },
		"maxPacketBytes": { "type": "integer", "minimum": 1, "description": "Read buffer size (default 65507, the max UDP payload)." }
	},
	"required": ["addr"]
}`

func init() {
	flow.Register("udp-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "UDP In",
		Category:     flow.CategorySource,
		Description:  "Listen for UDP packets, one datagram per packet (CON-290).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "udp-in" node's "config" object.
type Config struct {
	Addr           string `json:"addr"`
	MaxPacketBytes int    `json:"maxPacketBytes,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "udp-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Addr == "" {
		return nil, fmt.Errorf("udp-in: addr is required")
	}
	if cfg.MaxPacketBytes <= 0 {
		cfg.MaxPacketBytes = 65507
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	var lc net.ListenConfig
	pconn, err := lc.ListenPacket(ctx, "udp", n.cfg.Addr)
	if err != nil {
		return fmt.Errorf("udp-in: listening on %s: %w", n.cfg.Addr, err)
	}
	defer func() { _ = pconn.Close() }()
	go func() { <-ctx.Done(); _ = pconn.Close() }()

	buf := make([]byte, n.cfg.MaxPacketBytes)
	for {
		nr, remote, err := pconn.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("udp-in: read: %w", err)
		}
		d := datagram.New(datagram.Source{NodeID: "udp-in", Origin: remote.String()}, datagram.Payload{Value: base64.StdEncoding.EncodeToString(buf[:nr])})
		if err := emit("out", d); err != nil {
			return err
		}
	}
}
