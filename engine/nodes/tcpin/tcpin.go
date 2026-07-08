// Package tcpin implements the "tcp-in" node (CON-290): server mode listens
// and accepts connections, client mode dials out; either way, each accepted/
// dialed connection's byte stream is split into frames per
// engine/nodes/framing and emitted as base64 binary payload datagrams (the
// companion "convert" node's binaryParse, PROC-120, decodes them).
package tcpin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/framing"
	"github.com/1uedev/DataPipe/engine/nodes/tcpshared"
)

const pollTimeout = 100 * time.Millisecond

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["server", "client"] },
		"addr": { "type": "string", "description": "Listen address, mode \"server\" (e.g. \":9000\")." },
		"host": { "type": "string", "description": "Remote host, mode \"client\"." },
		"port": { "type": "integer", "description": "Remote port, mode \"client\"." },
		"framing": {
			"type": "object",
			"properties": {
				"mode": { "type": "string", "enum": ["delimiter", "fixedLength", "lengthPrefix", "timeout"] },
				"delimiter": { "type": "string" },
				"length": { "type": "integer" },
				"lengthPrefixBytes": { "type": "integer", "enum": [1, 2, 4] },
				"lengthPrefixEndianness": { "type": "string", "enum": ["big", "little"] },
				"timeoutMs": { "type": "integer" }
			},
			"required": ["mode"]
		}
	},
	"required": ["mode", "framing"]
}`

func init() {
	flow.Register("tcp-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "TCP In",
		Category:     flow.CategorySource,
		Description:  "Accept (server) or dial (client) a raw TCP byte stream, framed per config (CON-290).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "tcp-in" node's "config" object.
type Config struct {
	Mode    string         `json:"mode"`
	Addr    string         `json:"addr,omitempty"`
	Host    string         `json:"host,omitempty"`
	Port    int            `json:"port,omitempty"`
	Framing framing.Config `json:"framing"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "tcp-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "server":
		if cfg.Addr == "" {
			return nil, fmt.Errorf("tcp-in: addr is required for mode \"server\"")
		}
	case "client":
		if cfg.Host == "" || cfg.Port == 0 {
			return nil, fmt.Errorf("tcp-in: host and port are required for mode \"client\"")
		}
	default:
		return nil, fmt.Errorf("tcp-in: mode must be \"server\" or \"client\", got %q", cfg.Mode)
	}
	if err := cfg.Framing.Validate(); err != nil {
		return nil, fmt.Errorf("tcp-in: %w", err)
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
	addr := fmt.Sprintf("%s:%d", n.cfg.Host, n.cfg.Port)
	var d net.Dialer
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("tcp-in: dialing %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()
	go func() { <-ctx.Done(); _ = conn.Close() }()

	return n.stream(ctx, conn, addr, emit)
}

func (n *node) runServer(ctx context.Context, emit func(string, datagram.Datagram) error) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", n.cfg.Addr)
	if err != nil {
		return fmt.Errorf("tcp-in: listening on %s: %w", n.cfg.Addr, err)
	}
	hub := tcpshared.HubFor(n.cfg.Addr)
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		hub.CloseAll()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("tcp-in: accept: %w", err)
		}
		hub.Add(conn)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				hub.Remove(conn)
				_ = conn.Close()
			}()
			_ = n.stream(ctx, conn, conn.RemoteAddr().String(), emit)
		}()
	}
}

func (n *node) stream(ctx context.Context, conn net.Conn, origin string, emit func(string, datagram.Datagram) error) error {
	poll := func(buf []byte) (int, error) {
		_ = conn.SetReadDeadline(time.Now().Add(pollTimeout))
		nr, err := conn.Read(buf)
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return 0, nil
			}
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, err
		}
		return nr, nil
	}
	err := framing.Run(ctx, poll, n.cfg.Framing, func(frame []byte) error {
		d := datagram.New(datagram.Source{NodeID: "tcp-in", Origin: origin}, datagram.Payload{Value: base64.StdEncoding.EncodeToString(frame)})
		return emit("out", d)
	})
	if err != nil && ctx.Err() != nil {
		return nil // shutdown-triggered close/read-error, not a real failure
	}
	return err
}
