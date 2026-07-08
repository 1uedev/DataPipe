// Package serialout implements the "serial-out" node (CON-290 companion
// sink): writes the payload's bytes to a serial port. Not verified against
// real serial hardware in this environment — see TODO.md.
package serialout

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sync"

	"go.bug.st/serial"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"port": { "type": "string" },
		"baudRate": { "type": "integer", "default": 9600 },
		"dataBits": { "type": "integer", "enum": [5, 6, 7, 8], "default": 8 },
		"parity": { "type": "string", "enum": ["none", "odd", "even", "mark", "space"], "default": "none" },
		"stopBits": { "type": "string", "enum": ["1", "1.5", "2"], "default": "1" }
	},
	"required": ["port"]
}`

func init() {
	flow.Register("serial-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Serial Out",
		Category:     flow.CategoryProcessor,
		Description:  "Write the payload's bytes to a serial port (CON-290).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "serial-out" node's "config" object.
type Config struct {
	Port     string `json:"port"`
	BaudRate int    `json:"baudRate,omitempty"`
	DataBits int    `json:"dataBits,omitempty"`
	Parity   string `json:"parity,omitempty"`
	StopBits string `json:"stopBits,omitempty"`
}

type node struct {
	cfg Config

	openOnce sync.Once
	port     serial.Port
	openErr  error
}

// New is the flow.Factory for the "serial-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Port == "" {
		return nil, fmt.Errorf("serial-out: port is required")
	}
	if cfg.BaudRate <= 0 {
		cfg.BaudRate = 9600
	}
	if cfg.DataBits == 0 {
		cfg.DataBits = 8
	}
	return &node{cfg: cfg}, nil
}

func (n *node) open() (serial.Port, error) {
	n.openOnce.Do(func() {
		mode := &serial.Mode{BaudRate: n.cfg.BaudRate, DataBits: n.cfg.DataBits}
		n.port, n.openErr = serial.Open(n.cfg.Port, mode)
	})
	return n.port, n.openErr
}

func (n *node) Process(_ context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	port, err := n.open()
	if err != nil {
		return nil, fmt.Errorf("serial-out: %w", err)
	}
	data, err := payloadBytes(in)
	if err != nil {
		return nil, fmt.Errorf("serial-out: %w", err)
	}
	if _, err := port.Write(data); err != nil {
		return nil, fmt.Errorf("serial-out: writing: %w", err)
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
