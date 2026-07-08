// Package serialin implements the "serial-in" node (CON-290): reads a
// serial (RS-232/RS-485 via USB or native) port, framed per
// engine/nodes/framing, emitting base64 binary payload datagrams. Not
// verified against real serial hardware in this environment — see TODO.md.
package serialin

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"go.bug.st/serial"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/framing"
)

const pollTimeout = 100 * time.Millisecond

const configSchema = `{
	"type": "object",
	"properties": {
		"port": { "type": "string", "description": "Device path, e.g. \"/dev/ttyUSB0\" or \"COM3\"." },
		"baudRate": { "type": "integer", "default": 9600 },
		"dataBits": { "type": "integer", "enum": [5, 6, 7, 8], "default": 8 },
		"parity": { "type": "string", "enum": ["none", "odd", "even", "mark", "space"], "default": "none" },
		"stopBits": { "type": "string", "enum": ["1", "1.5", "2"], "default": "1" },
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
	"required": ["port", "framing"]
}`

func init() {
	flow.Register("serial-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Serial In",
		Category:     flow.CategorySource,
		Description:  "Read a serial port, framed per config (CON-290).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "serial-in" node's "config" object.
type Config struct {
	Port     string         `json:"port"`
	BaudRate int            `json:"baudRate,omitempty"`
	DataBits int            `json:"dataBits,omitempty"`
	Parity   string         `json:"parity,omitempty"`
	StopBits string         `json:"stopBits,omitempty"`
	Framing  framing.Config `json:"framing"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "serial-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Port == "" {
		return nil, fmt.Errorf("serial-in: port is required")
	}
	if cfg.BaudRate <= 0 {
		cfg.BaudRate = 9600
	}
	if cfg.DataBits == 0 {
		cfg.DataBits = 8
	}
	if err := cfg.Framing.Validate(); err != nil {
		return nil, fmt.Errorf("serial-in: %w", err)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) mode() (*serial.Mode, error) {
	parity, err := parseParity(n.cfg.Parity)
	if err != nil {
		return nil, err
	}
	stopBits, err := parseStopBits(n.cfg.StopBits)
	if err != nil {
		return nil, err
	}
	return &serial.Mode{BaudRate: n.cfg.BaudRate, DataBits: n.cfg.DataBits, Parity: parity, StopBits: stopBits}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	mode, err := n.mode()
	if err != nil {
		return fmt.Errorf("serial-in: %w", err)
	}
	port, err := serial.Open(n.cfg.Port, mode)
	if err != nil {
		return fmt.Errorf("serial-in: opening %s: %w", n.cfg.Port, err)
	}
	defer func() { _ = port.Close() }()
	if err := port.SetReadTimeout(pollTimeout); err != nil {
		return fmt.Errorf("serial-in: setting read timeout: %w", err)
	}
	go func() { <-ctx.Done(); _ = port.Close() }()

	poll := func(buf []byte) (int, error) {
		nr, err := port.Read(buf)
		if err != nil {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}
			return 0, err
		}
		return nr, nil // SetReadTimeout makes a timed-out Read return (0, nil), matching PollFunc's contract directly
	}
	err = framing.Run(ctx, poll, n.cfg.Framing, func(frame []byte) error {
		d := datagram.New(datagram.Source{NodeID: "serial-in", Origin: n.cfg.Port}, datagram.Payload{Value: base64.StdEncoding.EncodeToString(frame)})
		return emit("out", d)
	})
	if err != nil && ctx.Err() != nil {
		return nil
	}
	return err
}

func parseParity(p string) (serial.Parity, error) {
	switch p {
	case "", "none":
		return serial.NoParity, nil
	case "odd":
		return serial.OddParity, nil
	case "even":
		return serial.EvenParity, nil
	case "mark":
		return serial.MarkParity, nil
	case "space":
		return serial.SpaceParity, nil
	default:
		return 0, fmt.Errorf("unknown parity %q", p)
	}
}

func parseStopBits(s string) (serial.StopBits, error) {
	switch s {
	case "", "1":
		return serial.OneStopBit, nil
	case "1.5":
		return serial.OnePointFiveStopBits, nil
	case "2":
		return serial.TwoStopBits, nil
	default:
		return 0, fmt.Errorf("unknown stopBits %q", s)
	}
}
