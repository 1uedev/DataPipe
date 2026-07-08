// Package modbussink implements the "modbus-sink" node (CON-230 write
// side): write a single coil/register or a typed multi-register value.
package modbussink

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/goburrow/modbus"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/modbusshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["tcp", "rtu"] },
		"tcp": { "type": "object", "properties": { "host": { "type": "string" }, "port": { "type": "integer" } } },
		"rtu": {
			"type": "object",
			"properties": {
				"port": { "type": "string" },
				"baudRate": { "type": "integer" },
				"dataBits": { "type": "integer" },
				"parity": { "type": "string", "enum": ["N", "E", "O"] },
				"stopBits": { "type": "integer" }
			}
		},
		"slaveId": { "type": "integer", "minimum": 0, "maximum": 255 },
		"timeoutMs": { "type": "integer" },
		"area": { "type": "string", "enum": ["coil", "register"] },
		"address": { "type": "integer", "minimum": 0 },
		"field": {
			"type": "object",
			"description": "Multi-register typed write, area \"register\"; omit for a plain single-register uint16 write.",
			"properties": {
				"type": { "type": "string", "enum": ["uint16", "int16", "uint32", "int32", "uint64", "int64", "float32", "float64", "string"] },
				"length": { "type": "integer" },
				"wordOrder": { "type": "string", "enum": ["big", "little"] }
			}
		},
		"valueField": { "type": "string", "description": "Payload field carrying the value to write (default: the whole payload)." }
	},
	"required": ["mode", "slaveId", "area", "address"]
}`

func init() {
	flow.Register("modbus-sink", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Modbus Sink",
		Category:     flow.CategoryProcessor,
		Description:  "Write a coil or (optionally typed multi-register) register value (CON-230).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "modbus-sink" node's "config" object.
type Config struct {
	modbusshared.Config
	Area       string              `json:"area"`
	Address    uint16              `json:"address"`
	Field      *modbusshared.Field `json:"field,omitempty"`
	ValueField string              `json:"valueField,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	client      modbus.Client
	closer      io.Closer
	connectErr  error
}

// New is the flow.Factory for the "modbus-sink" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Config.Validate(); err != nil {
		return nil, fmt.Errorf("modbus-sink: %w", err)
	}
	if cfg.Area != "coil" && cfg.Area != "register" {
		return nil, fmt.Errorf("modbus-sink: area must be \"coil\" or \"register\", got %q", cfg.Area)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) connect(ctx context.Context) (modbus.Client, error) {
	n.connectOnce.Do(func() {
		n.client, n.closer, n.connectErr = modbusshared.Open(n.cfg.Config)
		if n.connectErr == nil {
			go func() { <-ctx.Done(); _ = n.closer.Close() }()
		}
	})
	return n.client, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	client, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("modbus-sink: %w", err)
	}

	value := in.Payload.Value
	if n.cfg.ValueField != "" {
		if m, ok := in.Payload.Value.(map[string]any); ok {
			value = m[n.cfg.ValueField]
		}
	}

	if n.cfg.Area == "coil" {
		b, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("modbus-sink: area \"coil\" requires a boolean value, got %T", value)
		}
		v := uint16(0)
		if b {
			v = 0xFF00
		}
		if _, err := client.WriteSingleCoil(n.cfg.Address, v); err != nil {
			return nil, fmt.Errorf("modbus-sink: writing coil: %w", err)
		}
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	}

	if n.cfg.Field == nil {
		regValue, ok := toUint16(value)
		if !ok {
			return nil, fmt.Errorf("modbus-sink: area \"register\" without \"field\" requires a numeric value")
		}
		if _, err := client.WriteSingleRegister(n.cfg.Address, regValue); err != nil {
			return nil, fmt.Errorf("modbus-sink: writing register: %w", err)
		}
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	}

	words, err := modbusshared.EncodeField(*n.cfg.Field, value)
	if err != nil {
		return nil, fmt.Errorf("modbus-sink: %w", err)
	}
	data := make([]byte, len(words)*2)
	for i, w := range words {
		data[i*2] = byte(w >> 8)
		data[i*2+1] = byte(w)
	}
	if _, err := client.WriteMultipleRegisters(n.cfg.Address, uint16(len(words)), data); err != nil {
		return nil, fmt.Errorf("modbus-sink: writing registers: %w", err)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func toUint16(v any) (uint16, bool) {
	switch t := v.(type) {
	case float64:
		return uint16(int64(t)), true
	case int:
		return uint16(t), true
	default:
		return 0, false
	}
}
