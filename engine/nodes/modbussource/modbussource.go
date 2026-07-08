// Package modbussource implements the "modbus-source" node (CON-230):
// Modbus TCP/RTU master, polling groups with independent intervals over
// coils/discrete inputs/holding/input registers, decoded per
// engine/nodes/modbusshared.Field's type/byte/word-order options.
package modbussource

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

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
		"pollingGroups": {
			"type": "array",
			"items": {
				"type": "object",
				"properties": {
					"name": { "type": "string" },
					"area": { "type": "string", "enum": ["coils", "discreteInputs", "holdingRegisters", "inputRegisters"] },
					"address": { "type": "integer", "minimum": 0 },
					"quantity": { "type": "integer", "minimum": 1 },
					"intervalMs": { "type": "integer", "minimum": 1 },
					"fields": {
						"type": "array",
						"items": {
							"type": "object",
							"properties": {
								"name": { "type": "string" },
								"register": { "type": "integer" },
								"type": { "type": "string", "enum": ["uint16", "int16", "uint32", "int32", "uint64", "int64", "float32", "float64", "string", "bit"] },
								"length": { "type": "integer" },
								"bitOffset": { "type": "integer" },
								"wordOrder": { "type": "string", "enum": ["big", "little"] }
							},
							"required": ["name", "register", "type"]
						}
					}
				},
				"required": ["name", "area", "address", "quantity", "intervalMs"]
			}
		}
	},
	"required": ["mode", "slaveId", "pollingGroups"]
}`

func init() {
	flow.Register("modbus-source", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Modbus Source",
		Category:     flow.CategorySource,
		Description:  "Modbus TCP/RTU master; polling groups over coils/registers with independent intervals and typed decoding (CON-230).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// PollingGroup is one entry in the "pollingGroups" config array.
type PollingGroup struct {
	Name       string               `json:"name"`
	Area       string               `json:"area"`
	Address    uint16               `json:"address"`
	Quantity   uint16               `json:"quantity"`
	IntervalMs int                  `json:"intervalMs"`
	Fields     []modbusshared.Field `json:"fields,omitempty"`
}

// Config is the "modbus-source" node's "config" object.
type Config struct {
	modbusshared.Config
	PollingGroups []PollingGroup `json:"pollingGroups"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "modbus-source" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("modbus-source: %w", err)
	}
	if len(cfg.PollingGroups) == 0 {
		return nil, fmt.Errorf("modbus-source: at least one polling group is required")
	}
	for _, g := range cfg.PollingGroups {
		if g.Name == "" {
			return nil, fmt.Errorf("modbus-source: polling group name is required")
		}
		switch g.Area {
		case "coils", "discreteInputs", "holdingRegisters", "inputRegisters":
		default:
			return nil, fmt.Errorf("modbus-source: group %q: unknown area %q", g.Name, g.Area)
		}
		if g.Quantity == 0 {
			return nil, fmt.Errorf("modbus-source: group %q: quantity must be positive", g.Name)
		}
		if g.IntervalMs <= 0 {
			return nil, fmt.Errorf("modbus-source: group %q: intervalMs must be positive", g.Name)
		}
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	client, closer, err := modbusshared.Open(n.cfg.Config)
	if err != nil {
		return fmt.Errorf("modbus-source: %w", err)
	}
	defer func() { _ = closer.Close() }()

	errCh := make(chan error, len(n.cfg.PollingGroups))
	for _, g := range n.cfg.PollingGroups {
		go n.pollGroup(ctx, client, g, emit, errCh)
	}

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (n *node) pollGroup(ctx context.Context, client modbus.Client, g PollingGroup, emit func(string, datagram.Datagram) error, errCh chan<- error) {
	ticker := time.NewTicker(time.Duration(g.IntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := n.pollOnce(client, g, emit); err != nil {
			select {
			case errCh <- fmt.Errorf("modbus-source: group %q: %w", g.Name, err):
			default:
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (n *node) pollOnce(client modbus.Client, g PollingGroup, emit func(string, datagram.Datagram) error) error {
	var value any
	switch g.Area {
	case "coils":
		raw, err := client.ReadCoils(g.Address, g.Quantity)
		if err != nil {
			return err
		}
		value = decodeBits(raw, int(g.Quantity))
	case "discreteInputs":
		raw, err := client.ReadDiscreteInputs(g.Address, g.Quantity)
		if err != nil {
			return err
		}
		value = decodeBits(raw, int(g.Quantity))
	case "holdingRegisters":
		raw, err := client.ReadHoldingRegisters(g.Address, g.Quantity)
		if err != nil {
			return err
		}
		value = decodeRegisterValue(raw, g.Fields)
	case "inputRegisters":
		raw, err := client.ReadInputRegisters(g.Address, g.Quantity)
		if err != nil {
			return err
		}
		value = decodeRegisterValue(raw, g.Fields)
	}
	d := datagram.New(datagram.Source{NodeID: "modbus-source"}, datagram.Payload{Value: value})
	d.Header.Tags = map[string]string{"modbus.group": g.Name}
	return emit("out", d)
}

func decodeRegisterValue(raw []byte, fields []modbusshared.Field) any {
	if len(fields) == 0 {
		words := make([]float64, len(raw)/2)
		for i := range words {
			words[i] = float64(uint16(raw[i*2])<<8 | uint16(raw[i*2+1]))
		}
		return words
	}
	m, err := modbusshared.DecodeRegisters(raw, fields)
	if err != nil {
		return map[string]any{"error": err.Error()}
	}
	return m
}

// decodeBits unpacks a Modbus coil/discrete-input byte block (1 bit per
// input, LSB first per byte, per the Modbus spec) into a bool slice.
func decodeBits(raw []byte, quantity int) []bool {
	out := make([]bool, quantity)
	for i := 0; i < quantity; i++ {
		out[i] = raw[i/8]&(1<<uint(i%8)) != 0
	}
	return out
}
