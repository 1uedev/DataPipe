// Package modbusshared is the connection-establishment code shared by
// "modbus-source" and "modbus-sink" (CON-230): Modbus TCP and RTU (serial)
// master setup, plus register <-> typed-value decoding shared by both.
package modbusshared

import (
	"fmt"
	"io"
	"time"

	"github.com/goburrow/modbus"
)

// Config is a Modbus connection's config: exactly one of TCP/RTU is set,
// matching Mode.
type Config struct {
	Mode      string    `json:"mode"` // "tcp" | "rtu"
	TCP       TCPConfig `json:"tcp,omitempty"`
	RTU       RTUConfig `json:"rtu,omitempty"`
	SlaveID   int       `json:"slaveId"`
	TimeoutMs int       `json:"timeoutMs,omitempty"`
}

// TCPConfig is the "tcp" mode's transport config.
type TCPConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// RTUConfig is the "rtu" (serial) mode's transport config.
type RTUConfig struct {
	Port     string `json:"port"`
	BaudRate int    `json:"baudRate,omitempty"`
	DataBits int    `json:"dataBits,omitempty"`
	Parity   string `json:"parity,omitempty"` // "N" (default) | "E" | "O"
	StopBits int    `json:"stopBits,omitempty"`
}

// Validate checks cfg is internally consistent for its Mode.
func (cfg Config) Validate() error {
	switch cfg.Mode {
	case "tcp":
		if cfg.TCP.Host == "" || cfg.TCP.Port == 0 {
			return fmt.Errorf("modbus: tcp.host and tcp.port are required for mode \"tcp\"")
		}
	case "rtu":
		if cfg.RTU.Port == "" {
			return fmt.Errorf("modbus: rtu.port is required for mode \"rtu\"")
		}
	default:
		return fmt.Errorf("modbus: mode must be \"tcp\" or \"rtu\", got %q", cfg.Mode)
	}
	if cfg.SlaveID < 0 || cfg.SlaveID > 255 {
		return fmt.Errorf("modbus: slaveId must be 0-255, got %d", cfg.SlaveID)
	}
	return nil
}

// Open dials cfg's Modbus master (TCP or RTU/serial) and returns a ready
// client plus its underlying closer.
func Open(cfg Config) (modbus.Client, io.Closer, error) {
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	switch cfg.Mode {
	case "tcp":
		h := modbus.NewTCPClientHandler(fmt.Sprintf("%s:%d", cfg.TCP.Host, cfg.TCP.Port))
		h.SlaveId = byte(cfg.SlaveID)
		h.Timeout = timeout
		if err := h.Connect(); err != nil {
			return nil, nil, fmt.Errorf("modbus: connecting to %s:%d: %w", cfg.TCP.Host, cfg.TCP.Port, err)
		}
		return modbus.NewClient(h), h, nil

	case "rtu":
		h := modbus.NewRTUClientHandler(cfg.RTU.Port)
		h.SlaveId = byte(cfg.SlaveID)
		h.Timeout = timeout
		h.BaudRate = cfg.RTU.BaudRate
		if h.BaudRate == 0 {
			h.BaudRate = 19200
		}
		h.DataBits = cfg.RTU.DataBits
		if h.DataBits == 0 {
			h.DataBits = 8
		}
		h.StopBits = cfg.RTU.StopBits
		if h.StopBits == 0 {
			h.StopBits = 1
		}
		switch cfg.RTU.Parity {
		case "", "N":
			h.Parity = "N"
		case "E", "O":
			h.Parity = cfg.RTU.Parity
		default:
			return nil, nil, fmt.Errorf("modbus: rtu.parity must be \"N\", \"E\", or \"O\", got %q", cfg.RTU.Parity)
		}
		if err := h.Connect(); err != nil {
			return nil, nil, fmt.Errorf("modbus: opening %s: %w", cfg.RTU.Port, err)
		}
		return modbus.NewClient(h), h, nil

	default:
		return nil, nil, fmt.Errorf("modbus: unknown mode %q", cfg.Mode)
	}
}
