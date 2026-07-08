// Package kafkain implements the "kafka-in" node (CON-260): consumer group
// with offset management (earliest/latest/committed), key/value
// deserialization (JSON/string/binary; Avro via schema registry is P2, not
// implemented — see TODO.md), headers into datagram tags.
package kafkain

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/segmentio/kafka-go"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/kafkashared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"topic": { "type": "string" },
		"groupID": { "type": "string", "description": "Consumer group id; required — kafka-go always joins a group so the runtime can restart without reprocessing from the start." },
		"startOffset": { "type": "string", "enum": ["earliest", "latest"], "default": "latest", "description": "Where to start when the group has no committed offset yet. Once a group has committed offsets, resumption always continues from there regardless of this setting." },
		"valueFormat": { "type": "string", "enum": ["json", "string", "binary"], "default": "json" }
	},
	"required": ["topic", "groupID"]
}`

func init() {
	flow.Register("kafka-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Kafka Consumer",
		Category:     flow.CategorySource,
		Description:  "Consumer group with offset management, one datagram per message (CON-260).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "kafka-in" node's "config" object.
type Config struct {
	Topic       string `json:"topic"`
	GroupID     string `json:"groupID"`
	StartOffset string `json:"startOffset,omitempty"`
	ValueFormat string `json:"valueFormat,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "kafka-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka-in: topic is required")
	}
	if cfg.GroupID == "" {
		return nil, fmt.Errorf("kafka-in: groupID is required")
	}
	if cfg.StartOffset == "" {
		cfg.StartOffset = "latest"
	}
	if cfg.StartOffset != "earliest" && cfg.StartOffset != "latest" {
		return nil, fmt.Errorf("kafka-in: startOffset must be \"earliest\" or \"latest\", got %q", cfg.StartOffset)
	}
	if cfg.ValueFormat == "" {
		cfg.ValueFormat = "json"
	}
	if cfg.ValueFormat != "json" && cfg.ValueFormat != "string" && cfg.ValueFormat != "binary" {
		return nil, fmt.Errorf("kafka-in: valueFormat must be \"json\", \"string\", or \"binary\", got %q", cfg.ValueFormat)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	resolved, err := kafkashared.Resolve(ctx)
	if err != nil {
		return err
	}
	startOffset := kafka.LastOffset
	if n.cfg.StartOffset == "earliest" {
		startOffset = kafka.FirstOffset
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     resolved.Brokers,
		Dialer:      resolved.Dialer,
		Topic:       n.cfg.Topic,
		GroupID:     n.cfg.GroupID,
		StartOffset: startOffset,
	})
	defer func() { _ = reader.Close() }()

	for {
		msg, err := reader.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("kafka-in: reading message: %w", err)
		}
		value, err := n.decodeValue(msg.Value)
		if err != nil {
			return fmt.Errorf("kafka-in: %w", err)
		}
		d := datagram.New(datagram.Source{NodeID: "kafka-in"}, datagram.Payload{Value: value})
		d.Header.Tags = make(map[string]string, len(msg.Headers)+2)
		d.Header.Tags["kafka.topic"] = msg.Topic
		if len(msg.Key) > 0 {
			d.Header.Tags["kafka.key"] = string(msg.Key)
		}
		for _, h := range msg.Headers {
			d.Header.Tags[h.Key] = string(h.Value)
		}
		if err := emit("out", d); err != nil {
			return err
		}
	}
}

func (n *node) decodeValue(raw []byte) (any, error) {
	switch n.cfg.ValueFormat {
	case "string":
		return string(raw), nil
	case "binary":
		return base64.StdEncoding.EncodeToString(raw), nil
	default: // json
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("decoding JSON value: %w", err)
		}
		return v, nil
	}
}
