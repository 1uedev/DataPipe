// Package kafkaout implements the "kafka-out" node (SNK: Kafka producer).
package kafkaout

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/kafkashared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"topic": { "type": "string" },
		"keyField": { "type": "string", "description": "Payload field used as the message key (default: none, key omitted)." }
	},
	"required": ["topic"]
}`

func init() {
	flow.Register("kafka-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Kafka Producer",
		Category:     flow.CategoryProcessor,
		Description:  "Produce the incoming datagram's payload (JSON-encoded) to a Kafka topic.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "kafka-out" node's "config" object.
type Config struct {
	Topic    string `json:"topic"`
	KeyField string `json:"keyField,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	writer      *kafka.Writer
	connectErr  error
}

// New is the flow.Factory for the "kafka-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafka-out: topic is required")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) connect(ctx context.Context) (*kafka.Writer, error) {
	n.connectOnce.Do(func() {
		resolved, err := kafkashared.Resolve(ctx)
		if err != nil {
			n.connectErr = err
			return
		}
		n.writer = &kafka.Writer{
			Addr:     kafka.TCP(resolved.Brokers...),
			Topic:    n.cfg.Topic,
			Balancer: &kafka.LeastBytes{},
			Transport: &kafka.Transport{
				Dial: resolved.Dialer.DialFunc,
				SASL: resolved.Dialer.SASLMechanism,
				TLS:  resolved.Dialer.TLS,
			},
		}
	})
	return n.writer, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	writer, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("kafka-out: %w", err)
	}

	value, err := json.Marshal(in.Payload.Value)
	if err != nil {
		return nil, fmt.Errorf("kafka-out: encoding payload: %w", err)
	}
	msg := kafka.Message{Value: value}
	if n.cfg.KeyField != "" {
		if m, ok := in.Payload.Value.(map[string]any); ok {
			if k, ok := m[n.cfg.KeyField].(string); ok {
				msg.Key = []byte(k)
			}
		}
	}
	if err := writer.WriteMessages(ctx, msg); err != nil {
		return nil, fmt.Errorf("kafka-out: writing message: %w", err)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}
