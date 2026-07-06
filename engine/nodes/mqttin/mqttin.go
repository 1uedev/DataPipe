// Package mqttin implements the "mqtt-in" node (CON-200 MQTT, subscribe
// side): subscribes to a wildcard topic pattern at a configured QoS,
// emitting one datagram per received message. Broker URL and credentials
// come from the node's connection (SEC-120), never literal config.
package mqttin

import (
	"context"
	"encoding/json"
	"fmt"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/mqttshared"
)

// InboxCapacity bounds the buffer between the MQTT client's callback
// goroutine and this node's emit loop (BUS-110: every queue has a bound and
// an overflow policy) — a slow downstream drops the oldest unread message
// rather than blocking the MQTT client's internal read loop indefinitely.
const InboxCapacity = 256

const configSchema = `{
	"type": "object",
	"properties": {
		"topic": { "type": "string", "description": "Topic filter, MQTT wildcards supported (\"+\" one level, \"#\" the rest)." },
		"qos": { "type": "integer", "enum": [0, 1, 2], "description": "Subscription QoS (default 0)." }
	},
	"required": ["topic"]
}`

func init() {
	flow.Register("mqtt-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "MQTT In",
		Category:     flow.CategorySource,
		Description:  "Subscribes to an MQTT topic (CON-200): wildcards, QoS 0/1/2.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "mqtt-in" node's "config" object.
type Config struct {
	Topic string `json:"topic"`
	QoS   int    `json:"qos,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "mqtt-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("mqtt-in: topic is required")
	}
	if cfg.QoS < 0 || cfg.QoS > 2 {
		return nil, fmt.Errorf("mqtt-in: qos must be 0, 1, or 2")
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	client, err := mqttshared.Connect(ctx, "in-"+mqttshared.RandSuffix())
	if err != nil {
		return err
	}
	defer client.Disconnect(250)

	msgs := make(chan mqtt.Message, InboxCapacity)
	token := client.Subscribe(n.cfg.Topic, byte(n.cfg.QoS), func(_ mqtt.Client, msg mqtt.Message) {
		select {
		case msgs <- msg:
		default: // bounded inbox full: drop rather than block the client's read loop
		}
	})
	if !token.WaitTimeout(mqttshared.ConnectTimeout) {
		return fmt.Errorf("mqtt-in: subscribe timed out")
	}
	if err := token.Error(); err != nil {
		return fmt.Errorf("mqtt-in: subscribe: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case msg := <-msgs:
			d := datagram.New(datagram.Source{NodeID: "mqtt-in", Origin: msg.Topic()}, datagram.Payload{Value: decodePayload(msg.Payload())})
			d.Header.Tags = map[string]string{"topic": msg.Topic()}
			if err := emit("out", d); err != nil {
				return err
			}
		}
	}
}

// decodePayload parses the message body as JSON when possible, falling
// back to the raw string.
func decodePayload(raw []byte) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return string(raw)
	}
	return v
}
