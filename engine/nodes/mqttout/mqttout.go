// Package mqttout implements the "mqtt-out" node (SNK-110 MQTT Out):
// publishes each datagram to a topic (with simple {{path}} templating from
// the payload) at a configured QoS/retain, passing the datagram through
// unchanged. Broker URL and credentials come from the node's connection.
package mqttout

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/mqttshared"
)

// PublishTimeout bounds how long a single publish waits for
// acknowledgement (QoS 1/2) before it's treated as a failure.
const PublishTimeout = 10 * time.Second

const configSchema = `{
	"type": "object",
	"properties": {
		"topic": { "type": "string", "description": "Topic to publish to; \"{{path.to.field}}\" substitutes from the input payload." },
		"qos": { "type": "integer", "enum": [0, 1, 2], "description": "Publish QoS (default 0)." },
		"retain": { "type": "boolean" }
	},
	"required": ["topic"]
}`

func init() {
	flow.Register("mqtt-out", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "MQTT Out",
		Category:     flow.CategoryProcessor,
		Description:  "Publishes to an MQTT topic (SNK-110): QoS, retain, topic templating from the payload.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "mqtt-out" node's "config" object.
type Config struct {
	Topic  string `json:"topic"`
	QoS    int    `json:"qos,omitempty"`
	Retain bool   `json:"retain,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	client      mqtt.Client
	connectErr  error
}

// New is the flow.Factory for the "mqtt-out" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("mqtt-out: topic is required")
	}
	if cfg.QoS < 0 || cfg.QoS > 2 {
		return nil, fmt.Errorf("mqtt-out: qos must be 0, 1, or 2")
	}
	return &node{cfg: cfg}, nil
}

// connect connects at most once per node instance; a redeploy is needed to
// pick up a changed connection, the same tradeoff as http-request's cached
// auth (request-per-datagram throughput vs. per-call re-resolution).
func (n *node) connect(ctx context.Context) (mqtt.Client, error) {
	n.connectOnce.Do(func() {
		n.client, n.connectErr = mqttshared.Connect(ctx, "out-"+mqttshared.RandSuffix())
	})
	return n.client, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	client, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("mqtt-out: %w", err)
	}

	topic := renderTemplate(n.cfg.Topic, in.Payload.Value)
	payload, err := encodePayload(in.Payload.Value)
	if err != nil {
		return nil, fmt.Errorf("mqtt-out: encoding payload: %w", err)
	}

	token := client.Publish(topic, byte(n.cfg.QoS), n.cfg.Retain, payload)
	if !token.WaitTimeout(PublishTimeout) {
		return nil, fmt.Errorf("mqtt-out: publish to %q timed out", topic)
	}
	if err := token.Error(); err != nil {
		return nil, fmt.Errorf("mqtt-out: publish to %q: %w", topic, err)
	}

	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

// encodePayload sends a string payload verbatim; anything else is
// JSON-encoded.
func encodePayload(value any) ([]byte, error) {
	if s, ok := value.(string); ok {
		return []byte(s), nil
	}
	return json.Marshal(value)
}

var templateToken = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)

// renderTemplate is the same minimal "{{dot.path}}" substitution used by
// the "http-request" node; kept as its own small copy rather than a shared
// dependency, consistent with this codebase's existing per-node literal
// helpers (e.g. "set"/"debug-log").
func renderTemplate(tmpl string, payload any) string {
	return templateToken.ReplaceAllStringFunc(tmpl, func(match string) string {
		path := templateToken.FindStringSubmatch(match)[1]
		v := evalPath(payload, path)
		if v == nil {
			return ""
		}
		if s, ok := v.(string); ok {
			return s
		}
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	})
}

func evalPath(root any, path string) any {
	if path == "" || path == "payload" {
		return root
	}
	cur := root
	for _, k := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[k]
	}
	return cur
}
