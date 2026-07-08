// Package redissink implements the "redis-sink" node (SNK-200 NoSQL: Redis
// set/publish/stream-add).
package redissink

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/redisshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["set", "publish", "streamAdd"] },
		"key": { "type": "string", "description": "Key template for mode \"set\" (use {{ field }} to reference a payload field)." },
		"valueField": { "type": "string", "description": "Payload field written as the value, mode \"set\" (default: the whole payload, JSON-encoded if not a string)." },
		"ttlMs": { "type": "integer", "minimum": 0, "description": "Optional TTL for mode \"set\"; 0 = no expiry." },
		"channel": { "type": "string", "description": "Pub/sub channel, mode \"publish\"." },
		"streamKey": { "type": "string", "description": "Stream key, mode \"streamAdd\"." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("redis-sink", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Redis Sink",
		Category:     flow.CategoryProcessor,
		Description:  "SET a key, PUBLISH to a channel, or XADD to a stream (SNK-200).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "redis-sink" node's "config" object.
type Config struct {
	Mode       string `json:"mode"`
	Key        string `json:"key,omitempty"`
	ValueField string `json:"valueField,omitempty"`
	TTLMs      int    `json:"ttlMs,omitempty"`
	Channel    string `json:"channel,omitempty"`
	StreamKey  string `json:"streamKey,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	client      *redis.Client
	connectErr  error
}

// New is the flow.Factory for the "redis-sink" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "set":
		if cfg.Key == "" {
			return nil, fmt.Errorf("redis-sink: key is required for mode \"set\"")
		}
	case "publish":
		if cfg.Channel == "" {
			return nil, fmt.Errorf("redis-sink: channel is required for mode \"publish\"")
		}
	case "streamAdd":
		if cfg.StreamKey == "" {
			return nil, fmt.Errorf("redis-sink: streamKey is required for mode \"streamAdd\"")
		}
	default:
		return nil, fmt.Errorf("redis-sink: mode must be \"set\", \"publish\", or \"streamAdd\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) connect(ctx context.Context) (*redis.Client, error) {
	n.connectOnce.Do(func() { n.client, n.connectErr = redisshared.Connect(ctx) })
	return n.client, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	client, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis-sink: %w", err)
	}

	switch n.cfg.Mode {
	case "set":
		if err := n.doSet(ctx, client, in); err != nil {
			return nil, fmt.Errorf("redis-sink: %w", err)
		}
	case "publish":
		if err := client.Publish(ctx, n.cfg.Channel, n.stringValue(in)).Err(); err != nil {
			return nil, fmt.Errorf("redis-sink: PUBLISH: %w", err)
		}
	case "streamAdd":
		fields, ok := in.Payload.Value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("redis-sink: mode \"streamAdd\" requires an object payload")
		}
		if err := client.XAdd(ctx, &redis.XAddArgs{Stream: n.cfg.StreamKey, Values: fields}).Err(); err != nil {
			return nil, fmt.Errorf("redis-sink: XADD: %w", err)
		}
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) doSet(ctx context.Context, client *redis.Client, in datagram.Datagram) error {
	key := n.cfg.Key
	if m, ok := in.Payload.Value.(map[string]any); ok {
		if v, ok := m["key"]; ok {
			if s, ok := v.(string); ok && n.cfg.Key == "" {
				key = s
			}
		}
	}
	var ttl time.Duration
	if n.cfg.TTLMs > 0 {
		ttl = time.Duration(n.cfg.TTLMs) * time.Millisecond
	}
	return client.Set(ctx, key, n.stringValue(in), ttl).Err()
}

func (n *node) stringValue(in datagram.Datagram) string {
	value := in.Payload.Value
	if n.cfg.ValueField != "" {
		if m, ok := in.Payload.Value.(map[string]any); ok {
			value = m[n.cfg.ValueField]
		}
	}
	if s, ok := value.(string); ok {
		return s
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%v", value)
	}
	return string(raw)
}
