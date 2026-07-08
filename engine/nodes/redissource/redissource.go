// Package redissource implements the "redis-source" node (CON-520 NoSQL:
// Redis get/scan, pub/sub subscribe, streams).
package redissource

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/redisshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["poll", "subscribe", "stream"] },
		"key": { "type": "string", "description": "Single key to GET, mode \"poll\"." },
		"keyPattern": { "type": "string", "description": "SCAN MATCH pattern; emits one datagram per matching key, mode \"poll\". Takes precedence over \"key\" if both are set." },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "Poll period, mode \"poll\"." },
		"channel": { "type": "string", "description": "Pub/sub channel to SUBSCRIBE, mode \"subscribe\"." },
		"streamKey": { "type": "string", "description": "Stream key to XREAD, mode \"stream\"." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("redis-source", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "Redis Source",
		Category:     flow.CategorySource,
		Description:  "Poll a key/pattern, subscribe to a pub/sub channel, or read a stream (CON-520).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "redis-source" node's "config" object.
type Config struct {
	Mode       string `json:"mode"`
	Key        string `json:"key,omitempty"`
	KeyPattern string `json:"keyPattern,omitempty"`
	IntervalMs int    `json:"intervalMs,omitempty"`
	Channel    string `json:"channel,omitempty"`
	StreamKey  string `json:"streamKey,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "redis-source" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "poll":
		if cfg.Key == "" && cfg.KeyPattern == "" {
			return nil, fmt.Errorf("redis-source: key or keyPattern is required for mode \"poll\"")
		}
		if cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("redis-source: intervalMs must be positive for mode \"poll\"")
		}
	case "subscribe":
		if cfg.Channel == "" {
			return nil, fmt.Errorf("redis-source: channel is required for mode \"subscribe\"")
		}
	case "stream":
		if cfg.StreamKey == "" {
			return nil, fmt.Errorf("redis-source: streamKey is required for mode \"stream\"")
		}
	default:
		return nil, fmt.Errorf("redis-source: mode must be \"poll\", \"subscribe\", or \"stream\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	client, err := redisshared.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = client.Close() }()

	switch n.cfg.Mode {
	case "poll":
		return n.runPoll(ctx, client, emit)
	case "subscribe":
		return n.runSubscribe(ctx, client, emit)
	case "stream":
		return n.runStream(ctx, client, emit)
	default:
		return fmt.Errorf("redis-source: unhandled mode %q", n.cfg.Mode)
	}
}

func (n *node) runPoll(ctx context.Context, client *redis.Client, emit func(string, datagram.Datagram) error) error {
	ticker := time.NewTicker(time.Duration(n.cfg.IntervalMs) * time.Millisecond)
	defer ticker.Stop()
	poll := func() error {
		if n.cfg.KeyPattern != "" {
			return n.pollPattern(ctx, client, emit)
		}
		return n.pollKey(ctx, client, n.cfg.Key, emit)
	}
	if err := poll(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := poll(); err != nil {
				return err
			}
		}
	}
}

func (n *node) pollKey(ctx context.Context, client *redis.Client, key string, emit func(string, datagram.Datagram) error) error {
	val, err := client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("redis-source: GET %s: %w", key, err)
	}
	d := datagram.New(datagram.Source{NodeID: "redis-source"}, datagram.Payload{Value: map[string]any{"key": key, "value": val}})
	return emit("out", d)
}

func (n *node) pollPattern(ctx context.Context, client *redis.Client, emit func(string, datagram.Datagram) error) error {
	var cursor uint64
	for {
		keys, next, err := client.Scan(ctx, cursor, n.cfg.KeyPattern, 100).Result()
		if err != nil {
			return fmt.Errorf("redis-source: SCAN: %w", err)
		}
		for _, k := range keys {
			if err := n.pollKey(ctx, client, k, emit); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

func (n *node) runSubscribe(ctx context.Context, client *redis.Client, emit func(string, datagram.Datagram) error) error {
	sub := client.Subscribe(ctx, n.cfg.Channel)
	defer func() { _ = sub.Close() }()

	ch := sub.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			d := datagram.New(datagram.Source{NodeID: "redis-source"}, datagram.Payload{Value: map[string]any{"channel": msg.Channel, "payload": msg.Payload}})
			if err := emit("out", d); err != nil {
				return err
			}
		}
	}
}

func (n *node) runStream(ctx context.Context, client *redis.Client, emit func(string, datagram.Datagram) error) error {
	lastID := "$" // only new entries from now on
	for {
		if ctx.Err() != nil {
			return nil
		}
		res, err := client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{n.cfg.StreamKey, lastID},
			Block:   5 * time.Second,
			Count:   100,
		}).Result()
		if err == redis.Nil {
			continue // timed out waiting, poll again
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("redis-source: XREAD: %w", err)
		}
		for _, stream := range res {
			for _, msg := range stream.Messages {
				lastID = msg.ID
				value := map[string]any{"id": msg.ID, "values": msg.Values}
				d := datagram.New(datagram.Source{NodeID: "redis-source"}, datagram.Payload{Value: value})
				if err := emit("out", d); err != nil {
					return err
				}
			}
		}
	}
}
