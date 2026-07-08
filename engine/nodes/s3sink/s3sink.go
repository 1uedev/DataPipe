// Package s3sink implements the "s3-sink" node (SNK-180's S3-compatible
// target clause / Increment 10 MVP catalog "S3 files"): writes the incoming
// datagram's payload as an object (atomic — PutObject either fully succeeds
// or fails, there is no partial-write state to clean up).
package s3sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/s3shared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"key": { "type": "string", "description": "Object key template; \"{{ field }}\" references a payload field, plus \"{{ timestamp }}\" for a unix-nano suffix." },
		"format": { "type": "string", "enum": ["json", "raw"], "default": "json", "description": "\"json\" JSON-encodes the payload; \"raw\" writes a string payload's bytes directly." },
		"contentType": { "type": "string" }
	},
	"required": ["key"]
}`

func init() {
	flow.Register("s3-sink", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "S3 Sink",
		Category:     flow.CategoryProcessor,
		Description:  "Write the incoming datagram's payload as an object (S3-compatible object storage).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "s3-sink" node's "config" object.
type Config struct {
	Key         string `json:"key"`
	Format      string `json:"format,omitempty"`
	ContentType string `json:"contentType,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	resolved    s3shared.Resolved
	connectErr  error
}

// New is the flow.Factory for the "s3-sink" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Key == "" {
		return nil, fmt.Errorf("s3-sink: key is required")
	}
	if cfg.Format == "" {
		cfg.Format = "json"
	}
	if cfg.Format != "json" && cfg.Format != "raw" {
		return nil, fmt.Errorf("s3-sink: format must be \"json\" or \"raw\", got %q", cfg.Format)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) connect(ctx context.Context) (s3shared.Resolved, error) {
	n.connectOnce.Do(func() { n.resolved, n.connectErr = s3shared.Connect(ctx) })
	return n.resolved, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	resolved, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("s3-sink: %w", err)
	}

	body, err := n.encodeBody(in)
	if err != nil {
		return nil, fmt.Errorf("s3-sink: %w", err)
	}
	key := n.resolveKey(in)

	input := &s3.PutObjectInput{Bucket: aws.String(resolved.Bucket), Key: aws.String(key), Body: bytes.NewReader(body)}
	if n.cfg.ContentType != "" {
		input.ContentType = aws.String(n.cfg.ContentType)
	}
	if _, err := resolved.Client.PutObject(ctx, input); err != nil {
		return nil, fmt.Errorf("s3-sink: putting object %q: %w", key, err)
	}
	return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
}

func (n *node) encodeBody(in datagram.Datagram) ([]byte, error) {
	if n.cfg.Format == "raw" {
		if s, ok := in.Payload.Value.(string); ok {
			return []byte(s), nil
		}
		return nil, fmt.Errorf("format \"raw\" requires a string payload, got %T", in.Payload.Value)
	}
	return json.Marshal(in.Payload.Value)
}

func (n *node) resolveKey(in datagram.Datagram) string {
	key := n.cfg.Key
	key = strings.ReplaceAll(key, "{{ timestamp }}", fmt.Sprintf("%d", time.Now().UnixNano()))
	if m, ok := in.Payload.Value.(map[string]any); ok {
		for field, v := range m {
			placeholder := fmt.Sprintf("{{ %s }}", field)
			if strings.Contains(key, placeholder) {
				key = strings.ReplaceAll(key, placeholder, fmt.Sprint(v))
			}
		}
	}
	return key
}
