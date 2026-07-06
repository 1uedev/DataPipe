// Package httpresponse implements the "http-response" node (SNK-170: "...
// respond to CON-300 requests"): replies to the "http-in" request that
// produced (via DGM-160 lineage) the datagram it receives.
package httpresponse

import (
	"context"
	"encoding/json"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/webhook"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"status": { "type": "integer", "minimum": 100, "maximum": 599, "description": "HTTP status code to reply with (default 200)." },
		"headers": { "type": "object", "description": "Additional response headers." }
	}
}`

func init() {
	flow.Register("http-response", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		DisplayName:  "HTTP Response",
		Category:     flow.CategorySink,
		Description:  "Replies to the http-in request that produced this datagram's lineage (SNK-170).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "http-response" node's "config" object.
type Config struct {
	Status  int               `json:"status,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

type node struct{ cfg Config }

// New is the flow.Factory for the "http-response" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &cfg); err != nil {
			return nil, err
		}
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(_ context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	status := n.cfg.Status
	if status == 0 {
		status = 200
	}
	body, contentType := responseBody(in.Payload.Value)

	headers := make(map[string]string, len(n.cfg.Headers)+1)
	if contentType != "" {
		headers["Content-Type"] = contentType
	}
	for k, v := range n.cfg.Headers {
		headers[k] = v // explicit config wins over the inferred default
	}

	webhook.Default.Reply(in.Header.CorrelationID, webhook.Response{
		Status:  status,
		Headers: headers,
		Body:    body,
	})
	return nil, nil
}

// responseBody renders a string payload verbatim (text/plain); anything
// else is JSON-encoded (application/json).
func responseBody(value any) (body []byte, contentType string) {
	if s, ok := value.(string); ok {
		return []byte(s), "text/plain; charset=utf-8"
	}
	b, err := json.Marshal(value)
	if err != nil {
		return nil, ""
	}
	return b, "application/json"
}
