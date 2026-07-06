// Package httprequest implements the "http-request" node (CON-315 REST API
// Client / SNK-160 HTTP Request): a generic HTTP client usable mid-flow as
// a processor (request per datagram) or as a terminal sink (a Processor
// with its output left unwired, per this codebase's existing convention).
// Retry-with-backoff is deliberately not reimplemented here: returning an
// error lets the per-node ERR-100 policy (already retry-with-backoff
// capable) handle it, so every node gets consistent retry behavior instead
// of each connector inventing its own.
package httprequest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"method": { "type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"] },
		"url": { "type": "string", "description": "Request URL; \"{{path.to.field}}\" substitutes from the input payload (literal templating; full expressions land Increment 7)." },
		"headers": { "type": "object", "description": "Request headers; values support the same {{...}} templating as url." },
		"query": { "type": "object", "description": "Query parameters; values support the same {{...}} templating as url." },
		"bodyFrom": { "type": "string", "enum": ["payload", "none"], "description": "Whether to send the input payload as the JSON request body (default \"payload\" for non-GET methods)." },
		"responseField": { "type": "string", "description": "\".\"-separated path into the parsed JSON response to use as the output payload; empty uses the whole response." },
		"auth": {
			"type": "object",
			"properties": {
				"type": { "type": "string", "enum": ["none", "basic", "bearer", "apikey"], "description": "Secrets come from this node's connection, never literal config." }
			}
		},
		"timeoutMs": { "type": "integer", "minimum": 1, "description": "Request timeout in milliseconds (default 30000)." }
	},
	"required": ["method", "url"]
}`

func init() {
	flow.Register("http-request", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "HTTP Request",
		Category:     flow.CategoryProcessor,
		Description:  "Generic REST API client (CON-315/SNK-160): one request per datagram, usable mid-flow or as a terminal sink.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// AuthConfig declares how outgoing requests authenticate; the secret itself
// comes from the node's connection (SEC-120), resolved at first use.
type AuthConfig struct {
	Type string `json:"type,omitempty"` // "none" (default) | "basic" | "bearer" | "apikey"
}

// Config is the "http-request" node's "config" object.
type Config struct {
	Method        string            `json:"method"`
	URL           string            `json:"url"`
	Headers       map[string]string `json:"headers,omitempty"`
	Query         map[string]string `json:"query,omitempty"`
	BodyFrom      string            `json:"bodyFrom,omitempty"`
	ResponseField string            `json:"responseField,omitempty"`
	Auth          AuthConfig        `json:"auth,omitempty"`
	TimeoutMs     int               `json:"timeoutMs,omitempty"`
}

type node struct {
	cfg    Config
	client *http.Client

	authOnce sync.Once
	authErr  error
	setAuth  func(*http.Request) // no-op until resolved
}

// New is the flow.Factory for the "http-request" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("http-request: url is required")
	}
	if cfg.Method == "" {
		return nil, fmt.Errorf("http-request: method is required")
	}
	timeout := time.Duration(cfg.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &node{cfg: cfg, client: &http.Client{Timeout: timeout}, setAuth: func(*http.Request) {}}, nil
}

type basicCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type bearerCredential struct {
	Token string `json:"token"`
}
type apiKeyCredential struct {
	HeaderName string `json:"headerName"`
	Value      string `json:"value"`
}

// resolveAuth resolves this node's connection at most once (subsequent
// calls reuse the cached setter); a redeploy is needed to pick up rotated
// credentials, a deliberate simplification for request-per-datagram
// throughput.
func (n *node) resolveAuth(ctx context.Context) error {
	n.authOnce.Do(func() {
		switch n.cfg.Auth.Type {
		case "", "none":
			return
		case "basic":
			info, err := flow.ResolveConnection(ctx)
			if err != nil {
				n.authErr = fmt.Errorf("resolving basic auth connection: %w", err)
				return
			}
			var cred basicCredential
			if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
				n.authErr = fmt.Errorf("parsing basic auth credential: %w", err)
				return
			}
			n.setAuth = func(r *http.Request) { r.SetBasicAuth(cred.Username, cred.Password) }
		case "bearer":
			info, err := flow.ResolveConnection(ctx)
			if err != nil {
				n.authErr = fmt.Errorf("resolving bearer auth connection: %w", err)
				return
			}
			var cred bearerCredential
			if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
				n.authErr = fmt.Errorf("parsing bearer auth credential: %w", err)
				return
			}
			n.setAuth = func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+cred.Token) }
		case "apikey":
			info, err := flow.ResolveConnection(ctx)
			if err != nil {
				n.authErr = fmt.Errorf("resolving apikey auth connection: %w", err)
				return
			}
			var cred apiKeyCredential
			if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
				n.authErr = fmt.Errorf("parsing apikey credential: %w", err)
				return
			}
			n.setAuth = func(r *http.Request) { r.Header.Set(cred.HeaderName, cred.Value) }
		default:
			n.authErr = fmt.Errorf("unknown auth type %q", n.cfg.Auth.Type)
		}
	})
	return n.authErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	if err := n.resolveAuth(ctx); err != nil {
		return nil, fmt.Errorf("http-request: %w", err)
	}

	url := renderTemplate(n.cfg.URL, in.Payload.Value)
	if len(n.cfg.Query) > 0 {
		q := make([]string, 0, len(n.cfg.Query))
		for k, v := range n.cfg.Query {
			q = append(q, k+"="+renderTemplate(v, in.Payload.Value))
		}
		sep := "?"
		if strings.Contains(url, "?") {
			sep = "&"
		}
		url += sep + strings.Join(q, "&")
	}

	var body io.Reader
	bodyFrom := n.cfg.BodyFrom
	if bodyFrom == "" && n.cfg.Method != http.MethodGet && n.cfg.Method != http.MethodDelete {
		bodyFrom = "payload"
	}
	if bodyFrom == "payload" {
		b, err := json.Marshal(in.Payload.Value)
		if err != nil {
			return nil, fmt.Errorf("http-request: encoding request body: %w", err)
		}
		body = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, n.cfg.Method, url, body)
	if err != nil {
		return nil, fmt.Errorf("http-request: building request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range n.cfg.Headers {
		req.Header.Set(k, renderTemplate(v, in.Payload.Value))
	}
	n.setAuth(req)

	resp, err := n.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http-request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("http-request: reading response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http-request: %s %s returned %d: %s", n.cfg.Method, url, resp.StatusCode, truncate(respBody, 500))
	}

	value := parseResponse(respBody, n.cfg.ResponseField)
	out := datagram.NewCaused(in, datagram.Source{NodeID: "http-request", Origin: "http"}, datagram.Payload{Value: value})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

var templateToken = regexp.MustCompile(`\{\{\s*([^}]+?)\s*\}\}`)

// renderTemplate substitutes every "{{path}}" in tmpl with the value at
// that dot-path in payload (the minimal literal-path style already used by
// the "set"/"debug-log" nodes; full expression support is Increment 7).
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

// parseResponse decodes body as JSON when possible (selecting responseField
// if set), falling back to the raw string otherwise.
func parseResponse(body []byte, responseField string) any {
	var decoded any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return string(body)
	}
	if responseField == "" {
		return decoded
	}
	return evalPath(decoded, responseField)
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}
