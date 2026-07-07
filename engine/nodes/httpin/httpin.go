// Package httpin implements the "http-in" node (CON-300 HTTP In/Webhook):
// a configurable HTTP(S) endpoint whose requests become datagrams, with an
// optional paired "http-response" node (SNK-170) for a synchronous reply.
// Auth secrets (basic password, header key value, HMAC secret) come from
// the node's connection, never literal config (SEC-120) — see
// engine/flow.ResolveConnection.
package httpin

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/webhook"
)

// DefaultResponseTimeout bounds how long a request waits for a paired
// "http-response" node before falling back to a default 200 (never hangs
// forever — BUS-110's "nothing buffers/waits unboundedly" applies here
// too).
const DefaultResponseTimeout = 30 * time.Second

const configSchema = `{
	"type": "object",
	"properties": {
		"path": { "type": "string", "description": "URL path to expose, e.g. \"/hooks/orders\"." },
		"method": { "type": "string", "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"], "description": "HTTP method this endpoint accepts." },
		"auth": {
			"type": "object",
			"properties": {
				"type": { "type": "string", "enum": ["none", "basic", "header", "hmac"], "description": "How incoming requests must authenticate; secrets come from this node's connection, never from literal config." }
			}
		},
		"responseTimeoutMs": { "type": "integer", "minimum": 1, "description": "How long to wait for a paired http-response node before replying 200 by default (default 30000)." }
	},
	"required": ["path", "method"]
}`

func init() {
	flow.Register("http-in", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Trigger:      true, // each HTTP request becomes a tracked execution (ENG-100/ENG-130)
		Outputs:      []string{"out"},
		DisplayName:  "HTTP In",
		Category:     flow.CategorySource,
		Description:  "Exposes an HTTP(S) endpoint; each request becomes a datagram (CON-300). Pair with 'HTTP Response' for a synchronous reply.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// AuthConfig declares how incoming requests must authenticate; type is the
// only non-secret part, kept in node config, while the actual secret value
// (password / header value / HMAC key) is resolved from the node's
// connection at Run time.
type AuthConfig struct {
	Type string `json:"type,omitempty"` // "none" (default) | "basic" | "header" | "hmac"
}

// Config is the "http-in" node's "config" object.
type Config struct {
	Path              string     `json:"path"`
	Method            string     `json:"method"`
	Auth              AuthConfig `json:"auth,omitempty"`
	ResponseTimeoutMs int        `json:"responseTimeoutMs,omitempty"`
}

type node struct{ cfg Config }

// TriggerKind reports http-in's ENG-130 trigger-kind label ("webhook") for
// execution-history display (flow.TriggerKindProvider).
func (n *node) TriggerKind() string { return "webhook" }

// New is the flow.Factory for the "http-in" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Path == "" {
		return nil, fmt.Errorf("http-in: path is required")
	}
	if cfg.Method == "" {
		return nil, fmt.Errorf("http-in: method is required")
	}
	return &node{cfg: cfg}, nil
}

// basicCredential/headerCredential/hmacCredential are the CredentialJSON
// shapes expected for each auth type, resolved via flow.ResolveConnection.
type basicCredential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
type headerCredential struct {
	HeaderName  string `json:"headerName"`
	HeaderValue string `json:"headerValue"`
}
type hmacCredential struct {
	HeaderName string `json:"headerName"`
	Secret     string `json:"secret"`
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	checkAuth, err := n.buildAuthChecker(ctx)
	if err != nil {
		return fmt.Errorf("http-in: %w", err)
	}

	timeout := time.Duration(n.cfg.ResponseTimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = DefaultResponseTimeout
	}

	unregister := webhook.DefaultRegistry.Register(n.cfg.Method, n.cfg.Path, n.handler(emit, checkAuth, timeout))
	defer unregister()

	<-ctx.Done()
	return nil
}

func (n *node) buildAuthChecker(ctx context.Context) (func(*http.Request, []byte) bool, error) {
	switch n.cfg.Auth.Type {
	case "", "none":
		return func(*http.Request, []byte) bool { return true }, nil
	case "basic":
		info, err := flow.ResolveConnection(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolving basic auth connection: %w", err)
		}
		var cred basicCredential
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("parsing basic auth credential: %w", err)
		}
		return func(r *http.Request, _ []byte) bool {
			u, p, ok := r.BasicAuth()
			return ok && subtle.ConstantTimeCompare([]byte(u), []byte(cred.Username)) == 1 && subtle.ConstantTimeCompare([]byte(p), []byte(cred.Password)) == 1
		}, nil
	case "header":
		info, err := flow.ResolveConnection(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolving header auth connection: %w", err)
		}
		var cred headerCredential
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("parsing header auth credential: %w", err)
		}
		return func(r *http.Request, _ []byte) bool {
			return subtle.ConstantTimeCompare([]byte(r.Header.Get(cred.HeaderName)), []byte(cred.HeaderValue)) == 1
		}, nil
	case "hmac":
		info, err := flow.ResolveConnection(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolving hmac auth connection: %w", err)
		}
		var cred hmacCredential
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("parsing hmac credential: %w", err)
		}
		return func(r *http.Request, body []byte) bool {
			mac := hmac.New(sha256.New, []byte(cred.Secret))
			mac.Write(body)
			expected := hex.EncodeToString(mac.Sum(nil))
			return subtle.ConstantTimeCompare([]byte(r.Header.Get(cred.HeaderName)), []byte(expected)) == 1
		}, nil
	default:
		return nil, fmt.Errorf("unknown auth type %q", n.cfg.Auth.Type)
	}
}

func (n *node) handler(emit func(port string, d datagram.Datagram) error, checkAuth func(*http.Request, []byte) bool, timeout time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "reading body", http.StatusBadRequest)
			return
		}

		if !checkAuth(r, body) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		headers := map[string]string{}
		for k := range r.Header {
			headers[k] = r.Header.Get(k)
		}
		query := map[string]string{}
		for k := range r.URL.Query() {
			query[k] = r.URL.Query().Get(k)
		}
		d := datagram.New(datagram.Source{NodeID: "http-in", Origin: "http"}, datagram.Payload{Value: map[string]any{
			"method":  r.Method,
			"path":    r.URL.Path,
			"headers": headers,
			"query":   query,
			"body":    string(body),
		}})

		respCh, cancel := webhook.Default.Await(d.Header.ID)
		defer cancel()

		if err := emit("out", d); err != nil {
			if errors.Is(err, flow.ErrConcurrencyRejected) {
				http.Error(w, "too many concurrent executions", http.StatusTooManyRequests)
				return
			}
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}

		select {
		case resp := <-respCh:
			for k, v := range resp.Headers {
				w.Header().Set(k, v)
			}
			status := resp.Status
			if status == 0 {
				status = http.StatusOK
			}
			w.WriteHeader(status)
			_, _ = w.Write(resp.Body)
		case <-time.After(timeout):
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
		}
	}
}
