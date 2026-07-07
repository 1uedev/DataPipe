// Package lookup implements the "lookup" node (PROC-400): enrich a
// datagram from a static table, SQL (PostgreSQL, reusing engine/nodes/
// sqlshared), or HTTP source, with an in-memory TTL+max-entries cache and a
// configurable cache-miss policy. NoSQL is not implemented — no NoSQL
// connector exists in this project yet (see TODO.md).
package lookup

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/nodeutil"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"keyExpression": { "type": "string", "description": "JavaScript expression producing the lookup key." },
		"source": { "type": "string", "enum": ["static", "sql", "http"] },
		"static": {
			"type": "object",
			"description": "mode \"static\": a literal key -> value table.",
			"additionalProperties": true
		},
		"sql": {
			"type": "object",
			"properties": { "query": { "type": "string", "description": "Postgres-style \"$1\" placeholder for the key." } }
		},
		"http": {
			"type": "object",
			"properties": { "urlTemplate": { "type": "string", "description": "A literal \"{key}\" placeholder is replaced with the lookup key." } }
		},
		"cache": {
			"type": "object",
			"properties": {
				"ttlMs": { "type": "integer", "minimum": 0, "description": "0 disables expiry (entries only evicted by maxEntries)." },
				"maxEntries": { "type": "integer", "minimum": 1, "default": 1000 }
			}
		},
		"cacheMissPolicy": { "type": "string", "enum": ["fail", "passthrough", "default"], "default": "fail" },
		"defaultValue": { "description": "Used when cacheMissPolicy is \"default\"." },
		"as": { "type": "string", "description": "\".\"-separated path where the looked-up value is written into the payload." }
	},
	"required": ["keyExpression", "source", "as"]
}`

func init() {
	flow.Register("lookup", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Lookup",
		Category:     flow.CategoryProcessor,
		Description:  "Enrich from a static table, SQL, or HTTP source with an in-memory TTL cache and cache-miss policy (PROC-400).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// CacheConfig controls the in-memory cache.
type CacheConfig struct {
	TTLMs      int `json:"ttlMs,omitempty"`
	MaxEntries int `json:"maxEntries,omitempty"`
}

// SQLConfig is mode "sql"'s config.
type SQLConfig struct {
	Query string `json:"query"`
}

// HTTPConfig is mode "http"'s config.
type HTTPConfig struct {
	URLTemplate string `json:"urlTemplate"`
}

// Config is the "lookup" node's "config" object.
type Config struct {
	KeyExpression   string         `json:"keyExpression"`
	Source          string         `json:"source"`
	Static          map[string]any `json:"static,omitempty"`
	SQL             SQLConfig      `json:"sql,omitempty"`
	HTTP            HTTPConfig     `json:"http,omitempty"`
	Cache           CacheConfig    `json:"cache,omitempty"`
	CacheMissPolicy string         `json:"cacheMissPolicy,omitempty"`
	DefaultValue    any            `json:"defaultValue,omitempty"`
	As              string         `json:"as"`
}

const DefaultMaxEntries = 1000

type cacheEntry struct {
	value     any
	expiresAt time.Time // zero = never expires
}

type node struct {
	cfg       Config
	keyProg   *expr.Program
	rt        *expr.Runtime
	db        *sql.DB
	dbOnce    sync.Once
	dbErr     error
	client    *http.Client
	mu        sync.Mutex
	cache     map[string]cacheEntry
	cacheKeys []string // FIFO insertion order, for maxEntries eviction
}

// New is the flow.Factory for the "lookup" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.KeyExpression == "" {
		return nil, fmt.Errorf("lookup: keyExpression is required")
	}
	if cfg.As == "" {
		return nil, fmt.Errorf("lookup: as is required")
	}
	switch cfg.Source {
	case "static":
		if cfg.Static == nil {
			return nil, fmt.Errorf("lookup: static is required for source \"static\"")
		}
	case "sql":
		if cfg.SQL.Query == "" {
			return nil, fmt.Errorf("lookup: sql.query is required for source \"sql\"")
		}
	case "http":
		if cfg.HTTP.URLTemplate == "" || !strings.Contains(cfg.HTTP.URLTemplate, "{key}") {
			return nil, fmt.Errorf("lookup: http.urlTemplate is required and must contain \"{key}\" for source \"http\"")
		}
	default:
		return nil, fmt.Errorf("lookup: unknown source %q", cfg.Source)
	}
	if cfg.CacheMissPolicy == "" {
		cfg.CacheMissPolicy = "fail"
	}
	if cfg.CacheMissPolicy != "fail" && cfg.CacheMissPolicy != "passthrough" && cfg.CacheMissPolicy != "default" {
		return nil, fmt.Errorf("lookup: unknown cacheMissPolicy %q", cfg.CacheMissPolicy)
	}
	if cfg.Cache.MaxEntries <= 0 {
		cfg.Cache.MaxEntries = DefaultMaxEntries
	}

	prog, err := expr.Compile(cfg.KeyExpression)
	if err != nil {
		return nil, fmt.Errorf("lookup: keyExpression: %w", err)
	}
	return &node{cfg: cfg, keyProg: prog, rt: expr.New(), client: &http.Client{Timeout: 10 * time.Second}, cache: map[string]cacheEntry{}}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	keyVal, err := n.rt.Run(ctx, n.keyProg, nodeutil.ExprData(ctx, in), 0)
	if err != nil {
		return nil, fmt.Errorf("lookup: keyExpression: %w", err)
	}
	key := fmt.Sprint(keyVal)

	value, found, err := n.lookupCached(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("lookup: %w", err)
	}

	if !found {
		switch n.cfg.CacheMissPolicy {
		case "passthrough":
			return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
		case "default":
			value = n.cfg.DefaultValue
		default: // "fail"
			return nil, fmt.Errorf("lookup: no value found for key %q", key)
		}
	}

	merged := applyPath(deepCopy(in.Payload.Value), n.cfg.As, value)
	out := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: merged})
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

func (n *node) lookupCached(ctx context.Context, key string) (any, bool, error) {
	n.mu.Lock()
	if e, ok := n.cache[key]; ok && (e.expiresAt.IsZero() || time.Now().Before(e.expiresAt)) {
		n.mu.Unlock()
		return e.value, true, nil
	}
	n.mu.Unlock()

	value, found, err := n.fetch(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if found {
		n.store(key, value)
	}
	return value, found, nil
}

func (n *node) store(key string, value any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.cache[key]; !exists {
		n.cacheKeys = append(n.cacheKeys, key)
		for len(n.cacheKeys) > n.cfg.Cache.MaxEntries {
			oldest := n.cacheKeys[0]
			n.cacheKeys = n.cacheKeys[1:]
			delete(n.cache, oldest)
		}
	}
	var expiresAt time.Time
	if n.cfg.Cache.TTLMs > 0 {
		expiresAt = time.Now().Add(time.Duration(n.cfg.Cache.TTLMs) * time.Millisecond)
	}
	n.cache[key] = cacheEntry{value: value, expiresAt: expiresAt}
}

func (n *node) fetch(ctx context.Context, key string) (any, bool, error) {
	switch n.cfg.Source {
	case "static":
		v, ok := n.cfg.Static[key]
		return v, ok, nil
	case "sql":
		return n.fetchSQL(ctx, key)
	case "http":
		return n.fetchHTTP(ctx, key)
	default:
		return nil, false, fmt.Errorf("unhandled source %q", n.cfg.Source)
	}
}

func (n *node) fetchSQL(ctx context.Context, key string) (any, bool, error) {
	n.dbOnce.Do(func() { n.db, n.dbErr = sqlshared.Connect(ctx) })
	if n.dbErr != nil {
		return nil, false, n.dbErr
	}
	rows, err := n.db.QueryContext(ctx, n.cfg.SQL.Query, key)
	if err != nil {
		return nil, false, fmt.Errorf("sql: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, false, fmt.Errorf("sql: %w", err)
	}
	if !rows.Next() {
		return nil, false, rows.Err()
	}
	vals := make([]any, len(cols))
	ptrs := make([]any, len(cols))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := rows.Scan(ptrs...); err != nil {
		return nil, false, fmt.Errorf("sql: %w", err)
	}
	row := make(map[string]any, len(cols))
	for i, col := range cols {
		row[col] = sqlshared.NormalizeValue(vals[i])
	}
	return row, true, nil
}

func (n *node) fetchHTTP(ctx context.Context, key string) (any, bool, error) {
	url := strings.ReplaceAll(n.cfg.HTTP.URLTemplate, "{key}", key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, false, fmt.Errorf("http: %w", err)
	}
	resp, err := n.client.Do(req)
	if err != nil {
		return nil, false, fmt.Errorf("http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, false, fmt.Errorf("http: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("http: status %d: %s", resp.StatusCode, string(body))
	}
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		value = string(body)
	}
	return value, true, nil
}

func applyPath(root any, path string, value any) any {
	if path == "" {
		return value
	}
	keys := strings.Split(path, ".")
	m, ok := root.(map[string]any)
	if !ok {
		m = map[string]any{}
	}
	cur := m
	for _, k := range keys[:len(keys)-1] {
		next, ok := cur[k].(map[string]any)
		if !ok {
			next = map[string]any{}
			cur[k] = next
		}
		cur = next
	}
	cur[keys[len(keys)-1]] = value
	return m
}

func deepCopy(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = deepCopy(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = deepCopy(val)
		}
		return out
	default:
		return v
	}
}
