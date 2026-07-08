// Package mongoshared is the connection-establishment and BSON-normalization
// code shared by "mongo-source" and "mongo-sink" (CON-520/SNK-200).
package mongoshared

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

// Config is a "mongodb" connection's non-secret config.
type Config struct {
	Host       string `json:"host"`
	Port       int    `json:"port,omitempty"`
	Database   string `json:"database"`
	AuthSource string `json:"authSource,omitempty"`
}

// Credential is a "mongodb" connection's credential shape.
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// PingTimeout bounds a single connectivity check before it's counted as a
// failure and retried with backoff.
const PingTimeout = 5 * time.Second

// Connect resolves the calling node's connection, dials a *mongo.Client, and
// retries pinging it with exponential backoff+jitter (CON-130) until ctx is
// cancelled or the connection succeeds. The returned *mongo.Database is the
// connection's configured default database.
func Connect(ctx context.Context) (*mongo.Database, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("mongoshared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return nil, fmt.Errorf("mongoshared: parsing connection config: %w", err)
	}
	if cfg.Host == "" || cfg.Database == "" {
		return nil, fmt.Errorf("mongoshared: connection config requires host and database")
	}
	port := cfg.Port
	if port == 0 {
		port = 27017
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("mongoshared: parsing credential: %w", err)
		}
	}

	q := url.Values{}
	if cfg.AuthSource != "" {
		q.Set("authSource", cfg.AuthSource)
	}
	u := url.URL{Scheme: "mongodb", Host: fmt.Sprintf("%s:%d", cfg.Host, port), RawQuery: q.Encode()}
	if cred.Username != "" {
		u.User = url.UserPassword(cred.Username, cred.Password)
	}

	client, err := mongo.Connect(options.Client().ApplyURI(u.String()))
	if err != nil {
		return nil, fmt.Errorf("mongoshared: opening connection: %w", err)
	}

	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			_ = client.Disconnect(context.Background())
			return nil, ctx.Err()
		}
		pingCtx, cancel := context.WithTimeout(ctx, PingTimeout)
		err := client.Ping(pingCtx, nil)
		cancel()
		if err == nil {
			return client.Database(cfg.Database), nil
		}
		select {
		case <-ctx.Done():
			_ = client.Disconnect(context.Background())
			return nil, ctx.Err()
		case <-time.After(bo.Next()):
		}
	}
}

// NormalizeValue converts a decoded BSON value into a JSON-friendly shape
// (mirroring engine/nodes/sqlshared.NormalizeValue's role for SQL rows):
// ObjectIDs become their 24-char hex string, dates become RFC3339 strings,
// binary becomes base64 text, and documents/arrays are normalized
// recursively so a whole document round-trips cleanly through datagram JSON.
func NormalizeValue(v any) any {
	switch t := v.(type) {
	case bson.ObjectID:
		return t.Hex()
	case bson.DateTime:
		return t.Time().UTC().Format(time.RFC3339Nano)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case bson.Binary:
		return base64.StdEncoding.EncodeToString(t.Data)
	case bson.M:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = NormalizeValue(val)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			out[k] = NormalizeValue(val)
		}
		return out
	case bson.A:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = NormalizeValue(val)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			out[i] = NormalizeValue(val)
		}
		return out
	default:
		return v
	}
}
