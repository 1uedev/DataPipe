// Package sqlshared is the connection-establishment code shared by
// "sql-source" and "sql-sink" (CON-500/SNK-190, PostgreSQL for this
// increment — MySQL/MSSQL/Oracle are P1-but-deferred, SQLite/generic
// JDBC-ODBC-style fallback P2, see TODO.md). Connecting retries with the
// shared backoff helper (CON-130); credentials never appear in the
// non-secret connection config.
package sqlshared

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

// Config is a "postgres" connection's non-secret config.
type Config struct {
	Host     string `json:"host"`
	Port     int    `json:"port,omitempty"`
	Database string `json:"database"`
	SSLMode  string `json:"sslMode,omitempty"` // default "disable"
}

// Credential is a "postgres" connection's credential shape.
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// PingTimeout bounds a single connectivity check before it's counted as a
// failure and retried with backoff.
const PingTimeout = 5 * time.Second

// Connect resolves the calling node's connection, opens a pgx-backed
// *sql.DB, and retries pinging it with exponential backoff+jitter until
// ctx is cancelled or the connection succeeds.
func Connect(ctx context.Context) (*sql.DB, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlshared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return nil, fmt.Errorf("sqlshared: parsing connection config: %w", err)
	}
	if cfg.Host == "" || cfg.Database == "" {
		return nil, fmt.Errorf("sqlshared: connection config requires host and database")
	}
	if cfg.Port == 0 {
		cfg.Port = 5432
	}
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("sqlshared: parsing credential: %w", err)
		}
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		url.QueryEscape(cred.Username), url.QueryEscape(cred.Password), cfg.Host, cfg.Port, cfg.Database, sslMode)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlshared: opening connection: %w", err)
	}

	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			_ = db.Close()
			return nil, ctx.Err()
		}
		pingCtx, cancel := context.WithTimeout(ctx, PingTimeout)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return db, nil
		}
		select {
		case <-ctx.Done():
			_ = db.Close()
			return nil, ctx.Err()
		case <-time.After(bo.Next()):
		}
	}
}
