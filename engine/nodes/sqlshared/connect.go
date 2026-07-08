// Package sqlshared is the connection-establishment code shared by
// "sql-source" and "sql-sink" (CON-500/SNK-190: PostgreSQL, MySQL, MSSQL,
// SQLite). Connecting retries with the shared backoff helper (CON-130);
// credentials never appear in the non-secret connection config.
package sqlshared

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	_ "github.com/go-sql-driver/mysql"  // registers the "mysql" database/sql driver
	_ "github.com/jackc/pgx/v5/stdlib"  // registers the "pgx" database/sql driver
	_ "github.com/microsoft/go-mssqldb" // registers the "sqlserver" database/sql driver
	_ "modernc.org/sqlite"              // registers the "sqlite" database/sql driver (pure Go, CGO-free)

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

// Dialect identifies which SQL variant a resolved connection speaks, so
// sql-source/sql-sink can adapt placeholder syntax and dialect-specific
// clauses (upsert, RETURNING) without hard-coding Postgres everywhere.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectMySQL    Dialect = "mysql"
	DialectMSSQL    Dialect = "mssql"
	DialectSQLite   Dialect = "sqlite"
)

// Placeholder returns the i'th (1-based) bound-parameter placeholder in this
// dialect's SQL syntax.
func (d Dialect) Placeholder(i int) string {
	switch d {
	case DialectMSSQL:
		return fmt.Sprintf("@p%d", i)
	case DialectPostgres:
		return fmt.Sprintf("$%d", i)
	default: // mysql, sqlite
		return "?"
	}
}

// Conn is a resolved SQL connection: the pool plus which dialect it speaks.
type Conn struct {
	DB      *sql.DB
	Dialect Dialect
}

// Config is a SQL connection's non-secret config. Host/Port/Database apply
// to postgres/mysql/mssql; File applies to sqlite (a local path, no network
// credentials).
type Config struct {
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Database string `json:"database,omitempty"`
	SSLMode  string `json:"sslMode,omitempty"` // postgres only; default "disable"
	File     string `json:"file,omitempty"`    // sqlite only
}

// Credential is a SQL connection's credential shape (unused for sqlite).
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// PingTimeout bounds a single connectivity check before it's counted as a
// failure and retried with backoff.
const PingTimeout = 5 * time.Second

// Connect resolves the calling node's connection, opens a *sql.DB using the
// driver matching the connection's declared type, and retries pinging it
// with exponential backoff+jitter until ctx is cancelled or it succeeds.
func Connect(ctx context.Context) (Conn, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return Conn{}, fmt.Errorf("sqlshared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return Conn{}, fmt.Errorf("sqlshared: parsing connection config: %w", err)
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return Conn{}, fmt.Errorf("sqlshared: parsing credential: %w", err)
		}
	}

	driver, dialect, dsn, err := dsnFor(Dialect(info.Type), cfg, cred)
	if err != nil {
		return Conn{}, fmt.Errorf("sqlshared: %w", err)
	}

	db, err := sql.Open(driver, dsn)
	if err != nil {
		return Conn{}, fmt.Errorf("sqlshared: opening connection: %w", err)
	}

	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			_ = db.Close()
			return Conn{}, ctx.Err()
		}
		pingCtx, cancel := context.WithTimeout(ctx, PingTimeout)
		err := db.PingContext(pingCtx)
		cancel()
		if err == nil {
			return Conn{DB: db, Dialect: dialect}, nil
		}
		select {
		case <-ctx.Done():
			_ = db.Close()
			return Conn{}, ctx.Err()
		case <-time.After(bo.Next()):
		}
	}
}

func dsnFor(connType Dialect, cfg Config, cred Credential) (driver string, dialect Dialect, dsn string, err error) {
	switch connType {
	case "", DialectPostgres:
		if cfg.Host == "" || cfg.Database == "" {
			return "", "", "", fmt.Errorf("postgres connection config requires host and database")
		}
		port := cfg.Port
		if port == 0 {
			port = 5432
		}
		sslMode := cfg.SSLMode
		if sslMode == "" {
			sslMode = "disable"
		}
		dsn = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			url.QueryEscape(cred.Username), url.QueryEscape(cred.Password), cfg.Host, port, cfg.Database, sslMode)
		return "pgx", DialectPostgres, dsn, nil

	case DialectMySQL:
		if cfg.Host == "" || cfg.Database == "" {
			return "", "", "", fmt.Errorf("mysql connection config requires host and database")
		}
		port := cfg.Port
		if port == 0 {
			port = 3306
		}
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
			url.QueryEscape(cred.Username), url.QueryEscape(cred.Password), cfg.Host, port, cfg.Database)
		return "mysql", DialectMySQL, dsn, nil

	case DialectMSSQL:
		if cfg.Host == "" || cfg.Database == "" {
			return "", "", "", fmt.Errorf("mssql connection config requires host and database")
		}
		port := cfg.Port
		if port == 0 {
			port = 1433
		}
		q := url.Values{}
		q.Set("database", cfg.Database)
		u := url.URL{
			Scheme:   "sqlserver",
			User:     url.UserPassword(cred.Username, cred.Password),
			Host:     fmt.Sprintf("%s:%d", cfg.Host, port),
			RawQuery: q.Encode(),
		}
		return "sqlserver", DialectMSSQL, u.String(), nil

	case DialectSQLite:
		if cfg.File == "" {
			return "", "", "", fmt.Errorf("sqlite connection config requires file")
		}
		return "sqlite", DialectSQLite, cfg.File, nil

	default:
		return "", "", "", fmt.Errorf("unknown SQL connection type %q (expected postgres, mysql, mssql, or sqlite)", connType)
	}
}
