// Package db is the control plane's persistence layer: a thin wrapper
// around database/sql that works against either backend named in
// Architecture.md §2.4 ("PostgreSQL... also the SQLite option for
// all-in-one/edge-less small installs"). All application SQL is written
// with "?" placeholders; DB.Rebind (used automatically by the Context
// methods below) converts them to Postgres's "$N" style when needed, so
// callers never branch on dialect.
package db

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

// DB wraps *sql.DB with dialect-aware placeholder rebinding.
type DB struct {
	*sql.DB
	dialect Dialect
}

// Open picks the driver from dsn's scheme: postgres:// or postgresql://
// selects pgx; anything else (a file path, or ":memory:") selects the
// pure-Go, CGO-free modernc.org/sqlite driver (kept CGO-free to match the
// CGO_ENABLED=0 Docker builds in deploy/).
func Open(dsn string) (*DB, error) {
	dialect := DialectSQLite
	driver := "sqlite"
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		dialect = DialectPostgres
		driver = "pgx"
	}
	sqlDB, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, fmt.Errorf("db: open %s: %w", driver, err)
	}
	return &DB{DB: sqlDB, dialect: dialect}, nil
}

func (d *DB) Dialect() Dialect { return d.dialect }

// Rebind converts this package's "?" placeholders into Postgres's "$1",
// "$2", ... form; a no-op for SQLite, which already uses "?".
func (d *DB) Rebind(query string) string {
	if d.dialect != DialectPostgres {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (d *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return d.DB.ExecContext(ctx, d.Rebind(query), args...)
}

func (d *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return d.DB.QueryContext(ctx, d.Rebind(query), args...)
}

func (d *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return d.DB.QueryRowContext(ctx, d.Rebind(query), args...)
}

// Migrate applies every migrations/*.sql file not yet recorded in
// schema_migrations, in filename order, each as its own transaction.
func (d *DB) Migrate(ctx context.Context) error {
	if _, err := d.DB.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TEXT NOT NULL)`); err != nil {
		return fmt.Errorf("db: creating schema_migrations: %w", err)
	}

	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("db: reading migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)

	for _, name := range names {
		var applied int
		row := d.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, name)
		if err := row.Scan(&applied); err != nil {
			return fmt.Errorf("db: checking migration %s: %w", name, err)
		}
		if applied > 0 {
			continue
		}

		content, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("db: reading migration %s: %w", name, err)
		}

		tx, err := d.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("db: begin migration %s: %w", name, err)
		}
		for _, stmt := range splitStatements(string(content)) {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("db: applying migration %s: %w", name, err)
			}
		}
		if _, err := tx.ExecContext(ctx, d.Rebind(`INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)`), name, time.Now().UTC().Format(time.RFC3339)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("db: recording migration %s: %w", name, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("db: commit migration %s: %w", name, err)
		}
	}
	return nil
}

// splitStatements strips "--" line comments (which may themselves contain
// ";") and splits what remains into individual statements on ";" —
// sufficient for this schema's plain CREATE TABLE statements (no stored
// procedures or string literals containing ";").
func splitStatements(sqlText string) []string {
	var withoutComments strings.Builder
	for _, line := range strings.Split(sqlText, "\n") {
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		withoutComments.WriteString(line)
		withoutComments.WriteByte('\n')
	}

	parts := strings.Split(withoutComments.String(), ";")
	stmts := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			stmts = append(stmts, s)
		}
	}
	return stmts
}
