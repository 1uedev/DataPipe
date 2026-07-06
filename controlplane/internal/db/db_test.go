package db

import (
	"context"
	"os"
	"testing"
)

func TestDB_DialectDetection(t *testing.T) {
	sqliteDB, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open sqlite: %v", err)
	}
	defer func() { _ = sqliteDB.Close() }()
	if sqliteDB.Dialect() != DialectSQLite {
		t.Errorf("dialect = %v, want %v", sqliteDB.Dialect(), DialectSQLite)
	}
}

func TestDB_RebindOnlyAffectsPostgres(t *testing.T) {
	sqliteDB, _ := Open(":memory:")
	defer func() { _ = sqliteDB.Close() }()
	if got := sqliteDB.Rebind("SELECT * FROM t WHERE a = ? AND b = ?"); got != "SELECT * FROM t WHERE a = ? AND b = ?" {
		t.Errorf("sqlite Rebind changed the query: %s", got)
	}

	pgDB := &DB{dialect: DialectPostgres}
	got := pgDB.Rebind("SELECT * FROM t WHERE a = ? AND b = ?")
	want := "SELECT * FROM t WHERE a = $1 AND b = $2"
	if got != want {
		t.Errorf("postgres Rebind = %q, want %q", got, want)
	}
}

func TestDB_MigrateSQLite(t *testing.T) {
	d, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()

	ctx := context.Background()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	// Applying twice must be a no-op (idempotent).
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate (2nd time): %v", err)
	}

	for _, table := range []string{"users", "sessions", "projects", "project_members", "flows", "flow_versions", "connections", "credentials", "audit_log"} {
		var name string
		row := d.QueryRowContext(ctx, `SELECT name FROM sqlite_master WHERE type='table' AND name = ?`, table)
		if err := row.Scan(&name); err != nil {
			t.Errorf("table %q missing after migrate: %v", table, err)
		}
	}
}

func TestDB_MigratePostgres(t *testing.T) {
	dsn := os.Getenv("DATAPIPE_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("DATAPIPE_TEST_POSTGRES_DSN not set; skipping Postgres integration test")
	}
	d, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = d.Close() }()
	if d.Dialect() != DialectPostgres {
		t.Fatalf("dialect = %v, want postgres", d.Dialect())
	}

	ctx := context.Background()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate (2nd time): %v", err)
	}

	for _, table := range []string{"users", "sessions", "projects", "project_members", "flows", "flow_versions", "connections", "credentials", "audit_log"} {
		var exists bool
		row := d.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = ?)`, table)
		if err := row.Scan(&exists); err != nil {
			t.Fatalf("checking table %q: %v", table, err)
		}
		if !exists {
			t.Errorf("table %q missing after migrate", table)
		}
	}
}
