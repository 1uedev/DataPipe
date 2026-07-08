// CON-500/SNK-190 SQLite dialect coverage. Unlike the postgres/mysql/mssql
// itests, this needs no Docker container (modernc.org/sqlite is a pure-Go,
// embedded, file-backed driver), so it runs as a normal test — proof that
// sqlshared.Connect's dialect dispatch (Increment 10) actually works
// end-to-end against a real database, not just in unit tests of the SQL
// string builders.
package e2e

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
	"github.com/1uedev/DataPipe/engine/nodes/sqlsink"
	"github.com/1uedev/DataPipe/engine/nodes/sqlsource"
)

type sqliteResolver struct {
	config json.RawMessage
}

func (r sqliteResolver) ResolveConnection(context.Context, string) (flow.ConnectionInfo, error) {
	return flow.ConnectionInfo{Type: "sqlite", Config: r.config}, nil
}

func TestCON500_SNK190_SQLSourceAndSinkAgainstRealSQLite(t *testing.T) {
	dbFile := filepath.Join(t.TempDir(), "itest.db")
	connCfg, err := json.Marshal(sqlshared.Config{File: dbFile})
	if err != nil {
		t.Fatal(err)
	}
	resolver := sqliteResolver{config: connCfg}
	ctx := context.Background()
	rctx := flow.WithConnection(ctx, resolver, "conn-1")

	conn, err := sqlshared.Connect(rctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.DB.Close() }()
	if conn.Dialect != sqlshared.DialectSQLite {
		t.Fatalf("dialect = %q, want sqlite", conn.Dialect)
	}
	if _, err := conn.DB.ExecContext(ctx, `CREATE TABLE readings (id INTEGER PRIMARY KEY AUTOINCREMENT, sensor_id TEXT NOT NULL UNIQUE, celsius REAL NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	// --- sql-sink: upsert with RETURNING (SQLite supports Postgres-style
	// ON CONFLICT ... DO UPDATE ... RETURNING) ---
	sinkRaw, err := json.Marshal(sqlsink.Config{
		Mode: "upsert", Table: "readings", Columns: []string{"sensor_id", "celsius"},
		ConflictColumns: []string{"sensor_id"}, Returning: []string{"id"},
	})
	if err != nil {
		t.Fatal(err)
	}
	sinkNodeAny, err := sqlsink.New(sinkRaw)
	if err != nil {
		t.Fatalf("sqlsink.New: %v", err)
	}
	sinkNode := sinkNodeAny.(flow.Processor)

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"sensor_id": "room1", "celsius": 21.5}})
	results, err := sinkNode.Process(rctx, in)
	if err != nil {
		t.Fatalf("sql-sink Process: %v", err)
	}
	generated, ok := results[0].Datagram.Payload.Value.(map[string]any)
	if !ok || generated["id"] == nil {
		t.Fatalf("expected a generated id, got %+v", results[0].Datagram.Payload.Value)
	}

	// --- sql-source: read it back ---
	srcRaw, err := json.Marshal(sqlsource.Config{Mode: "once", Query: "SELECT id, sensor_id, celsius FROM readings"})
	if err != nil {
		t.Fatal(err)
	}
	srcNodeAny, err := sqlsource.New(srcRaw)
	if err != nil {
		t.Fatalf("sqlsource.New: %v", err)
	}
	srcNode := srcNodeAny.(flow.Source)

	var rows []map[string]any
	if err := srcNode.Run(rctx, func(_ string, d datagram.Datagram) error {
		m, _ := d.Payload.Value.(map[string]any)
		rows = append(rows, m)
		return nil
	}); err != nil {
		t.Fatalf("sql-source Run: %v", err)
	}
	if len(rows) != 1 || rows[0]["sensor_id"] != "room1" {
		t.Fatalf("rows = %+v", rows)
	}

	// --- upsert on the same conflict key updates rather than duplicating ---
	in2 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"sensor_id": "room1", "celsius": 23.0}})
	if _, err := sinkNode.Process(rctx, in2); err != nil {
		t.Fatalf("sql-sink upsert Process: %v", err)
	}
	var count int
	if err := conn.DB.QueryRowContext(ctx, "SELECT count(*) FROM readings").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 1 {
		t.Fatalf("count = %d, want 1 (upsert should update, not insert a duplicate)", count)
	}
}
