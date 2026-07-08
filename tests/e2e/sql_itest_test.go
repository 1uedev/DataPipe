//go:build itest

// CON-500/SNK-190 integration test against a real PostgreSQL (Docker
// postgres:16-alpine), per Development-Plan.md's "every connector has
// integration tests against containerized targets." Run via `make itest`.
package e2e

import (
	"context"
	"encoding/json"
	"os/exec"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
	"github.com/1uedev/DataPipe/engine/nodes/sqlsink"
	"github.com/1uedev/DataPipe/engine/nodes/sqlsource"
)

// startPostgres runs a disposable postgres:16-alpine container, waits for
// it to accept connections, and returns a resolver ready to hand out its
// connection info plus a cleanup func.
func startPostgres(t *testing.T) (resolver fakeResolver, cleanup func()) {
	t.Helper()
	const containerName = "datapipe-itest-postgres"
	_ = exec.Command("docker", "rm", "-f", containerName).Run()

	run := exec.Command("docker", "run", "-d", "--rm", "--name", containerName,
		"-e", "POSTGRES_USER=datapipe",
		"-e", "POSTGRES_PASSWORD=datapipe",
		"-e", "POSTGRES_DB=datapipe_itest",
		"-p", "15432:5432",
		"postgres:16-alpine")
	out, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("starting postgres container: %v\n%s", err, out)
	}
	cleanup = func() { _ = exec.Command("docker", "rm", "-f", containerName).Run() }

	connCfg, err := json.Marshal(sqlshared.Config{Host: "localhost", Port: 15432, Database: "datapipe_itest", SSLMode: "disable"})
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	cred, err := json.Marshal(sqlshared.Credential{Username: "datapipe", Password: "datapipe"})
	if err != nil {
		cleanup()
		t.Fatal(err)
	}
	resolver = fakeResolver{connType: "postgres", config: connCfg, credential: cred}

	// Wait for the server to actually accept connections rather than a
	// fixed sleep: sqlshared.Connect already retries with backoff, so just
	// give it a generous ceiling here.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := sqlshared.Connect(flow.WithConnection(ctx, resolver, "conn-1"))
	if err != nil {
		cleanup()
		t.Fatalf("postgres did not become ready in time: %v", err)
	}
	_ = conn.DB.Close()

	return resolver, cleanup
}

func TestCON500_SNK190_SQLSourceAndSinkAgainstRealPostgres(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	resolver, cleanup := startPostgres(t)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rctx := flow.WithConnection(ctx, resolver, "conn-1")

	conn, err := sqlshared.Connect(rctx)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.DB.Close() }()
	if _, err := conn.DB.ExecContext(ctx, `CREATE TABLE readings (id SERIAL PRIMARY KEY, sensor_id TEXT NOT NULL, celsius DOUBLE PRECISION NOT NULL)`); err != nil {
		t.Fatalf("creating table: %v", err)
	}

	// --- sql-sink: insert with RETURNING ---
	sinkRaw, err := json.Marshal(sqlsink.Config{
		Mode: "insert", Table: "readings", Columns: []string{"sensor_id", "celsius"}, Returning: []string{"id"},
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
	if len(results) != 1 {
		t.Fatalf("expected 1 output, got %d", len(results))
	}
	generated, ok := results[0].Datagram.Payload.Value.(map[string]any)
	if !ok || generated["id"] == nil {
		t.Fatalf("expected a generated id in the output payload, got %+v", results[0].Datagram.Payload.Value)
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

	// --- batch insert (transaction per batch) via an array payload ---
	batch := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: []any{
		map[string]any{"sensor_id": "room2", "celsius": 19.0},
		map[string]any{"sensor_id": "room3", "celsius": 22.0},
	}})
	if _, err := sinkNode.Process(rctx, batch); err != nil {
		t.Fatalf("sql-sink batch Process: %v", err)
	}

	var count int
	if err := conn.DB.QueryRowContext(ctx, "SELECT count(*) FROM readings").Scan(&count); err != nil {
		t.Fatalf("count query: %v", err)
	}
	if count != 3 {
		t.Fatalf("count = %d, want 3 (1 + a 2-row batch)", count)
	}
}
