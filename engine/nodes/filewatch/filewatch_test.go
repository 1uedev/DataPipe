package filewatch

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/recordformat"
)

func TestCON400_NewRequiresDirectoryPatternAndFormat(t *testing.T) {
	if _, err := New(json.RawMessage(`{"pattern":"*.csv","format":"csv"}`)); err == nil {
		t.Fatal("expected an error when directory is missing")
	}
	if _, err := New(json.RawMessage(`{"directory":"/tmp","format":"csv"}`)); err == nil {
		t.Fatal("expected an error when pattern is missing")
	}
	if _, err := New(json.RawMessage(`{"directory":"/tmp","pattern":"*.csv","format":"parquet"}`)); err == nil {
		t.Fatal("expected an error for an unsupported format")
	}
}

type collector struct {
	mu   sync.Mutex
	vals []any
}

func (c *collector) emit(_ string, d datagram.Datagram) error {
	c.mu.Lock()
	c.vals = append(c.vals, d.Payload.Value)
	c.mu.Unlock()
	return nil
}

func (c *collector) snapshot() []any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]any(nil), c.vals...)
}

func waitForRecords(t *testing.T, c *collector, min int) []any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := c.snapshot(); len(got) >= min {
			return got
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for at least %d records, got %d", min, len(c.snapshot()))
	return nil
}

func TestCON400_410_WatchesDirectoryAndParsesCSVPerRecord(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Config{Directory: dir, Pattern: "*.csv", Format: "csv", CSV: recordformat.CSVConfig{HasHeader: true}, StabilityMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, c.emit)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond) // let the watcher start

	if err := os.WriteFile(filepath.Join(dir, "data.csv"), []byte("name,age\nalice,30\nbob,25\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	got := waitForRecords(t, c, 2)
	first, ok := got[0].(map[string]any)
	if !ok || first["name"] != "alice" {
		t.Errorf("first record = %+v", got[0])
	}
}

func TestCON400_IgnoresNonMatchingFiles(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Config{Directory: dir, Pattern: "*.csv", Format: "csv", StabilityMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, c.emit)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	time.Sleep(200 * time.Millisecond)
	if got := c.snapshot(); len(got) != 0 {
		t.Errorf("expected no records for a non-matching file, got %+v", got)
	}
}

func TestCON400_PostActionMarkerFile(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Config{
		Directory: dir, Pattern: "*.json", Format: "json", StabilityMs: 50,
		PostAction: PostAction{Action: "markerFile"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, c.emit)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	path := filepath.Join(dir, "event.json")
	if err := os.WriteFile(path, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	waitForRecords(t, c, 1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path + ".processed"); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected a marker file to be created after processing")
}

func TestCON400_PostActionDelete(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Config{
		Directory: dir, Pattern: "*.json", Format: "json", StabilityMs: 50,
		PostAction: PostAction{Action: "delete"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, c.emit)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	path := filepath.Join(dir, "event.json")
	if err := os.WriteFile(path, []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	waitForRecords(t, c, 1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("expected the source file to be deleted after processing")
}

func TestCON400_BatchEmitProducesOneDatagramWithAllRecords(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Config{Directory: dir, Pattern: "*.csv", Format: "csv", CSV: recordformat.CSVConfig{HasHeader: true}, Emit: "batch", StabilityMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, c.emit)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	if err := os.WriteFile(filepath.Join(dir, "data.csv"), []byte("name\nalice\nbob\n"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	got := waitForRecords(t, c, 1)
	if len(got) != 1 {
		t.Fatalf("expected exactly 1 batch datagram, got %d", len(got))
	}
	batch, ok := got[0].([]any)
	if !ok || len(batch) != 2 {
		t.Errorf("batch = %+v, want 2 records in one datagram", got[0])
	}
}

func TestCON400_RecursiveWatchesSubdirectoriesCreatedLater(t *testing.T) {
	dir := t.TempDir()
	raw, err := json.Marshal(Config{Directory: dir, Pattern: "*.json", Format: "json", Recursive: true, StabilityMs: 50})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithCancel(context.Background())
	c := &collector{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = src.Run(ctx, c.emit)
	}()
	defer func() { cancel(); <-done }()
	time.Sleep(50 * time.Millisecond)

	sub := filepath.Join(dir, "sub")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	time.Sleep(100 * time.Millisecond) // let the new-subdirectory watch register
	if err := os.WriteFile(filepath.Join(sub, "event.json"), []byte(`{"a":1}`), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	waitForRecords(t, c, 1)
}
