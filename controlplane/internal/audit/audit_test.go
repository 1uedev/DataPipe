package audit

import (
	"context"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/db"
)

func newTestLog(t *testing.T) *Log {
	t.Helper()
	d, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}
	return NewLog(d)
}

func TestSEC140_AppendAndList(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	e1, err := l.Append(ctx, "user-1", "login", "session", "sess-1", "", nil, nil)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	e2, err := l.Append(ctx, "user-1", "flow.deploy", "flow", "flow-1", "proj-1",
		map[string]any{"version": 1}, map[string]any{"version": 2})
	if err != nil {
		t.Fatalf("Append: %v", err)
	}

	if e1.Seq != 1 || e2.Seq != 2 {
		t.Errorf("seqs = %d, %d, want 1, 2", e1.Seq, e2.Seq)
	}
	if e2.PrevHash != e1.Hash {
		t.Errorf("e2.PrevHash = %q, want e1.Hash %q", e2.PrevHash, e1.Hash)
	}
	if e1.PrevHash != "" {
		t.Errorf("first entry's PrevHash = %q, want empty", e1.PrevHash)
	}

	all, err := l.List(ctx, "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("List() = %d entries, want 2", len(all))
	}
	if all[0].ID != e2.ID {
		t.Error("List should be newest-first")
	}
}

func TestSEC140_ListFiltersByProject(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()

	if _, err := l.Append(ctx, "u", "flow.deploy", "flow", "f1", "proj-1", nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Append(ctx, "u", "flow.deploy", "flow", "f2", "proj-2", nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, err := l.List(ctx, "proj-1")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 || entries[0].ObjectID != "f1" {
		t.Errorf("List(proj-1) = %+v, want just f1", entries)
	}
}

func TestSEC140_VerifyPassesOnUntamperedChain(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()
	for i := 0; i < 5; i++ {
		if _, err := l.Append(ctx, "u", "action", "obj", "id", "", nil, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := l.Verify(ctx); err != nil {
		t.Errorf("Verify on untampered chain = %v, want nil", err)
	}
}

func TestSEC140_VerifyDetectsTamperedContent(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()
	if _, err := l.Append(ctx, "u", "action", "obj", "id", "", nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if _, err := l.Append(ctx, "u", "action", "obj", "id", "", nil, nil); err != nil {
		t.Fatalf("Append: %v", err)
	}

	// Simulate an attacker editing a historical row directly in the DB.
	if _, err := l.db.ExecContext(ctx, `UPDATE audit_log SET action = 'tampered' WHERE seq = 1`); err != nil {
		t.Fatalf("tamper UPDATE: %v", err)
	}

	if err := l.Verify(ctx); err == nil {
		t.Error("Verify should detect the tampered row")
	}
}

func TestSEC140_VerifyDetectsDeletedEntry(t *testing.T) {
	l := newTestLog(t)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := l.Append(ctx, "u", "action", "obj", "id", "", nil, nil); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}

	if _, err := l.db.ExecContext(ctx, `DELETE FROM audit_log WHERE seq = 2`); err != nil {
		t.Fatalf("tamper DELETE: %v", err)
	}

	if err := l.Verify(ctx); err == nil {
		t.Error("Verify should detect the deleted row (chain gap)")
	}
}
