// Package audit implements SEC-140's tamper-evident, append-only audit
// log: every entry hashes its own content plus the previous entry's hash,
// so altering or deleting a historical row breaks the chain for every
// entry after it — detectable by Verify without needing an external system.
package audit

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/db"
)

// Entry is one immutable audit record (SEC-140: "actor, time, object,
// before/after where feasible").
type Entry struct {
	ID          string
	Seq         int64
	At          time.Time
	ActorUserID string
	Action      string
	ObjectType  string
	ObjectID    string
	ProjectID   string // empty if the action isn't project-scoped
	Before      any
	After       any
	PrevHash    string
	Hash        string
}

// Log appends to and reads the audit_log table.
type Log struct {
	db *db.DB
	mu sync.Mutex // serializes appends so the seq/prevHash chain never races
}

func NewLog(d *db.DB) *Log {
	return &Log{db: d}
}

// Append records one audit entry. before/after are marshaled to JSON as-is
// (nil is recorded as an empty object); pass nil for actions with no
// natural before/after (e.g. login).
func (l *Log) Append(ctx context.Context, actorUserID, action, objectType, objectID, projectID string, before, after any) (*Entry, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	lastSeq, prevHash, err := l.tail(ctx)
	if err != nil {
		return nil, err
	}

	beforeJSON, err := marshalOrEmpty(before)
	if err != nil {
		return nil, err
	}
	afterJSON, err := marshalOrEmpty(after)
	if err != nil {
		return nil, err
	}

	e := &Entry{
		ID:          uuid.NewString(),
		Seq:         lastSeq + 1,
		At:          time.Now().UTC(),
		ActorUserID: actorUserID,
		Action:      action,
		ObjectType:  objectType,
		ObjectID:    objectID,
		ProjectID:   projectID,
		Before:      before,
		After:       after,
		PrevHash:    prevHash,
	}
	e.Hash = computeHash(e.PrevHash, e.Seq, e.At, e.ActorUserID, e.Action, e.ObjectType, e.ObjectID, beforeJSON, afterJSON)

	_, err = l.db.ExecContext(ctx,
		`INSERT INTO audit_log (id, seq, at, actor_user_id, action, object_type, object_id, project_id, before_json, after_json, prev_hash, hash)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.ID, e.Seq, e.At.Format(time.RFC3339Nano), e.ActorUserID, e.Action, e.ObjectType, e.ObjectID, nullableString(e.ProjectID), beforeJSON, afterJSON, e.PrevHash, e.Hash)
	if err != nil {
		return nil, fmt.Errorf("audit: appending entry: %w", err)
	}
	return e, nil
}

func (l *Log) tail(ctx context.Context) (seq int64, hash string, err error) {
	row := l.db.QueryRowContext(ctx, `SELECT seq, hash FROM audit_log ORDER BY seq DESC LIMIT 1`)
	if err := row.Scan(&seq, &hash); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, "", nil
		}
		return 0, "", fmt.Errorf("audit: reading chain tail: %w", err)
	}
	return seq, hash, nil
}

// List returns entries newest-first, optionally filtered to one project.
func (l *Log) List(ctx context.Context, projectID string) ([]*Entry, error) {
	var rows *sql.Rows
	var err error
	if projectID == "" {
		rows, err = l.db.QueryContext(ctx, `SELECT id, seq, at, actor_user_id, action, object_type, object_id, project_id, before_json, after_json, prev_hash, hash FROM audit_log ORDER BY seq DESC`)
	} else {
		rows, err = l.db.QueryContext(ctx, `SELECT id, seq, at, actor_user_id, action, object_type, object_id, project_id, before_json, after_json, prev_hash, hash FROM audit_log WHERE project_id = ? ORDER BY seq DESC`, projectID)
	}
	if err != nil {
		return nil, fmt.Errorf("audit: listing: %w", err)
	}
	defer func() { _ = rows.Close() }()

	entries := make([]*Entry, 0)
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// Verify walks the whole chain in sequence order and confirms every entry's
// hash matches its recorded content and its predecessor's hash — proving (or
// disproving) that no entry has been altered or removed since it was
// written (SEC-140: "tamper-evident").
func (l *Log) Verify(ctx context.Context) error {
	rows, err := l.db.QueryContext(ctx, `SELECT id, seq, at, actor_user_id, action, object_type, object_id, project_id, before_json, after_json, prev_hash, hash FROM audit_log ORDER BY seq ASC`)
	if err != nil {
		return fmt.Errorf("audit: verifying: %w", err)
	}
	defer func() { _ = rows.Close() }()

	prevHash := ""
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			return err
		}
		if e.PrevHash != prevHash {
			return fmt.Errorf("audit: chain broken at seq %d: prev_hash mismatch", e.Seq)
		}
		beforeJSON, _ := marshalOrEmpty(e.Before)
		afterJSON, _ := marshalOrEmpty(e.After)
		want := computeHash(e.PrevHash, e.Seq, e.At, e.ActorUserID, e.Action, e.ObjectType, e.ObjectID, beforeJSON, afterJSON)
		if want != e.Hash {
			return fmt.Errorf("audit: entry %s (seq %d) hash mismatch — tampered", e.ID, e.Seq)
		}
		prevHash = e.Hash
	}
	return rows.Err()
}

func scanEntry(row rowScanner) (*Entry, error) {
	var e Entry
	var at string
	var projectID sql.NullString
	var beforeJSON, afterJSON string
	if err := row.Scan(&e.ID, &e.Seq, &at, &e.ActorUserID, &e.Action, &e.ObjectType, &e.ObjectID, &projectID, &beforeJSON, &afterJSON, &e.PrevHash, &e.Hash); err != nil {
		return nil, fmt.Errorf("audit: scanning entry: %w", err)
	}
	t, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, fmt.Errorf("audit: parsing at: %w", err)
	}
	e.At = t
	e.ProjectID = projectID.String
	if err := json.Unmarshal([]byte(beforeJSON), &e.Before); err != nil {
		return nil, fmt.Errorf("audit: unmarshaling before: %w", err)
	}
	if err := json.Unmarshal([]byte(afterJSON), &e.After); err != nil {
		return nil, fmt.Errorf("audit: unmarshaling after: %w", err)
	}
	return &e, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func computeHash(prevHash string, seq int64, at time.Time, actorUserID, action, objectType, objectID string, beforeJSON, afterJSON []byte) string {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%d|%s|%s|%s|%s|%s|%s|%s",
		prevHash, seq, at.Format(time.RFC3339Nano), actorUserID, action, objectType, objectID, beforeJSON, afterJSON)
	return hex.EncodeToString(h.Sum(nil))
}

func marshalOrEmpty(v any) ([]byte, error) {
	if v == nil {
		return []byte("{}"), nil
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("audit: marshaling: %w", err)
	}
	return b, nil
}

func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
