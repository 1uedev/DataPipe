// Dead letters (Increment 8, ERR-130): durable, browsable, re-injectable
// storage for datagrams a node failed to deliver, fed by
// controlplane/internal/registry's EventChannel handler.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// DeadLetter is one undeliverable/expired datagram (ERR-130).
type DeadLetter struct {
	ID           string          `json:"id"`
	FlowID       string          `json:"flowId"`
	NodeID       string          `json:"nodeId"`
	Port         string          `json:"port"`
	Reason       string          `json:"reason"`
	Datagram     json.RawMessage `json:"datagram"`
	CreatedAt    time.Time       `json:"createdAt"`
	ReinjectedAt *time.Time      `json:"reinjectedAt"`
}

// DeadLetterEventInput is one runtime-reported dead-lettered datagram,
// decoupled from registry.DeadLetterEvent the same way ExecutionEventInput
// is decoupled from registry.ExecutionEvent.
type DeadLetterEventInput struct {
	FlowID, NodeID, Port, Reason, DatagramJSON string
	TimeUnixMs                                 int64
}

// RecordDeadLetter implements controlplane/internal/registry.ExecutionStore
// (via an adapter in cmd/controlplane/main.go).
func (s *Store) RecordDeadLetter(ctx context.Context, runtimeID string, ev DeadLetterEventInput) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dead_letters (id, flow_id, node_id, port, reason, datagram, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		uuid.NewString(), ev.FlowID, ev.NodeID, ev.Port, ev.Reason, ev.DatagramJSON, time.UnixMilli(ev.TimeUnixMs).UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("api: recording dead letter: %w", err)
	}
	return nil
}

// ListDeadLetters returns flowID's dead letters, newest first.
func (s *Store) ListDeadLetters(ctx context.Context, flowID string, limit, offset int) ([]*DeadLetter, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, flow_id, node_id, port, reason, datagram, created_at, reinjected_at FROM dead_letters WHERE flow_id = ? ORDER BY created_at DESC LIMIT ? OFFSET ?`,
		flowID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("api: listing dead letters: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]*DeadLetter, 0)
	for rows.Next() {
		d, err := scanDeadLetter(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// GetDeadLetter returns one dead letter by id.
func (s *Store) GetDeadLetter(ctx context.Context, id string) (*DeadLetter, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, flow_id, node_id, port, reason, datagram, created_at, reinjected_at FROM dead_letters WHERE id = ?`, id)
	return scanDeadLetter(row)
}

// DeleteDeadLetter discards a dead letter.
func (s *Store) DeleteDeadLetter(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dead_letters WHERE id = ?`, id)
	return err
}

// MarkDeadLetterReinjected records when a re-injection command was
// successfully queued (not necessarily processed — there is no delivery
// acknowledgement channel back from the runtime, matching the optimistic
// pattern deploy already uses).
func (s *Store) MarkDeadLetterReinjected(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dead_letters SET reinjected_at = ? WHERE id = ?`, time.Now().UTC().Format(time.RFC3339), id)
	return err
}

func scanDeadLetter(row rowScanner) (*DeadLetter, error) {
	var d DeadLetter
	var datagram, createdAt string
	var reinjectedAt sql.NullString
	if err := row.Scan(&d.ID, &d.FlowID, &d.NodeID, &d.Port, &d.Reason, &datagram, &createdAt, &reinjectedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning dead letter: %w", err)
	}
	d.Datagram = json.RawMessage(datagram)
	t, err := time.Parse(time.RFC3339, createdAt)
	if err != nil {
		return nil, fmt.Errorf("api: parsing created_at: %w", err)
	}
	d.CreatedAt = t
	if reinjectedAt.Valid {
		rt, err := time.Parse(time.RFC3339, reinjectedAt.String)
		if err != nil {
			return nil, fmt.Errorf("api: parsing reinjected_at: %w", err)
		}
		d.ReinjectedAt = &rt
	}
	return &d, nil
}

// --- HTTP handlers ---

func (h *Handlers) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	limit, offset := parseLimitOffset(r, 50)
	dls, err := h.store.ListDeadLetters(r.Context(), f.ID, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, dls)
}

// deadLetterAndAuthorize looks up a dead letter and checks the caller's
// role on the owning project (resolved via its flow), mirroring
// flowAndAuthorize/executionAndAuthorize.
func (h *Handlers) deadLetterAndAuthorize(w http.ResponseWriter, r *http.Request, user *auth.User, min auth.ProjectRole) (*DeadLetter, bool) {
	dl, err := h.store.GetDeadLetter(r.Context(), r.PathValue("deadLetterId"))
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return nil, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	f, err := h.store.GetFlow(r.Context(), dl.FlowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if !requireProjectRole(w, r, h.authStore, user, f.ProjectID, min) {
		return nil, false
	}
	return dl, true
}

func (h *Handlers) deleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	dl, ok := h.deadLetterAndAuthorize(w, r, user, auth.RoleOperator)
	if !ok {
		return
	}
	if err := h.store.DeleteDeadLetter(r.Context(), dl.ID); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "deadletter.delete", "dead_letter", dl.ID, "", dl, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) reinjectDeadLetter(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	dl, ok := h.deadLetterAndAuthorize(w, r, user, auth.RoleOperator)
	if !ok {
		return
	}
	if h.commander == nil {
		writeError(w, http.StatusBadRequest, "execution commands are not configured")
		return
	}
	if err := h.commander.ReinjectDeadLetter(r.Context(), dl.FlowID, dl.NodeID, dl.Port, string(dl.Datagram)); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := h.store.MarkDeadLetterReinjected(r.Context(), dl.ID); err != nil {
		h.logger.Error("marking dead letter reinjected failed", "error", err)
	}
	h.audit(r, user.ID, "deadletter.reinject", "dead_letter", dl.ID, "", nil, nil)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}
