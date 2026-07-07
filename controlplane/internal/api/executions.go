// Triggered-execution history (Increment 8, ENG-130/DBG-140): durable
// per-execution status and per-node input/output trace, fed by
// controlplane/internal/registry's EventChannel handler (via the
// ExecutionEventInput/DeadLetterEventInput adapters below, which keep this
// package decoupled from runtimev1 the same way Deployer/RuntimeLister do)
// and browsable/re-runnable through the REST handlers at the bottom.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

// Execution is one tracked triggered-flow run (ENG-130).
type Execution struct {
	ID            string     `json:"id"`
	FlowID        string     `json:"flowId"`
	RuntimeID     string     `json:"runtimeId"`
	Status        string     `json:"status"`
	TriggerNodeID string     `json:"triggerNodeId"`
	TriggerKind   string     `json:"triggerKind"`
	ReRunOf       *string    `json:"reRunOf"`
	StartedAt     time.Time  `json:"startedAt"`
	FinishedAt    *time.Time `json:"finishedAt"`
	DurationMs    *int64     `json:"durationMs"`
	Reason        string     `json:"reason"`
	// SeedDatagram is the trigger's own recorded emission (raw JSON), used
	// internally for "re-run from start"; never rendered in the list view.
	SeedDatagram json.RawMessage `json:"-"`
}

// ExecutionNodeIO is one node's contribution to an execution's trace
// (DBG-140 "per-node in/out data").
type ExecutionNodeIO struct {
	NodeID     string          `json:"nodeId"`
	Port       string          `json:"port"`
	Attempt    int             `json:"attempt"`
	At         time.Time       `json:"at"`
	DurationUs int64           `json:"durationUs"`
	Input      json.RawMessage `json:"input"`
	Outputs    json.RawMessage `json:"outputs"`
	Error      *NodeErrorInfo  `json:"error"`
}

// NodeErrorInfo is ERR-100's error object shape.
type NodeErrorInfo struct {
	Message string `json:"message"`
	Code    string `json:"code"`
	Stack   string `json:"stack"`
}

// ExecutionEventInput is one runtime-reported execution lifecycle event,
// decoupled from controlplane/internal/registry.ExecutionEvent (which is
// itself decoupled from the runtimev1 proto message) so this package never
// imports registry or runtimev1 — the same layering as Deployer/
// RuntimeLister/ConnectionResolver.
type ExecutionEventInput struct {
	ExecutionID, FlowID, Phase                            string
	TimeUnixMs                                            int64
	TriggerNodeID, TriggerKind, ReRunOf, SeedDatagramJSON string
	NodeID, Port                                          string
	Attempt                                               int32
	DurationUs                                            int64
	InputJSON, OutputsJSON                                string
	ErrorMessage, ErrorCode, ErrorStack                   string
	Status, Reason                                        string
}

// RecordExecutionEvent implements controlplane/internal/registry.
// ExecutionStore (via an adapter in cmd/controlplane/main.go): persists one
// phase of an execution's lifecycle.
func (s *Store) RecordExecutionEvent(ctx context.Context, runtimeID string, ev ExecutionEventInput) error {
	switch ev.Phase {
	case "waiting":
		return s.upsertExecution(ctx, runtimeID, ev, "waiting")
	case "started":
		return s.upsertExecution(ctx, runtimeID, ev, "running")
	case "node":
		return s.insertExecutionNodeIO(ctx, ev)
	case "finished":
		return s.finishExecution(ctx, ev)
	}
	return nil
}

func (s *Store) upsertExecution(ctx context.Context, runtimeID string, ev ExecutionEventInput, status string) error {
	at := time.UnixMilli(ev.TimeUnixMs).UTC().Format(time.RFC3339)
	var reRunOf any
	if ev.ReRunOf != "" {
		reRunOf = ev.ReRunOf
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE executions SET status = ?, runtime_id = ?, trigger_node_id = ?, trigger_kind = ?, re_run_of = ?, seed_datagram = ? WHERE id = ?`,
		status, runtimeID, ev.TriggerNodeID, ev.TriggerKind, reRunOf, ev.SeedDatagramJSON, ev.ExecutionID)
	if err != nil {
		return fmt.Errorf("api: updating execution: %w", err)
	}
	if n, _ := res.RowsAffected(); n > 0 {
		return nil
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO executions (id, flow_id, runtime_id, status, trigger_node_id, trigger_kind, re_run_of, seed_datagram, started_at, reason)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '')`,
		ev.ExecutionID, ev.FlowID, runtimeID, status, ev.TriggerNodeID, ev.TriggerKind, reRunOf, ev.SeedDatagramJSON, at)
	if err != nil {
		return fmt.Errorf("api: creating execution: %w", err)
	}
	return nil
}

func (s *Store) insertExecutionNodeIO(ctx context.Context, ev ExecutionEventInput) error {
	at := time.UnixMilli(ev.TimeUnixMs).UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO execution_node_io (id, execution_id, node_id, port, attempt, at, duration_us, input, outputs, error_message, error_code, error_stack)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		uuid.NewString(), ev.ExecutionID, ev.NodeID, ev.Port, ev.Attempt, at, ev.DurationUs, ev.InputJSON, ev.OutputsJSON, ev.ErrorMessage, ev.ErrorCode, ev.ErrorStack)
	if err != nil {
		return fmt.Errorf("api: recording execution node I/O: %w", err)
	}
	return nil
}

func (s *Store) finishExecution(ctx context.Context, ev ExecutionEventInput) error {
	at := time.UnixMilli(ev.TimeUnixMs).UTC().Format(time.RFC3339)
	_, err := s.db.ExecContext(ctx, `UPDATE executions SET status = ?, finished_at = ?, reason = ? WHERE id = ?`,
		ev.Status, at, ev.Reason, ev.ExecutionID)
	if err != nil {
		return fmt.Errorf("api: finishing execution: %w", err)
	}
	return nil
}

// MarkRuntimeExecutionsCrashed implements controlplane/internal/registry.
// ExecutionStore: a runtime whose EventChannel just closed can no longer be
// running whatever it last reported as running/waiting (ERR-150).
func (s *Store) MarkRuntimeExecutionsCrashed(ctx context.Context, runtimeID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE executions SET status = 'crashed', finished_at = ?, reason = 'runtime disconnected' WHERE runtime_id = ? AND status IN ('running', 'waiting')`,
		time.Now().UTC().Format(time.RFC3339), runtimeID)
	if err != nil {
		return fmt.Errorf("api: marking runtime executions crashed: %w", err)
	}
	return nil
}

// ListExecutions returns flowID's executions, newest first, optionally
// filtered by status.
func (s *Store) ListExecutions(ctx context.Context, flowID, status string, limit, offset int) ([]*Execution, error) {
	query := `SELECT id, flow_id, runtime_id, status, trigger_node_id, trigger_kind, re_run_of, started_at, finished_at, reason FROM executions WHERE flow_id = ?`
	args := []any{flowID}
	if status != "" {
		query += ` AND status = ?`
		args = append(args, status)
	}
	query += ` ORDER BY started_at DESC LIMIT ? OFFSET ?`
	args = append(args, limit, offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("api: listing executions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	execs := make([]*Execution, 0)
	for rows.Next() {
		e, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		execs = append(execs, e)
	}
	return execs, rows.Err()
}

// GetExecution returns one execution, including its seed datagram
// (unexported from JSON, used internally for re-run from start).
func (s *Store) GetExecution(ctx context.Context, id string) (*Execution, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, flow_id, runtime_id, status, trigger_node_id, trigger_kind, re_run_of, started_at, finished_at, reason, seed_datagram FROM executions WHERE id = ?`, id)
	var e Execution
	var startedAt string
	var finishedAt, reRunOf, seedDatagram sql.NullString
	if err := row.Scan(&e.ID, &e.FlowID, &e.RuntimeID, &e.Status, &e.TriggerNodeID, &e.TriggerKind, &reRunOf, &startedAt, &finishedAt, &e.Reason, &seedDatagram); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, sql.ErrNoRows
		}
		return nil, fmt.Errorf("api: scanning execution: %w", err)
	}
	if err := fillExecutionTimes(&e, startedAt, finishedAt); err != nil {
		return nil, err
	}
	if reRunOf.Valid {
		e.ReRunOf = &reRunOf.String
	}
	if seedDatagram.Valid {
		e.SeedDatagram = json.RawMessage(seedDatagram.String)
	}
	return &e, nil
}

// GetExecutionNodeIO returns an execution's full per-node trace, in
// recorded order.
func (s *Store) GetExecutionNodeIO(ctx context.Context, executionID string) ([]*ExecutionNodeIO, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT node_id, port, attempt, at, duration_us, input, outputs, error_message, error_code, error_stack
		 FROM execution_node_io WHERE execution_id = ? ORDER BY at ASC`, executionID)
	if err != nil {
		return nil, fmt.Errorf("api: listing execution node I/O: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]*ExecutionNodeIO, 0)
	for rows.Next() {
		var n ExecutionNodeIO
		var at, input, outputs, errMsg, errCode, errStack string
		if err := rows.Scan(&n.NodeID, &n.Port, &n.Attempt, &at, &n.DurationUs, &input, &outputs, &errMsg, &errCode, &errStack); err != nil {
			return nil, fmt.Errorf("api: scanning execution node I/O: %w", err)
		}
		t, err := time.Parse(time.RFC3339, at)
		if err != nil {
			return nil, fmt.Errorf("api: parsing node I/O timestamp: %w", err)
		}
		n.At = t
		n.Input = json.RawMessage(input)
		n.Outputs = json.RawMessage(outputs)
		if errMsg != "" || errCode != "" || errStack != "" {
			n.Error = &NodeErrorInfo{Message: errMsg, Code: errCode, Stack: errStack}
		}
		out = append(out, &n)
	}
	return out, rows.Err()
}

// GetExecutionNodeInput returns the recorded input datagram (raw JSON) and
// the port it arrived on for nodeID's first recorded attempt within
// executionID — used for "re-run from failed node" (every retry attempt of
// the same node shares the same input).
func (s *Store) GetExecutionNodeInput(ctx context.Context, executionID, nodeID string) (inputJSON, port string, err error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT input, port FROM execution_node_io WHERE execution_id = ? AND node_id = ? ORDER BY at ASC LIMIT 1`, executionID, nodeID)
	if err := row.Scan(&inputJSON, &port); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", "", sql.ErrNoRows
		}
		return "", "", fmt.Errorf("api: scanning execution node input: %w", err)
	}
	return inputJSON, port, nil
}

func scanExecution(row rowScanner) (*Execution, error) {
	var e Execution
	var startedAt string
	var finishedAt, reRunOf sql.NullString
	if err := row.Scan(&e.ID, &e.FlowID, &e.RuntimeID, &e.Status, &e.TriggerNodeID, &e.TriggerKind, &reRunOf, &startedAt, &finishedAt, &e.Reason); err != nil {
		return nil, fmt.Errorf("api: scanning execution: %w", err)
	}
	if err := fillExecutionTimes(&e, startedAt, finishedAt); err != nil {
		return nil, err
	}
	if reRunOf.Valid {
		e.ReRunOf = &reRunOf.String
	}
	return &e, nil
}

func fillExecutionTimes(e *Execution, startedAt string, finishedAt sql.NullString) error {
	t, err := time.Parse(time.RFC3339, startedAt)
	if err != nil {
		return fmt.Errorf("api: parsing started_at: %w", err)
	}
	e.StartedAt = t
	if finishedAt.Valid {
		ft, err := time.Parse(time.RFC3339, finishedAt.String)
		if err != nil {
			return fmt.Errorf("api: parsing finished_at: %w", err)
		}
		e.FinishedAt = &ft
		ms := ft.Sub(t).Milliseconds()
		e.DurationMs = &ms
	}
	return nil
}

// --- HTTP handlers ---

// executionAndAuthorize looks up an execution by path value and checks the
// caller's role on the owning project (resolved via the execution's flow),
// mirroring flowAndAuthorize.
func (h *Handlers) executionAndAuthorize(w http.ResponseWriter, r *http.Request, user *auth.User, min auth.ProjectRole) (*Execution, bool) {
	e, err := h.store.GetExecution(r.Context(), r.PathValue("executionId"))
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return nil, false
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	f, err := h.store.GetFlow(r.Context(), e.FlowID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return nil, false
	}
	if !requireProjectRole(w, r, h.authStore, user, f.ProjectID, min) {
		return nil, false
	}
	return e, true
}

func (h *Handlers) listExecutions(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	limit, offset := parseLimitOffset(r, 50)
	execs, err := h.store.ListExecutions(r.Context(), f.ID, r.URL.Query().Get("status"), limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, execs)
}

func (h *Handlers) getExecution(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	e, ok := h.executionAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	nodeIO, err := h.store.GetExecutionNodeIO(r.Context(), e.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, struct {
		*Execution
		NodeIO []*ExecutionNodeIO `json:"nodeIO"`
	}{e, nodeIO})
}

func (h *Handlers) rerunExecution(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	e, ok := h.executionAndAuthorize(w, r, user, auth.RoleOperator)
	if !ok {
		return
	}
	var req struct {
		From   string `json:"from"`
		NodeID string `json:"nodeId"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	var nodeID, port, datagramJSON string
	switch req.From {
	case "start":
		nodeID = e.TriggerNodeID
		port = "out" // every trigger node built so far (http-in, error-trigger) emits on "out"
		datagramJSON = string(e.SeedDatagram)
	case "node":
		if req.NodeID == "" {
			writeError(w, http.StatusBadRequest, "nodeId is required when from is \"node\"")
			return
		}
		input, p, err := h.store.GetExecutionNodeInput(r.Context(), e.ID, req.NodeID)
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusBadRequest, "unknown node id for this execution")
			return
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
		nodeID, port, datagramJSON = req.NodeID, p, input
	default:
		writeError(w, http.StatusBadRequest, "from must be \"start\" or \"node\"")
		return
	}

	if h.commander == nil {
		writeError(w, http.StatusBadRequest, "execution commands are not configured")
		return
	}
	if err := h.commander.RunExecution(r.Context(), e.FlowID, req.From, nodeID, port, datagramJSON, e.ID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	h.audit(r, user.ID, "execution.rerun", "execution", e.ID, "", nil, req)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func (h *Handlers) cancelExecution(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	e, ok := h.executionAndAuthorize(w, r, user, auth.RoleOperator)
	if !ok {
		return
	}
	if h.commander == nil {
		writeError(w, http.StatusBadRequest, "execution commands are not configured")
		return
	}
	if err := h.commander.CancelExecution(r.Context(), e.ID); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	h.audit(r, user.ID, "execution.cancel", "execution", e.ID, "", nil, nil)
	writeJSON(w, http.StatusAccepted, map[string]bool{"accepted": true})
}

func parseLimitOffset(r *http.Request, defaultLimit int) (limit, offset int) {
	limit = defaultLimit
	offset = 0
	if v := r.URL.Query().Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	return limit, offset
}
