// Increment 5 live debugging: WebSocket relay (DBG-100/110/120/170, protocol
// in docs/api/debug-websocket.md), design-time single-node execution
// (DBG-130), and pinned sample data (DBG-130).
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/debughub"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

// --- pins store ---

// DebugPin is a captured sample pinned to a node's output port (DBG-130).
type DebugPin struct {
	FlowID    string    `json:"flowId"`
	NodeID    string    `json:"nodeId"`
	Port      string    `json:"port"`
	Value     any       `json:"value"`
	UpdatedAt time.Time `json:"updatedAt"`
}

func (s *Store) UpsertPin(ctx context.Context, flowID, nodeID, port string, value any) (*DebugPin, error) {
	valueJSON, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("api: marshaling pin value: %w", err)
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO debug_pins (flow_id, node_id, port, value, updated_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (flow_id, node_id, port) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
	`, flowID, nodeID, port, string(valueJSON), now.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("api: upserting pin: %w", err)
	}
	return &DebugPin{FlowID: flowID, NodeID: nodeID, Port: port, Value: value, UpdatedAt: now}, nil
}

func (s *Store) DeletePin(ctx context.Context, flowID, nodeID, port string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM debug_pins WHERE flow_id = ? AND node_id = ? AND port = ?`, flowID, nodeID, port)
	return err
}

func (s *Store) ListPins(ctx context.Context, flowID string) ([]*DebugPin, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT flow_id, node_id, port, value, updated_at FROM debug_pins WHERE flow_id = ? ORDER BY node_id, port`, flowID)
	if err != nil {
		return nil, fmt.Errorf("api: listing pins: %w", err)
	}
	defer func() { _ = rows.Close() }()

	pins := make([]*DebugPin, 0)
	for rows.Next() {
		var p DebugPin
		var valueJSON, updatedAt string
		if err := rows.Scan(&p.FlowID, &p.NodeID, &p.Port, &valueJSON, &updatedAt); err != nil {
			return nil, fmt.Errorf("api: scanning pin: %w", err)
		}
		if err := json.Unmarshal([]byte(valueJSON), &p.Value); err != nil {
			return nil, fmt.Errorf("api: unmarshaling pin value: %w", err)
		}
		if p.UpdatedAt, err = time.Parse(time.RFC3339, updatedAt); err != nil {
			return nil, fmt.Errorf("api: parsing updated_at: %w", err)
		}
		pins = append(pins, &p)
	}
	return pins, rows.Err()
}

// --- HTTP handlers ---

func (h *Handlers) listPins(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleViewer)
	if !ok {
		return
	}
	pins, err := h.store.ListPins(r.Context(), f.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, pins)
}

func (h *Handlers) upsertPin(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	var req struct {
		Value any `json:"value"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	pin, err := h.store.UpsertPin(r.Context(), f.ID, r.PathValue("nodeId"), r.PathValue("port"), req.Value)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "debug.pin.set", "flow", f.ID, f.ProjectID, nil, pin)
	writeJSON(w, http.StatusOK, pin)
}

func (h *Handlers) deletePin(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}
	if err := h.store.DeletePin(r.Context(), f.ID, r.PathValue("nodeId"), r.PathValue("port")); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "debug.pin.delete", "flow", f.ID, f.ProjectID, nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

// executeNode implements DBG-130's design-time single-node execution: it
// looks the node up in the flow's current draft (so callers don't need to
// resend its config) and runs it in-process via engine/flow.ExecuteNode —
// no live deployment or runtime round-trip involved.
func (h *Handlers) executeNode(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}

	ff, err := flow.Parse(f.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid flow file: "+err.Error())
		return
	}
	nodeID := r.PathValue("nodeId")
	var node *flow.Node
	for i := range ff.Graph.Nodes {
		if ff.Graph.Nodes[i].ID == nodeID {
			node = &ff.Graph.Nodes[i]
			break
		}
	}
	if node == nil {
		writeError(w, http.StatusBadRequest, "unknown node id")
		return
	}

	var req struct {
		Payload any `json:"payload"`
	}
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	in := datagram.New(datagram.Source{FlowID: f.ID, NodeID: nodeID, Origin: "design-time-execute"}, datagram.Payload{Value: req.Payload})
	results, err := flow.ExecuteNode(r.Context(), node.Type, node.Config, in)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"outputs": []any{}, "error": err.Error()})
		return
	}
	outputs := make([]map[string]any, len(results))
	for i, res := range results {
		outputs[i] = map[string]any{"port": res.Port, "datagram": res.Datagram}
	}
	writeJSON(w, http.StatusOK, map[string]any{"outputs": outputs, "error": nil})
}

// loadFullDebugEvent implements DBG-110's "load full on demand" for a
// payload the WebSocket relay truncated.
func (h *Handlers) loadFullDebugEvent(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleOperator)
	if !ok {
		return
	}
	full, found := h.debugHub.LoadFull(f.ID, r.PathValue("eventId"))
	if !found {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"valueJson": full})
}

// --- WebSocket relay ---

type wsDebugEvent struct {
	ID            string `json:"id"`
	FlowID        string `json:"flowId"`
	NodeID        string `json:"nodeId"`
	Port          string `json:"port"`
	Direction     string `json:"direction"`
	Label         string `json:"label"`
	TimeUnixMs    int64  `json:"timeUnixMs"`
	DatagramID    string `json:"datagramId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId"`
	Quality       string `json:"quality"`
	ValueJSON     string `json:"valueJson"`
	Truncated     bool   `json:"truncated"`
	FullLength    int    `json:"fullLength"`
}

type wsWireMetrics struct {
	FlowID    string `json:"flowId"`
	FromNode  string `json:"fromNode"`
	FromPort  string `json:"fromPort"`
	ToNode    string `json:"toNode"`
	ToPort    string `json:"toPort"`
	Delivered uint64 `json:"delivered"`
	Dropped   uint64 `json:"dropped"`
}

type wsMessage struct {
	Type    string         `json:"type"`
	Event   *wsDebugEvent  `json:"event,omitempty"`
	Metrics *wsWireMetrics `json:"metrics,omitempty"`
}

func toWSMessage(item debughub.Item) wsMessage {
	if item.Event != nil {
		e := item.Event
		return wsMessage{Type: "event", Event: &wsDebugEvent{
			ID: e.ID, FlowID: e.FlowID, NodeID: e.NodeID, Port: e.Port,
			Direction: e.Direction, Label: e.Label, TimeUnixMs: e.TimeUnixMs,
			DatagramID: e.DatagramID, CorrelationID: e.CorrelationID, CausationID: e.CausationID,
			Quality: e.Quality, ValueJSON: e.ValueJSON, Truncated: e.Truncated, FullLength: e.FullLength,
		}}
	}
	m := item.Metric
	return wsMessage{Type: "wireMetrics", Metrics: &wsWireMetrics{
		FlowID: m.FlowID, FromNode: m.FromNode, FromPort: m.FromPort, ToNode: m.ToNode, ToPort: m.ToPort,
		Delivered: m.Delivered, Dropped: m.Dropped,
	}}
}

// debugWebSocket implements docs/api/debug-websocket.md: the token is a
// query param (not a header) because the browser WebSocket handshake can't
// set one, so authentication happens here rather than via auth.Middleware —
// this route is intentionally mounted outside the bearer-header-protected
// mux (see Routes in server.go).
func (h *Handlers) debugWebSocket(w http.ResponseWriter, r *http.Request) {
	token := r.URL.Query().Get("token")
	if token == "" {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	user, err := h.authStore.ValidateSession(r.Context(), token)
	if err != nil {
		writeError(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	flowID := r.URL.Query().Get("flowId")
	if flowID == "" {
		writeError(w, http.StatusBadRequest, "flowId is required")
		return
	}
	f, err := h.store.GetFlow(r.Context(), flowID)
	if errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	// DBG-170/SEC-110: "Viewer ... no payload inspection unless granted" —
	// no granular grant mechanism exists yet, so operator+ is the bar.
	if err := h.authStore.RequireProjectRole(r.Context(), user, f.ProjectID, auth.RoleOperator); err != nil {
		writeError(w, http.StatusForbidden, "forbidden")
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer func() { _ = conn.CloseNow() }()

	items, cancel := h.debugHub.Subscribe(flowID)
	defer cancel()

	// The protocol is server-push only (docs/api/debug-websocket.md); this
	// discards any client frames while still answering pings/pongs/close.
	ctx := conn.CloseRead(r.Context())
	for {
		select {
		case <-ctx.Done():
			return
		case item, ok := <-items:
			if !ok {
				return
			}
			if err := wsjson.Write(ctx, conn, toWSMessage(item)); err != nil {
				return
			}
		}
	}
}
