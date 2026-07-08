// alerting.go implements OBS-140's rule/alert storage and REST surface
// (System Admin manages rules; any authenticated user can view alert
// state). The evaluation loop itself lives in
// controlplane/internal/alerting, wired against these Store methods via
// the small adapter in cmd/controlplane/main.go — mirroring the existing
// decoupled-DTO pattern (registry, conntest) so this package never depends
// on the alerting package's own types beyond what's copied here.
package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// AlertRule is one OBS-140 threshold rule.
type AlertRule struct {
	ID              string    `json:"id"`
	Name            string    `json:"name"`
	Metric          string    `json:"metric"` // "connectionDown" | "edgeOffline"
	TargetRuntimeID string    `json:"targetRuntimeId,omitempty"`
	WebhookURL      string    `json:"webhookUrl,omitempty"`
	Enabled         bool      `json:"enabled"`
	CreatedAt       time.Time `json:"createdAt"`
}

// Alert is one fired (and possibly since-resolved) instance of a rule.
type Alert struct {
	ID         string     `json:"id"`
	RuleID     string     `json:"ruleId"`
	RuleName   string     `json:"ruleName"`
	State      string     `json:"state"` // "firing" | "resolved"
	Message    string     `json:"message"`
	FiredAt    time.Time  `json:"firedAt"`
	ResolvedAt *time.Time `json:"resolvedAt,omitempty"`
}

func (s *Store) CreateAlertRule(ctx context.Context, name, metric, targetRuntimeID, webhookURL string) (*AlertRule, error) {
	rule := &AlertRule{ID: uuid.NewString(), Name: name, Metric: metric, TargetRuntimeID: targetRuntimeID, WebhookURL: webhookURL, Enabled: true, CreatedAt: time.Now().UTC()}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO alert_rules (id, name, metric, target_runtime_id, webhook_url, enabled, created_at) VALUES (?, ?, ?, ?, ?, 1, ?)`,
		rule.ID, rule.Name, rule.Metric, rule.TargetRuntimeID, rule.WebhookURL, rule.CreatedAt.Format(time.RFC3339))
	if err != nil {
		return nil, fmt.Errorf("api: creating alert rule: %w", err)
	}
	return rule, nil
}

func (s *Store) ListAlertRules(ctx context.Context) ([]*AlertRule, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, metric, target_runtime_id, webhook_url, enabled, created_at FROM alert_rules ORDER BY created_at`)
	if err != nil {
		return nil, fmt.Errorf("api: listing alert rules: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []*AlertRule{}
	for rows.Next() {
		var r AlertRule
		var enabled int
		var createdAt string
		if err := rows.Scan(&r.ID, &r.Name, &r.Metric, &r.TargetRuntimeID, &r.WebhookURL, &enabled, &createdAt); err != nil {
			return nil, fmt.Errorf("api: scanning alert rule: %w", err)
		}
		r.Enabled = enabled != 0
		if r.CreatedAt, err = time.Parse(time.RFC3339, createdAt); err != nil {
			return nil, fmt.Errorf("api: parsing created_at: %w", err)
		}
		out = append(out, &r)
	}
	return out, rows.Err()
}

func (s *Store) DeleteAlertRule(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM alert_rules WHERE id = ?`, id)
	return err
}

// OpenAlert implements alerting.Store: the most recent still-firing alert
// for ruleID, if any.
func (s *Store) OpenAlert(ctx context.Context, ruleID string) (string, bool, error) {
	var id string
	err := s.db.QueryRowContext(ctx, `SELECT id FROM alerts WHERE rule_id = ? AND state = 'firing' ORDER BY fired_at DESC LIMIT 1`, ruleID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("api: finding open alert: %w", err)
	}
	return id, true, nil
}

// CreateAlert implements alerting.Store.
func (s *Store) CreateAlert(ctx context.Context, ruleID, message string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO alerts (id, rule_id, state, message, fired_at) VALUES (?, ?, 'firing', ?, ?)`,
		uuid.NewString(), ruleID, message, time.Now().UTC().Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("api: creating alert: %w", err)
	}
	return nil
}

// ResolveAlert implements alerting.Store.
func (s *Store) ResolveAlert(ctx context.Context, alertID string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE alerts SET state = 'resolved', resolved_at = ? WHERE id = ?`,
		time.Now().UTC().Format(time.RFC3339), alertID)
	if err != nil {
		return fmt.Errorf("api: resolving alert: %w", err)
	}
	return nil
}

// ListAlerts returns every alert (firing and resolved), newest first, for
// the UI's alert-state view.
func (s *Store) ListAlerts(ctx context.Context, limit int) ([]*Alert, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT a.id, a.rule_id, r.name, a.state, a.message, a.fired_at, a.resolved_at
		FROM alerts a JOIN alert_rules r ON r.id = a.rule_id
		ORDER BY a.fired_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("api: listing alerts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := []*Alert{}
	for rows.Next() {
		var a Alert
		var firedAt string
		var resolvedAt sql.NullString
		if err := rows.Scan(&a.ID, &a.RuleID, &a.RuleName, &a.State, &a.Message, &firedAt, &resolvedAt); err != nil {
			return nil, fmt.Errorf("api: scanning alert: %w", err)
		}
		if a.FiredAt, err = time.Parse(time.RFC3339, firedAt); err != nil {
			return nil, fmt.Errorf("api: parsing fired_at: %w", err)
		}
		if resolvedAt.Valid {
			t, err := time.Parse(time.RFC3339, resolvedAt.String)
			if err != nil {
				return nil, fmt.Errorf("api: parsing resolved_at: %w", err)
			}
			a.ResolvedAt = &t
		}
		out = append(out, &a)
	}
	return out, rows.Err()
}

// --- HTTP handlers ---

func (h *Handlers) listAlertRules(w http.ResponseWriter, r *http.Request) {
	if _, ok := currentUser(w, r); !ok {
		return
	}
	rules, err := h.store.ListAlertRules(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, rules)
}

func (h *Handlers) createAlertRule(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	var req struct {
		Name            string `json:"name"`
		Metric          string `json:"metric"`
		TargetRuntimeID string `json:"targetRuntimeId"`
		WebhookURL      string `json:"webhookUrl"`
	}
	if err := readJSON(r, &req); err != nil || req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if req.Metric != "connectionDown" && req.Metric != "edgeOffline" {
		writeError(w, http.StatusBadRequest, "metric must be \"connectionDown\" or \"edgeOffline\"")
		return
	}
	if req.Metric == "connectionDown" && req.TargetRuntimeID == "" {
		writeError(w, http.StatusBadRequest, "targetRuntimeId is required for metric \"connectionDown\"")
		return
	}
	rule, err := h.store.CreateAlertRule(r.Context(), req.Name, req.Metric, req.TargetRuntimeID, req.WebhookURL)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "alertrule.create", "alert_rule", rule.ID, "", nil, rule)
	writeJSON(w, http.StatusCreated, rule)
}

func (h *Handlers) deleteAlertRule(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	if !requireSystemAdmin(w, user) {
		return
	}
	id := r.PathValue("ruleId")
	if err := h.store.DeleteAlertRule(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	h.audit(r, user.ID, "alertrule.delete", "alert_rule", id, "", nil, nil)
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handlers) listAlerts(w http.ResponseWriter, r *http.Request) {
	if _, ok := currentUser(w, r); !ok {
		return
	}
	alerts, err := h.store.ListAlerts(r.Context(), 100)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, alerts)
}
