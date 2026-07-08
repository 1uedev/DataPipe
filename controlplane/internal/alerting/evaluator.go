// Package alerting implements OBS-140's "threshold rules on metrics
// (connection down, edge offline) firing to ... webhooks; alert state
// visible in the UI". Rules are evaluated periodically against live
// runtime state (registry.Service already tracks Online per runtime); on a
// state transition an Alert row is created/resolved and, if the rule has a
// webhook_url, a JSON POST is fired. Error-rate/queue-depth threshold rules
// are not implemented — OBS-100's /metrics is pull-based per runtime with
// no central aggregation channel yet (edge runtimes are outbound-only,
// ARC-210, so the control plane can't just scrape them); see TODO.md.
package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// Metric is the fixed vocabulary of rule types this increment evaluates.
type Metric string

const (
	MetricConnectionDown Metric = "connectionDown" // any single targeted runtime offline
	MetricEdgeOffline    Metric = "edgeOffline"    // any runtime offline, target_runtime_id == "" (fleet-wide)
)

// Rule is one alert rule (decoupled from controlplane/internal/api's DB
// row type, mirroring the registry package's own decoupled-DTO pattern).
type Rule struct {
	ID              string
	Name            string
	Metric          Metric
	TargetRuntimeID string // "" = any/all runtimes
	WebhookURL      string // "" = no webhook, alert state still recorded
}

// RuntimeStatus is the minimal live-state slice an evaluator needs per
// runtime.
type RuntimeStatus struct {
	RuntimeID string
	Online    bool
}

// RuntimeLister answers "what's the live state of every known runtime".
// Implemented (via a small adapter) by controlplane/internal/registry.
type RuntimeLister interface {
	ListRuntimeStatuses(ctx context.Context) ([]RuntimeStatus, error)
}

// Store persists rules and alert state. Implemented (via a small adapter)
// by controlplane/internal/api.Store.
type Store interface {
	ListEnabledRules(ctx context.Context) ([]Rule, error)
	OpenAlert(ctx context.Context, ruleID string) (id string, found bool, err error)
	CreateAlert(ctx context.Context, ruleID, message string) error
	ResolveAlert(ctx context.Context, alertID string) error
}

// Evaluator periodically checks every enabled rule against live runtime
// state and fires/resolves alerts on transitions.
type Evaluator struct {
	store    Store
	runtimes RuntimeLister
	interval time.Duration
	client   *http.Client
}

// DefaultInterval is how often rules are re-evaluated.
const DefaultInterval = 15 * time.Second

func NewEvaluator(store Store, runtimes RuntimeLister) *Evaluator {
	return &Evaluator{store: store, runtimes: runtimes, interval: DefaultInterval, client: &http.Client{Timeout: 5 * time.Second}}
}

// Run evaluates every enabled rule every interval until ctx is cancelled.
func (e *Evaluator) Run(ctx context.Context) {
	ticker := time.NewTicker(e.interval)
	defer ticker.Stop()
	for {
		e.evaluateOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (e *Evaluator) evaluateOnce(ctx context.Context) {
	rules, err := e.store.ListEnabledRules(ctx)
	if err != nil {
		slog.Error("alerting: listing rules failed", "error", err)
		return
	}
	if len(rules) == 0 {
		return
	}
	statuses, err := e.runtimes.ListRuntimeStatuses(ctx)
	if err != nil {
		slog.Error("alerting: listing runtime statuses failed", "error", err)
		return
	}

	for _, rule := range rules {
		firing, message := evaluateRule(rule, statuses)
		if err := e.applyResult(ctx, rule, firing, message); err != nil {
			slog.Error("alerting: applying rule result failed", "rule", rule.ID, "error", err)
		}
	}
}

// evaluateRule reports whether rule's condition currently holds, and a
// human-readable message describing why.
func evaluateRule(rule Rule, statuses []RuntimeStatus) (firing bool, message string) {
	switch rule.Metric {
	case MetricConnectionDown:
		for _, s := range statuses {
			if s.RuntimeID == rule.TargetRuntimeID && !s.Online {
				return true, fmt.Sprintf("runtime %s is offline", s.RuntimeID)
			}
		}
		return false, ""
	case MetricEdgeOffline:
		var offline []string
		for _, s := range statuses {
			if !s.Online {
				offline = append(offline, s.RuntimeID)
			}
		}
		if len(offline) > 0 {
			return true, fmt.Sprintf("%d runtime(s) offline: %v", len(offline), offline)
		}
		return false, ""
	default:
		return false, ""
	}
}

func (e *Evaluator) applyResult(ctx context.Context, rule Rule, firing bool, message string) error {
	openID, found, err := e.store.OpenAlert(ctx, rule.ID)
	if err != nil {
		return err
	}
	switch {
	case firing && !found:
		if err := e.store.CreateAlert(ctx, rule.ID, message); err != nil {
			return err
		}
		e.fireWebhook(ctx, rule, "firing", message)
	case !firing && found:
		if err := e.store.ResolveAlert(ctx, openID); err != nil {
			return err
		}
		e.fireWebhook(ctx, rule, "resolved", "")
	}
	return nil
}

func (e *Evaluator) fireWebhook(ctx context.Context, rule Rule, state, message string) {
	if rule.WebhookURL == "" {
		return
	}
	payload, err := json.Marshal(map[string]string{
		"id": uuid.NewString(), "rule": rule.Name, "metric": string(rule.Metric), "state": state, "message": message,
	})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rule.WebhookURL, bytes.NewReader(payload))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.client.Do(req)
	if err != nil {
		slog.Warn("alerting: webhook delivery failed", "rule", rule.ID, "url", rule.WebhookURL, "error", err)
		return
	}
	_ = resp.Body.Close()
}
