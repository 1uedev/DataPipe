-- OBS-140: alerting hooks. metric is a fixed short vocabulary this
-- increment implements ("connectionDown" | "edgeOffline" — both derivable
-- from live registry runtime state); error-rate/queue-depth threshold
-- rules need a runtime->control-plane metrics push channel that doesn't
-- exist yet (OBS-100's /metrics is pull-based per runtime) — see TODO.md.
CREATE TABLE alert_rules (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  metric TEXT NOT NULL,
  target_runtime_id TEXT NOT NULL DEFAULT '',
  webhook_url TEXT NOT NULL DEFAULT '',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL
);

CREATE TABLE alerts (
  id TEXT PRIMARY KEY,
  rule_id TEXT NOT NULL REFERENCES alert_rules(id),
  state TEXT NOT NULL, -- 'firing' | 'resolved'
  message TEXT NOT NULL,
  fired_at TEXT NOT NULL,
  resolved_at TEXT
);
