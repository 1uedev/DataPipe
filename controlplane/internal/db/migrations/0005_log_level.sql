-- OBS-120: per-flow log level, settable at runtime without a redeploy
-- (kept separate from flow content/versioning, same reasoning as
-- projects.default_error_flow in 0003_executions.sql).
ALTER TABLE flows ADD COLUMN log_level TEXT NOT NULL DEFAULT 'info';
