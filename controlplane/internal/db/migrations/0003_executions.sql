-- Increment 8: triggered-execution history (ENG-130/DBG-140), per-node I/O
-- trace, dead letters (ERR-130), and the project-level default
-- error-handler flow (ERR-120). Same portability conventions as
-- 0001_init.sql: app-generated TEXT ids, no BLOB, RFC3339 TEXT timestamps.

ALTER TABLE projects ADD COLUMN default_error_flow TEXT;

CREATE TABLE executions (
  id TEXT PRIMARY KEY,
  flow_id TEXT NOT NULL REFERENCES flows(id),
  runtime_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  trigger_node_id TEXT NOT NULL DEFAULT '',
  trigger_kind TEXT NOT NULL DEFAULT '',
  re_run_of TEXT,
  seed_datagram TEXT NOT NULL DEFAULT '',
  started_at TEXT NOT NULL,
  finished_at TEXT,
  reason TEXT NOT NULL DEFAULT ''
);
CREATE INDEX executions_flow_started_idx ON executions (flow_id, started_at);

CREATE TABLE execution_node_io (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES executions(id),
  node_id TEXT NOT NULL,
  port TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  at TEXT NOT NULL,
  duration_us INTEGER NOT NULL,
  input TEXT NOT NULL DEFAULT '',
  outputs TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_stack TEXT NOT NULL DEFAULT ''
);
CREATE INDEX execution_node_io_execution_idx ON execution_node_io (execution_id);

CREATE TABLE dead_letters (
  id TEXT PRIMARY KEY,
  flow_id TEXT NOT NULL REFERENCES flows(id),
  node_id TEXT NOT NULL,
  port TEXT NOT NULL,
  reason TEXT NOT NULL,
  datagram TEXT NOT NULL,
  created_at TEXT NOT NULL,
  reinjected_at TEXT
);
CREATE INDEX dead_letters_flow_created_idx ON dead_letters (flow_id, created_at);
