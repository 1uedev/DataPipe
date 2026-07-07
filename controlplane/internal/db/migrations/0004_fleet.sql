-- Increment 9: fleet management (EDGE-120) — runtime groups, enrollment
-- tokens (per-device credentials, ARC-210), and enrolled device records.
-- Live health (online/cpu/memory/flow status) stays in the control plane's
-- in-memory registry (registry.Service), refreshed on every Heartbeat —
-- only admin-configured, durable fleet state lives here. Same portability
-- conventions as 0001_init.sql: app-generated TEXT ids, no BLOB, RFC3339
-- TEXT timestamps.

CREATE TABLE runtime_groups (
  name TEXT PRIMARY KEY,
  description TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE TABLE runtime_enroll_tokens (
  id TEXT PRIMARY KEY,
  token_hash TEXT NOT NULL UNIQUE,
  display_name TEXT NOT NULL DEFAULT '',
  group_name TEXT REFERENCES runtime_groups(name),
  created_by TEXT NOT NULL,
  created_at TEXT NOT NULL,
  used_by_runtime_id TEXT,
  revoked_at TEXT
);

CREATE TABLE devices (
  runtime_id TEXT PRIMARY KEY,
  kind TEXT NOT NULL DEFAULT 'server',
  display_name TEXT NOT NULL DEFAULT '',
  group_name TEXT REFERENCES runtime_groups(name),
  enroll_token_id TEXT REFERENCES runtime_enroll_tokens(id),
  enrolled_at TEXT NOT NULL
);
