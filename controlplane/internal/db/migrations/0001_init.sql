-- Portable across SQLite and Postgres (Architecture.md §2.4): no
-- autoincrement/serial columns (ids are app-generated UUIDs, the audit log's
-- monotonic sequence is app-managed too), no BLOB (ciphertext/nonce are
-- base64 TEXT), timestamps are RFC3339 TEXT.

CREATE TABLE users (
  id TEXT PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  system_role TEXT NOT NULL DEFAULT 'none',
  created_at TEXT NOT NULL
);

CREATE TABLE sessions (
  token_hash TEXT PRIMARY KEY,
  user_id TEXT NOT NULL REFERENCES users(id),
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE projects (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);

CREATE TABLE project_members (
  project_id TEXT NOT NULL REFERENCES projects(id),
  user_id TEXT NOT NULL REFERENCES users(id),
  role TEXT NOT NULL,
  PRIMARY KEY (project_id, user_id)
);

CREATE TABLE flows (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  name TEXT NOT NULL,
  draft_content TEXT NOT NULL,
  deployed_version INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE flow_versions (
  flow_id TEXT NOT NULL REFERENCES flows(id),
  version INTEGER NOT NULL,
  content TEXT NOT NULL,
  author TEXT NOT NULL,
  comment TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  deployed_at TEXT,
  PRIMARY KEY (flow_id, version)
);

CREATE TABLE connections (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  name TEXT NOT NULL,
  type TEXT NOT NULL,
  config TEXT NOT NULL,
  credential_id TEXT
);

-- Envelope encryption (SEC-120): the value is encrypted under a random,
-- per-credential data-encryption-key (DEK); the DEK itself is encrypted
-- ("wrapped") under the master key-encryption-key (KEK). key_version
-- records which KEK wrapped this DEK, so KEK rotation only needs to
-- re-wrap the (small) DEKs, never the secret values themselves.
CREATE TABLE credentials (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  name TEXT NOT NULL,
  key_version INTEGER NOT NULL,
  wrapped_dek TEXT NOT NULL,
  wrapped_dek_nonce TEXT NOT NULL,
  ciphertext TEXT NOT NULL,
  nonce TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE audit_log (
  id TEXT PRIMARY KEY,
  seq INTEGER NOT NULL,
  at TEXT NOT NULL,
  actor_user_id TEXT NOT NULL,
  action TEXT NOT NULL,
  object_type TEXT NOT NULL,
  object_id TEXT NOT NULL,
  project_id TEXT,
  before_json TEXT NOT NULL DEFAULT '',
  after_json TEXT NOT NULL DEFAULT '',
  prev_hash TEXT NOT NULL DEFAULT '',
  hash TEXT NOT NULL
);
