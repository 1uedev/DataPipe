-- VCS-140: environment profiles — named variable sets (dev/test/prod),
-- resolved against a flow's declared env[] (Flow-File-Format.md) at deploy
-- time. active_profile_id records which profile a flow currently deploys
-- against, so a re-push (log-level change, runtime reconnect) resolves the
-- same way without the caller having to repeat the choice every time.
CREATE TABLE environment_profiles (
  id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(id),
  name TEXT NOT NULL,
  variables TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);

ALTER TABLE flows ADD COLUMN active_profile_id TEXT REFERENCES environment_profiles(id);
