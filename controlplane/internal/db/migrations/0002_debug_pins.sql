-- DBG-130: pin captured sample data to a node's output port so downstream
-- configuration shows realistic values without live sources.
CREATE TABLE debug_pins (
  flow_id TEXT NOT NULL REFERENCES flows(id),
  node_id TEXT NOT NULL,
  port TEXT NOT NULL,
  value TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (flow_id, node_id, port)
);
