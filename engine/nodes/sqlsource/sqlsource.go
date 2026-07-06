// Package sqlsource implements the "sql-source" node (CON-500 SQL,
// PostgreSQL for this increment): one-shot or periodic parameterized
// queries, with an optional incremental watermark column, one datagram per
// row. The watermark lives in node-instance memory (resets on redeploy) —
// true cross-restart durability needs ENG-120's context store wired into
// node execution, not yet connected here; see TODO.md.
package sqlsource

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["once", "periodic"], "description": "Run the query once, or repeatedly on an interval." },
		"query": { "type": "string", "description": "Parameterized SELECT statement (no string concatenation — CON-500). If incrementalColumn is set, must not have its own WHERE/ORDER BY: a \"WHERE col > $1 ORDER BY col\" clause is appended." },
		"intervalMs": { "type": "integer", "minimum": 1, "description": "Period between queries in mode \"periodic\"." },
		"incrementalColumn": { "type": "string", "description": "Column used as an incremental watermark (id or timestamp); only rows greater than the last seen value are returned on each run." }
	},
	"required": ["mode", "query"]
}`

func init() {
	flow.Register("sql-source", flow.NodeTypeInfo{
		Kind:         flow.KindSource,
		Outputs:      []string{"out"},
		DisplayName:  "SQL Source",
		Category:     flow.CategorySource,
		Description:  "One-shot or periodic parameterized SQL query, one datagram per row (CON-500, PostgreSQL).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "sql-source" node's "config" object.
type Config struct {
	Mode              string `json:"mode"`
	Query             string `json:"query"`
	IntervalMs        int    `json:"intervalMs,omitempty"`
	IncrementalColumn string `json:"incrementalColumn,omitempty"`
}

type node struct {
	cfg Config

	mu        sync.Mutex
	watermark any
}

// New is the flow.Factory for the "sql-source" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if cfg.Query == "" {
		return nil, fmt.Errorf("sql-source: query is required")
	}
	switch cfg.Mode {
	case "once":
	case "periodic":
		if cfg.IntervalMs <= 0 {
			return nil, fmt.Errorf("sql-source: intervalMs must be positive in mode \"periodic\"")
		}
	default:
		return nil, fmt.Errorf("sql-source: mode must be \"once\" or \"periodic\", got %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	db, err := sqlshared.Connect(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if err := n.runQuery(ctx, db, emit); err != nil {
		return err
	}
	if n.cfg.Mode == "once" {
		return nil
	}

	ticker := time.NewTicker(time.Duration(n.cfg.IntervalMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := n.runQuery(ctx, db, emit); err != nil {
				return err
			}
		}
	}
}

func (n *node) runQuery(ctx context.Context, db *sql.DB, emit func(string, datagram.Datagram) error) error {
	query := n.cfg.Query
	var args []any
	if n.cfg.IncrementalColumn != "" {
		n.mu.Lock()
		wm := n.watermark
		n.mu.Unlock()
		// No watermark yet: the first run is a full backfill (matching
		// common CDC practice) rather than guessing a sentinel "zero" value
		// that may not even be comparable to the column's type (e.g. a
		// timestamp column can't be compared against integer 0).
		if wm != nil {
			query += fmt.Sprintf(" WHERE %s > $1 ORDER BY %s ASC", n.cfg.IncrementalColumn, n.cfg.IncrementalColumn)
			args = append(args, wm)
		} else {
			query += fmt.Sprintf(" ORDER BY %s ASC", n.cfg.IncrementalColumn)
		}
	}

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("sql-source: query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("sql-source: reading columns: %w", err)
	}

	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return fmt.Errorf("sql-source: scanning row: %w", err)
		}

		rec := make(map[string]any, len(cols))
		for i, c := range cols {
			rec[c] = sqlshared.NormalizeValue(vals[i])
		}

		if n.cfg.IncrementalColumn != "" {
			if v, ok := rec[n.cfg.IncrementalColumn]; ok {
				n.mu.Lock()
				n.watermark = v
				n.mu.Unlock()
			}
		}

		d := datagram.New(datagram.Source{NodeID: "sql-source"}, datagram.Payload{Value: rec})
		if err := emit("out", d); err != nil {
			return err
		}
	}
	return rows.Err()
}
