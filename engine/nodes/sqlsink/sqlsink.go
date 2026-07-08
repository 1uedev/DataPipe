// Package sqlsink implements the "sql-sink" node (SNK-190 SQL: PostgreSQL,
// MySQL, MSSQL, SQLite): insert/update/upsert/delete with parameter mapping
// from the payload, or arbitrary parameterized statement execution for
// DDL/maintenance. A datagram whose payload is an array is written in a
// single transaction ("transaction per batch"); a single-object payload is
// a batch of one. Upsert and RETURNING are full-fidelity on
// postgres/sqlite (identical syntax) and upsert-only (no RETURNING) on
// mysql (ON DUPLICATE KEY UPDATE); mssql supports neither upsert nor
// RETURNING yet (MERGE/OUTPUT are meaningfully more SQL to hand-generate
// correctly) — both report a clear runtime error rather than silently
// producing wrong SQL. See TODO.md.
package sqlsink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["insert", "update", "upsert", "delete", "exec"] },
		"table": { "type": "string", "description": "Target table (all modes except \"exec\")." },
		"columns": { "type": "array", "items": { "type": "string" }, "description": "Payload fields to write, in column order (insert/update/upsert)." },
		"conflictColumns": { "type": "array", "items": { "type": "string" }, "description": "Conflict target columns for mode \"upsert\"." },
		"whereColumns": { "type": "array", "items": { "type": "string" }, "description": "Payload fields used to match existing rows (update/delete)." },
		"returning": { "type": "array", "items": { "type": "string" }, "description": "Columns to RETURNING and merge back into the output payload (e.g. a generated id)." },
		"statement": { "type": "string", "description": "Parameterized SQL for mode \"exec\" (DDL/maintenance); placeholder syntax depends on the connection dialect: $1,$2,... (postgres), ?,?,... (mysql/sqlite), @p1,@p2,... (mssql)." },
		"params": { "type": "array", "items": { "type": "string" }, "description": "Payload fields bound to $1, $2, ... in \"statement\"." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("sql-sink", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "SQL Sink",
		Category:     flow.CategoryProcessor,
		Description:  "Insert/update/upsert/delete or arbitrary statement execution (SNK-190, PostgreSQL); parameterized statements only.",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "sql-sink" node's "config" object.
type Config struct {
	Mode            string   `json:"mode"`
	Table           string   `json:"table,omitempty"`
	Columns         []string `json:"columns,omitempty"`
	ConflictColumns []string `json:"conflictColumns,omitempty"`
	WhereColumns    []string `json:"whereColumns,omitempty"`
	Returning       []string `json:"returning,omitempty"`
	Statement       string   `json:"statement,omitempty"`
	Params          []string `json:"params,omitempty"`
}

type node struct {
	cfg Config

	connectOnce sync.Once
	conn        sqlshared.Conn
	connectErr  error
}

// New is the flow.Factory for the "sql-sink" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "insert", "upsert":
		if cfg.Table == "" || len(cfg.Columns) == 0 {
			return nil, fmt.Errorf("sql-sink: table and columns are required for mode %q", cfg.Mode)
		}
		if cfg.Mode == "upsert" && len(cfg.ConflictColumns) == 0 {
			return nil, fmt.Errorf("sql-sink: conflictColumns is required for mode \"upsert\"")
		}
	case "update":
		if cfg.Table == "" || len(cfg.Columns) == 0 || len(cfg.WhereColumns) == 0 {
			return nil, fmt.Errorf("sql-sink: table, columns, and whereColumns are required for mode \"update\"")
		}
	case "delete":
		if cfg.Table == "" || len(cfg.WhereColumns) == 0 {
			return nil, fmt.Errorf("sql-sink: table and whereColumns are required for mode \"delete\"")
		}
	case "exec":
		if cfg.Statement == "" {
			return nil, fmt.Errorf("sql-sink: statement is required for mode \"exec\"")
		}
	default:
		return nil, fmt.Errorf("sql-sink: unknown mode %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

// connect connects at most once per node instance (a redeploy is needed to
// pick up a changed connection), the same tradeoff used by http-request and
// mqtt-out.
func (n *node) connect(ctx context.Context) (sqlshared.Conn, error) {
	n.connectOnce.Do(func() {
		n.conn, n.connectErr = sqlshared.Connect(ctx)
	})
	return n.conn, n.connectErr
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	conn, err := n.connect(ctx)
	if err != nil {
		return nil, fmt.Errorf("sql-sink: %w", err)
	}
	if n.cfg.Mode == "upsert" && conn.Dialect == sqlshared.DialectMSSQL {
		return nil, fmt.Errorf("sql-sink: mode \"upsert\" is not yet supported for mssql connections (no MERGE generation yet)")
	}
	if len(n.cfg.Returning) > 0 && (conn.Dialect == sqlshared.DialectMSSQL || conn.Dialect == sqlshared.DialectMySQL) {
		return nil, fmt.Errorf("sql-sink: \"returning\" is not supported for %s connections", conn.Dialect)
	}

	rows := batchRows(in.Payload.Value)
	if len(rows) == 0 {
		return []flow.PortDatagram{{Port: "out", Datagram: in}}, nil
	}

	tx, err := conn.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("sql-sink: beginning transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	generated := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		g, err := n.execRow(ctx, tx, conn.Dialect, row)
		if err != nil {
			return nil, fmt.Errorf("sql-sink: %w", err)
		}
		if g != nil {
			generated = append(generated, g)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("sql-sink: committing transaction: %w", err)
	}

	out := in
	if len(generated) > 0 {
		var value any = generated
		if len(generated) == 1 && !isArrayPayload(in.Payload.Value) {
			value = generated[0]
		}
		out = datagram.NewCaused(in, datagram.Source{NodeID: "sql-sink"}, datagram.Payload{Value: value})
	}
	return []flow.PortDatagram{{Port: "out", Datagram: out}}, nil
}

// execRow executes one row's statement, returning RETURNING columns merged
// as a map if configured.
func (n *node) execRow(ctx context.Context, tx *sql.Tx, dialect sqlshared.Dialect, row map[string]any) (map[string]any, error) {
	switch n.cfg.Mode {
	case "insert":
		return n.execInsertLike(ctx, tx, dialect, row, "")
	case "upsert":
		conflict := upsertClause(dialect, n.cfg.ConflictColumns, n.cfg.Columns)
		return n.execInsertLike(ctx, tx, dialect, row, conflict)
	case "update":
		return n.execUpdate(ctx, tx, dialect, row)
	case "delete":
		return nil, n.execDelete(ctx, tx, dialect, row)
	case "exec":
		return nil, n.execStatement(ctx, tx, row)
	default:
		return nil, fmt.Errorf("unknown mode %q", n.cfg.Mode)
	}
}

// upsertClause builds the dialect-specific "on conflict, update" tail of an
// INSERT: Postgres/SQLite share ON CONFLICT ... DO UPDATE SET col=EXCLUDED.col
// syntax; MySQL uses ON DUPLICATE KEY UPDATE col=VALUES(col) and ignores the
// conflict target (MySQL infers it from the table's own unique/PK index).
func upsertClause(dialect sqlshared.Dialect, conflictColumns, columns []string) string {
	if dialect == sqlshared.DialectMySQL {
		setParts := make([]string, len(columns))
		for i, col := range columns {
			setParts[i] = fmt.Sprintf("%s = VALUES(%s)", col, col)
		}
		return "ON DUPLICATE KEY UPDATE " + strings.Join(setParts, ", ")
	}
	setParts := make([]string, len(columns))
	for i, col := range columns {
		setParts[i] = fmt.Sprintf("%s = EXCLUDED.%s", col, col)
	}
	return fmt.Sprintf("ON CONFLICT (%s) DO UPDATE SET %s", strings.Join(conflictColumns, ", "), strings.Join(setParts, ", "))
}

func (n *node) execInsertLike(ctx context.Context, tx *sql.Tx, dialect sqlshared.Dialect, row map[string]any, conflictClause string) (map[string]any, error) {
	placeholders := make([]string, len(n.cfg.Columns))
	args := make([]any, len(n.cfg.Columns))
	for i, col := range n.cfg.Columns {
		placeholders[i] = dialect.Placeholder(i + 1)
		args[i] = row[col]
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", n.cfg.Table, strings.Join(n.cfg.Columns, ", "), strings.Join(placeholders, ", "))
	if conflictClause != "" {
		stmt += " " + conflictClause
	}
	return n.execWithOptionalReturning(ctx, tx, dialect, stmt, args)
}

func (n *node) execUpdate(ctx context.Context, tx *sql.Tx, dialect sqlshared.Dialect, row map[string]any) (map[string]any, error) {
	args := make([]any, 0, len(n.cfg.Columns)+len(n.cfg.WhereColumns))
	setParts := make([]string, len(n.cfg.Columns))
	for i, col := range n.cfg.Columns {
		args = append(args, row[col])
		setParts[i] = fmt.Sprintf("%s = %s", col, dialect.Placeholder(i+1))
	}
	whereParts := make([]string, len(n.cfg.WhereColumns))
	for i, col := range n.cfg.WhereColumns {
		args = append(args, row[col])
		whereParts[i] = fmt.Sprintf("%s = %s", col, dialect.Placeholder(len(n.cfg.Columns)+i+1))
	}
	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s", n.cfg.Table, strings.Join(setParts, ", "), strings.Join(whereParts, " AND "))
	return n.execWithOptionalReturning(ctx, tx, dialect, stmt, args)
}

func (n *node) execDelete(ctx context.Context, tx *sql.Tx, dialect sqlshared.Dialect, row map[string]any) error {
	args := make([]any, len(n.cfg.WhereColumns))
	whereParts := make([]string, len(n.cfg.WhereColumns))
	for i, col := range n.cfg.WhereColumns {
		args[i] = row[col]
		whereParts[i] = fmt.Sprintf("%s = %s", col, dialect.Placeholder(i+1))
	}
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s", n.cfg.Table, strings.Join(whereParts, " AND "))
	_, err := tx.ExecContext(ctx, stmt, args...)
	return err
}

func (n *node) execStatement(ctx context.Context, tx *sql.Tx, row map[string]any) error {
	args := make([]any, len(n.cfg.Params))
	for i, field := range n.cfg.Params {
		args[i] = row[field]
	}
	_, err := tx.ExecContext(ctx, n.cfg.Statement, args...)
	return err
}

func (n *node) execWithOptionalReturning(ctx context.Context, tx *sql.Tx, dialect sqlshared.Dialect, stmt string, args []any) (map[string]any, error) {
	if len(n.cfg.Returning) == 0 {
		_, err := tx.ExecContext(ctx, stmt, args...)
		return nil, err
	}
	stmt += " RETURNING " + strings.Join(n.cfg.Returning, ", ")
	vals := make([]any, len(n.cfg.Returning))
	ptrs := make([]any, len(n.cfg.Returning))
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	if err := tx.QueryRowContext(ctx, stmt, args...).Scan(ptrs...); err != nil {
		return nil, err
	}
	result := make(map[string]any, len(n.cfg.Returning))
	for i, col := range n.cfg.Returning {
		result[col] = sqlshared.NormalizeValue(vals[i])
	}
	return result, nil
}

func isArrayPayload(v any) bool {
	_, ok := v.([]any)
	return ok
}

// batchRows normalizes a datagram payload into one or more rows: an array
// payload is a batch (map or non-map elements are skipped — a sink can only
// write structured rows), anything else (map or scalar) is a batch of one.
func batchRows(value any) []map[string]any {
	switch v := value.(type) {
	case []any:
		rows := make([]map[string]any, 0, len(v))
		for _, item := range v {
			if m, ok := item.(map[string]any); ok {
				rows = append(rows, m)
			}
		}
		return rows
	case map[string]any:
		return []map[string]any{v}
	default:
		return nil
	}
}
