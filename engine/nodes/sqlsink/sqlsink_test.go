package sqlsink

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestSNK190_NewValidatesRequiredFieldsPerMode(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"insert missing table", Config{Mode: "insert", Columns: []string{"a"}}, true},
		{"insert missing columns", Config{Mode: "insert", Table: "t"}, true},
		{"insert valid", Config{Mode: "insert", Table: "t", Columns: []string{"a"}}, false},
		{"upsert missing conflictColumns", Config{Mode: "upsert", Table: "t", Columns: []string{"a"}}, true},
		{"upsert valid", Config{Mode: "upsert", Table: "t", Columns: []string{"a"}, ConflictColumns: []string{"id"}}, false},
		{"update missing whereColumns", Config{Mode: "update", Table: "t", Columns: []string{"a"}}, true},
		{"update valid", Config{Mode: "update", Table: "t", Columns: []string{"a"}, WhereColumns: []string{"id"}}, false},
		{"delete missing whereColumns", Config{Mode: "delete", Table: "t"}, true},
		{"delete valid", Config{Mode: "delete", Table: "t", WhereColumns: []string{"id"}}, false},
		{"exec missing statement", Config{Mode: "exec"}, true},
		{"exec valid", Config{Mode: "exec", Statement: "SELECT 1"}, false},
		{"unknown mode", Config{Mode: "bogus"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := json.Marshal(c.cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(raw)
			if (err != nil) != c.wantErr {
				t.Errorf("New(%+v) error = %v, wantErr %v", c.cfg, err, c.wantErr)
			}
		})
	}
}

func TestSNK190_BatchRowsHandlesArrayMapAndScalar(t *testing.T) {
	rows := batchRows([]any{map[string]any{"a": 1}, map[string]any{"a": 2}, "skip-me"})
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (non-map array elements skipped), got %d: %+v", len(rows), rows)
	}

	single := batchRows(map[string]any{"a": 1})
	if len(single) != 1 {
		t.Fatalf("expected a map payload to be a batch of 1, got %d", len(single))
	}

	if got := batchRows("not structured"); got != nil {
		t.Errorf("expected nil for a scalar payload, got %+v", got)
	}
}

func TestSNK190_UpdateSetClauseFormatsAllColumns(t *testing.T) {
	got := updateSetClause([]string{"a", "b"})
	want := "a = EXCLUDED.a, b = EXCLUDED.b"
	if got != want {
		t.Errorf("updateSetClause = %q, want %q", got, want)
	}
}

func TestSNK190_IsArrayPayload(t *testing.T) {
	if !isArrayPayload([]any{1, 2}) {
		t.Error("expected []any to be an array payload")
	}
	if isArrayPayload(map[string]any{"a": 1}) {
		t.Error("expected a map to not be an array payload")
	}
}

func TestSNK190_ConfigRoundTripsThroughJSON(t *testing.T) {
	cfg := Config{Mode: "upsert", Table: "readings", Columns: []string{"sensor_id", "celsius"}, ConflictColumns: []string{"sensor_id"}, Returning: []string{"id"}}
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Config
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(cfg, decoded) {
		t.Errorf("round trip mismatch: got %+v, want %+v", decoded, cfg)
	}
}
