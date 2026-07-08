package mongosink

import (
	"encoding/json"
	"testing"
)

func TestSNK200_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"missing collection", Config{Mode: "insert"}, true},
		{"insert valid", Config{Mode: "insert", Collection: "readings"}, false},
		{"update missing filterFields", Config{Mode: "update", Collection: "readings"}, true},
		{"update valid", Config{Mode: "update", Collection: "readings", FilterFields: []string{"sensorId"}}, false},
		{"upsert valid", Config{Mode: "upsert", Collection: "readings", FilterFields: []string{"sensorId"}}, false},
		{"unknown mode", Config{Mode: "bogus", Collection: "readings"}, true},
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

func TestSNK200_BatchDocsHandlesArrayMapAndScalar(t *testing.T) {
	docs := batchDocs([]any{map[string]any{"a": 1}, map[string]any{"a": 2}, "skip-me"})
	if len(docs) != 2 {
		t.Fatalf("expected 2 docs, got %d", len(docs))
	}
	if got := batchDocs("not structured"); got != nil {
		t.Errorf("expected nil for scalar payload, got %+v", got)
	}
}

func TestSNK200_ContainsHelper(t *testing.T) {
	if !contains([]string{"a", "b"}, "b") {
		t.Error("expected contains to find \"b\"")
	}
	if contains([]string{"a", "b"}, "c") {
		t.Error("expected contains to not find \"c\"")
	}
}
