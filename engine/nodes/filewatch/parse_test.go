package filewatch

import (
	"testing"
)

func TestCON410_ParseCSVWithHeader(t *testing.T) {
	records, err := parseRecords("csv", []byte("name,age\nalice,30\nbob,25\n"), CSVConfig{HasHeader: true}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	first, ok := records[0].(map[string]any)
	if !ok {
		t.Fatalf("record type = %T, want map[string]any", records[0])
	}
	if first["name"] != "alice" || first["age"] != "30" {
		t.Errorf("first record = %+v", first)
	}
}

func TestCON410_ParseCSVWithoutHeaderProducesArrays(t *testing.T) {
	records, err := parseRecords("csv", []byte("a,1\nb,2\n"), CSVConfig{}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
	row, ok := records[0].([]any)
	if !ok || len(row) != 2 || row[0] != "a" {
		t.Errorf("first record = %+v", records[0])
	}
}

func TestCON410_ParseTSVDefaultsToTabDelimiter(t *testing.T) {
	records, err := parseRecords("tsv", []byte("a\tb\nc\td\n"), CSVConfig{}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestCON410_MalformedCSVRowFailsByDefault(t *testing.T) {
	// An unterminated quote is a genuine CSV syntax error, not just a ragged row.
	if _, err := parseRecords("csv", []byte("a,b\n\"unterminated,c\n"), CSVConfig{}, "", ""); err == nil {
		t.Fatal("expected a parse error for malformed CSV syntax with the default (fail) policy")
	}
}

func TestCON410_MalformedCSVRowSkippedWhenConfigured(t *testing.T) {
	records, err := parseRecords("csv", []byte("a,b\n\"unterminated,c\nvalid,row\n"), CSVConfig{}, "", "skip")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected the valid rows to still be parsed when malformed rows are skipped")
	}
}

func TestCON410_ParseJSONWholeDocument(t *testing.T) {
	records, err := parseRecords("json", []byte(`{"a":1}`), CSVConfig{}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record for a whole-document JSON, got %d", len(records))
	}
	m, ok := records[0].(map[string]any)
	if !ok || m["a"] != float64(1) {
		t.Errorf("record = %+v", records[0])
	}
}

func TestCON410_ParseJSONWithRootArraySelector(t *testing.T) {
	records, err := parseRecords("json", []byte(`{"data":{"items":[{"id":1},{"id":2}]}}`), CSVConfig{}, "data.items", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestCON410_ParseJSONLines(t *testing.T) {
	records, err := parseRecords("jsonl", []byte("{\"a\":1}\n{\"a\":2}\n\n"), CSVConfig{}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records (blank line skipped), got %d", len(records))
	}
}

func TestCON410_ParseRaw(t *testing.T) {
	records, err := parseRecords("raw", []byte("hello world"), CSVConfig{}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 1 || records[0] != "hello world" {
		t.Errorf("records = %+v", records)
	}
}

func TestCON410_UnknownFormatErrors(t *testing.T) {
	if _, err := parseRecords("xml", []byte(""), CSVConfig{}, "", ""); err == nil {
		t.Fatal("expected an error for an unsupported format (XML is deferred, see TODO.md)")
	}
}

func TestCON410_Latin1EncodingDecodesToUTF8(t *testing.T) {
	// 0xE9 in Latin-1 is 'é' (U+00E9); as raw UTF-8 bytes it would be invalid.
	raw := []byte{'n', 'a', 'm', 'e', '\n', 0xE9, '\n'}
	records, err := parseRecords("csv", raw, CSVConfig{Encoding: "latin1"}, "", "")
	if err != nil {
		t.Fatalf("parseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d: %+v", len(records), records)
	}
	row, ok := records[1].([]any)
	if !ok || row[0] != "é" {
		t.Errorf("records[1] = %+v, want the byte 0xE9 decoded as 'é'", records[1])
	}
}
