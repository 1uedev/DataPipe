package recordformat

import (
	"bytes"
	"testing"

	"github.com/xuri/excelize/v2"
)

func TestCON410_ParseCSVWithHeader(t *testing.T) {
	records, err := ParseRecords("csv", []byte("name,age\nalice,30\nbob,25\n"), Options{CSV: CSVConfig{HasHeader: true}})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
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
	records, err := ParseRecords("csv", []byte("a,1\nb,2\n"), Options{})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
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
	records, err := ParseRecords("tsv", []byte("a\tb\nc\td\n"), Options{})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestCON410_MalformedCSVRowFailsByDefault(t *testing.T) {
	// An unterminated quote is a genuine CSV syntax error, not just a ragged row.
	if _, err := ParseRecords("csv", []byte("a,b\n\"unterminated,c\n"), Options{}); err == nil {
		t.Fatal("expected a parse error for malformed CSV syntax with the default (fail) policy")
	}
}

func TestCON410_MalformedCSVRowSkippedWhenConfigured(t *testing.T) {
	records, err := ParseRecords("csv", []byte("a,b\n\"unterminated,c\nvalid,row\n"), Options{MalformedRowPolicy: "skip"})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("expected the valid rows to still be parsed when malformed rows are skipped")
	}
}

func TestCON410_ParseJSONWholeDocument(t *testing.T) {
	records, err := ParseRecords("json", []byte(`{"a":1}`), Options{})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
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
	records, err := ParseRecords("json", []byte(`{"data":{"items":[{"id":1},{"id":2}]}}`), Options{JSONRoot: "data.items"})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(records))
	}
}

func TestCON410_ParseJSONLines(t *testing.T) {
	records, err := ParseRecords("jsonl", []byte("{\"a\":1}\n{\"a\":2}\n\n"), Options{})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records (blank line skipped), got %d", len(records))
	}
}

func TestCON410_ParseRaw(t *testing.T) {
	records, err := ParseRecords("raw", []byte("hello world"), Options{})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 1 || records[0] != "hello world" {
		t.Errorf("records = %+v", records)
	}
}

func TestCON410_UnknownFormatErrors(t *testing.T) {
	if _, err := ParseRecords("parquet", []byte(""), Options{}); err == nil {
		t.Fatal("expected an error for an unsupported format (Parquet is deferred, see TODO.md)")
	}
}

func TestCON410_Latin1EncodingDecodesToUTF8(t *testing.T) {
	// 0xE9 in Latin-1 is 'é' (U+00E9); as raw UTF-8 bytes it would be invalid.
	raw := []byte{'n', 'a', 'm', 'e', '\n', 0xE9, '\n'}
	records, err := ParseRecords("csv", raw, Options{CSV: CSVConfig{Encoding: "latin1"}})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d: %+v", len(records), records)
	}
	row, ok := records[1].([]any)
	if !ok || row[0] != "é" {
		t.Errorf("records[1] = %+v, want the byte 0xE9 decoded as 'é'", records[1])
	}
}

func TestCON400_ParseXMLCollectsRecordElement(t *testing.T) {
	raw := []byte(`<readings><reading><id>1</id></reading><reading><id>2</id></reading></readings>`)
	records, err := ParseRecords("xml", raw, Options{XMLRecordElement: "reading"})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records, got %d: %+v", len(records), records)
	}
	first, ok := records[0].(map[string]any)
	if !ok || first["id"] != float64(1) {
		t.Errorf("first record = %+v", records[0])
	}
}

func TestCON400_ParseXMLSingleOccurrenceCollapsesToOneRecord(t *testing.T) {
	raw := []byte(`<readings><reading><id>1</id></reading></readings>`)
	records, err := ParseRecords("xml", raw, Options{XMLRecordElement: "reading"})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d: %+v", len(records), records)
	}
}

func TestCON400_ParseExcelWithHeader(t *testing.T) {
	f := excelize.NewFile()
	defer func() { _ = f.Close() }()
	sheet := f.GetSheetName(0)
	_ = f.SetCellValue(sheet, "A1", "name")
	_ = f.SetCellValue(sheet, "B1", "age")
	_ = f.SetCellValue(sheet, "A2", "alice")
	_ = f.SetCellValue(sheet, "B2", "30")

	var buf bytes.Buffer
	if err := f.Write(&buf); err != nil {
		t.Fatal(err)
	}

	records, err := ParseRecords("xlsx", buf.Bytes(), Options{Excel: ExcelConfig{HasHeader: true}})
	if err != nil {
		t.Fatalf("ParseRecords: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d: %+v", len(records), records)
	}
	rec, ok := records[0].(map[string]any)
	if !ok || rec["name"] != "alice" || rec["age"] != "30" {
		t.Errorf("record = %+v", records[0])
	}
}
