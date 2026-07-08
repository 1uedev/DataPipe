// Package recordformat is the file-format parsing code shared by
// "file-watch" (CON-400/410) and "s3-source" (CON-400's S3-compatible
// object storage clause / Increment 10 MVP catalog): turning raw file bytes
// into individual records for csv/tsv/json/jsonl/xml/xlsx/raw.
package recordformat

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/xuri/excelize/v2"

	"github.com/1uedev/DataPipe/engine/nodes/convert"
)

// CSVConfig configures CSV/TSV parsing (CON-410).
type CSVConfig struct {
	Delimiter string `json:"delimiter,omitempty"` // default "," (or "\t" for format "tsv")
	HasHeader bool   `json:"hasHeader,omitempty"`
	Encoding  string `json:"encoding,omitempty"` // "utf-8" (default) | "latin1"
}

// ExcelConfig configures .xlsx parsing (CON-400's "Excel" reader).
type ExcelConfig struct {
	SheetName string `json:"sheetName,omitempty"` // default: the workbook's first sheet
	HasHeader bool   `json:"hasHeader,omitempty"`
}

// Options bundles every format's config so ParseRecords has one small
// parameter list regardless of which format is active.
type Options struct {
	CSV                CSVConfig
	Excel              ExcelConfig
	JSONRoot           string // "."-path to an array, format "json"
	XMLRecordElement   string // child element name to collect as records, format "xml"
	MalformedRowPolicy string // "fail" (default) | "skip"
}

// ParseRecords turns raw file bytes into individual records; "raw"/"json"
// (non-jsonl, no jsonRoot) formats always produce exactly one record. An
// empty jsonRoot streams the whole JSON document as one record; a non-empty
// one selects a "."-path array to stream element-by-element.
func ParseRecords(format string, raw []byte, opts Options) ([]any, error) {
	switch format {
	case "csv", "tsv":
		return parseCSV(raw, format, opts.CSV, opts.MalformedRowPolicy)
	case "json":
		return parseJSON(raw, opts.JSONRoot)
	case "jsonl":
		return parseJSONLines(raw, opts.MalformedRowPolicy)
	case "xml":
		return parseXML(raw, opts.XMLRecordElement)
	case "xlsx":
		return parseExcel(raw, opts.Excel)
	case "raw":
		return []any{string(raw)}, nil
	default:
		return nil, fmt.Errorf("recordformat: unknown format %q", format)
	}
}

func parseCSV(raw []byte, format string, cfg CSVConfig, malformedRowPolicy string) ([]any, error) {
	raw = DecodeEncoding(raw, cfg.Encoding)

	delim := cfg.Delimiter
	if delim == "" {
		if format == "tsv" {
			delim = "\t"
		} else {
			delim = ","
		}
	}
	if len(delim) != 1 {
		return nil, fmt.Errorf("recordformat: csv delimiter must be exactly one character, got %q", delim)
	}

	r := csv.NewReader(bytes.NewReader(raw))
	r.Comma = rune(delim[0])
	r.FieldsPerRecord = -1 // tolerate ragged rows; validated per-row below only for malformed syntax

	var header []string
	var records []any
	rowNum := 0
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if malformedRowPolicy == "skip" {
				continue
			}
			return nil, fmt.Errorf("recordformat: csv row %d: %w", rowNum, err)
		}
		rowNum++

		if cfg.HasHeader && header == nil {
			header = row
			continue
		}
		records = append(records, rowToRecord(header, row))
	}
	return records, nil
}

func rowToRecord(header, row []string) any {
	if header != nil {
		rec := make(map[string]any, len(header))
		for i, col := range header {
			if i < len(row) {
				rec[col] = row[i]
			} else {
				rec[col] = ""
			}
		}
		return rec
	}
	cols := make([]any, len(row))
	for i, v := range row {
		cols[i] = v
	}
	return cols
}

func parseJSON(raw []byte, jsonRoot string) ([]any, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("recordformat: parsing json: %w", err)
	}
	if jsonRoot == "" {
		return []any{decoded}, nil
	}
	cur := decoded
	for _, k := range strings.Split(jsonRoot, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("recordformat: jsonRoot %q: %q is not an object", jsonRoot, k)
		}
		cur = m[k]
	}
	arr, ok := cur.([]any)
	if !ok {
		return nil, fmt.Errorf("recordformat: jsonRoot %q does not point to an array", jsonRoot)
	}
	return arr, nil
}

func parseJSONLines(raw []byte, malformedRowPolicy string) ([]any, error) {
	var records []any
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	scanner.Buffer(make([]byte, 0, 64*1024), 10*1024*1024)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var v any
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			if malformedRowPolicy == "skip" {
				continue
			}
			return nil, fmt.Errorf("recordformat: jsonl line %d: %w", lineNum, err)
		}
		records = append(records, v)
	}
	return records, scanner.Err()
}

// parseXML decodes the whole document via convert.XMLToJSON (the same
// simplified element-only convention used by the "convert" node's
// xml2json), then collects the named child element as the record array —
// e.g. recordElement "reading" for
// "<readings><reading>...</reading><reading>...</reading></readings>".
// An empty recordElement returns the whole decoded document as one record.
func parseXML(raw []byte, recordElement string) ([]any, error) {
	decoded, err := convert.XMLToJSON(string(raw))
	if err != nil {
		return nil, fmt.Errorf("recordformat: %w", err)
	}
	if recordElement == "" {
		return []any{decoded}, nil
	}
	m, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("recordformat: xmlRecordElement %q: root is not an object", recordElement)
	}
	v, ok := m[recordElement]
	if !ok {
		return nil, fmt.Errorf("recordformat: xmlRecordElement %q not found at the document root", recordElement)
	}
	if arr, ok := v.([]any); ok {
		return arr, nil
	}
	return []any{v}, nil // a single occurrence collapses to a scalar/object, not an array
}

func parseExcel(raw []byte, cfg ExcelConfig) ([]any, error) {
	f, err := excelize.OpenReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("recordformat: opening xlsx: %w", err)
	}
	defer func() { _ = f.Close() }()

	sheet := cfg.SheetName
	if sheet == "" {
		sheet = f.GetSheetName(0)
	}
	rows, err := f.GetRows(sheet)
	if err != nil {
		return nil, fmt.Errorf("recordformat: reading sheet %q: %w", sheet, err)
	}

	var header []string
	var records []any
	for i, row := range rows {
		if cfg.HasHeader && i == 0 {
			header = row
			continue
		}
		records = append(records, rowToRecord(header, row))
	}
	return records, nil
}

// DecodeEncoding converts Latin-1 (ISO-8859-1) input to UTF-8; anything else
// (including the "utf-8"/"" default) is passed through unchanged. Latin-1's
// byte values map 1:1 onto the first 256 Unicode code points, so this is
// just a byte-to-rune widening.
func DecodeEncoding(raw []byte, encoding string) []byte {
	if encoding != "latin1" {
		return raw
	}
	runes := make([]rune, len(raw))
	for i, b := range raw {
		runes[i] = rune(b)
	}
	return []byte(string(runes))
}
