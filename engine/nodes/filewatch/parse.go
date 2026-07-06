package filewatch

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// CSVConfig configures CSV/TSV parsing (CON-410).
type CSVConfig struct {
	Delimiter string `json:"delimiter,omitempty"` // default "," (or "\t" for format "tsv")
	HasHeader bool   `json:"hasHeader,omitempty"`
	Encoding  string `json:"encoding,omitempty"` // "utf-8" (default) | "latin1"
}

// parseRecords turns raw file bytes into individual records per CON-410;
// "raw"/"json" (non-jsonl) formats always produce exactly one record. An
// empty jsonRoot streams the whole JSON document as one record; a non-empty
// one selects a "."-path array to stream element-by-element.
func parseRecords(format string, raw []byte, csvCfg CSVConfig, jsonRoot string, malformedRowPolicy string) ([]any, error) {
	switch format {
	case "csv", "tsv":
		return parseCSV(raw, format, csvCfg, malformedRowPolicy)
	case "json":
		return parseJSON(raw, jsonRoot)
	case "jsonl":
		return parseJSONLines(raw, malformedRowPolicy)
	case "raw":
		return []any{string(raw)}, nil
	default:
		return nil, fmt.Errorf("filewatch: unknown format %q", format)
	}
}

func parseCSV(raw []byte, format string, cfg CSVConfig, malformedRowPolicy string) ([]any, error) {
	raw = decodeEncoding(raw, cfg.Encoding)

	delim := cfg.Delimiter
	if delim == "" {
		if format == "tsv" {
			delim = "\t"
		} else {
			delim = ","
		}
	}
	if len(delim) != 1 {
		return nil, fmt.Errorf("filewatch: csv delimiter must be exactly one character, got %q", delim)
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
			return nil, fmt.Errorf("filewatch: csv row %d: %w", rowNum, err)
		}
		rowNum++

		if cfg.HasHeader && header == nil {
			header = row
			continue
		}
		if header != nil {
			rec := make(map[string]any, len(header))
			for i, col := range header {
				if i < len(row) {
					rec[col] = row[i]
				} else {
					rec[col] = ""
				}
			}
			records = append(records, rec)
		} else {
			cols := make([]any, len(row))
			for i, v := range row {
				cols[i] = v
			}
			records = append(records, cols)
		}
	}
	return records, nil
}

func parseJSON(raw []byte, jsonRoot string) ([]any, error) {
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil, fmt.Errorf("filewatch: parsing json: %w", err)
	}
	if jsonRoot == "" {
		return []any{decoded}, nil
	}
	cur := decoded
	for _, k := range strings.Split(jsonRoot, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("filewatch: jsonRoot %q: %q is not an object", jsonRoot, k)
		}
		cur = m[k]
	}
	arr, ok := cur.([]any)
	if !ok {
		return nil, fmt.Errorf("filewatch: jsonRoot %q does not point to an array", jsonRoot)
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
			return nil, fmt.Errorf("filewatch: jsonl line %d: %w", lineNum, err)
		}
		records = append(records, v)
	}
	return records, scanner.Err()
}

// decodeEncoding converts Latin-1 (ISO-8859-1) input to UTF-8; anything else
// (including the "utf-8"/"" default) is passed through unchanged. Latin-1's
// byte values map 1:1 onto the first 256 Unicode code points, so this is
// just a byte-to-rune widening.
func decodeEncoding(raw []byte, encoding string) []byte {
	if encoding != "latin1" {
		return raw
	}
	runes := make([]rune, len(raw))
	for i, b := range raw {
		runes[i] = rune(b)
	}
	return []byte(string(runes))
}
