package convert

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

func csvDelimiter(cfg CSVConfig) rune {
	if cfg.Delimiter == "" {
		return ','
	}
	return []rune(cfg.Delimiter)[0]
}

func csvHasHeader(cfg CSVConfig) bool {
	return cfg.HasHeader == nil || *cfg.HasHeader
}

// csvToJSON parses raw CSV text into an array of objects (hasHeader) or an
// array of string arrays (otherwise). Numeric-looking cells are converted
// to numbers, matching Convert's "explicit, no silent surprises" type
// casting stance (MAP-150) by always trying the conversion the same way
// regardless of column.
func csvToJSON(raw string, cfg CSVConfig) ([]any, error) {
	r := csv.NewReader(strings.NewReader(raw))
	r.Comma = csvDelimiter(cfg)
	r.FieldsPerRecord = -1

	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("csv2json: %w", err)
	}
	if len(rows) == 0 {
		return []any{}, nil
	}

	if !csvHasHeader(cfg) {
		out := make([]any, len(rows))
		for i, row := range rows {
			cells := make([]any, len(row))
			for j, cell := range row {
				cells[j] = csvCellValue(cell)
			}
			out[i] = cells
		}
		return out, nil
	}

	header := rows[0]
	out := make([]any, 0, len(rows)-1)
	for _, row := range rows[1:] {
		record := make(map[string]any, len(header))
		for i, col := range header {
			if i < len(row) {
				record[col] = csvCellValue(row[i])
			} else {
				record[col] = nil
			}
		}
		out = append(out, record)
	}
	return out, nil
}

func csvCellValue(s string) any {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	return s
}

// jsonToCSV renders an array of objects (columns taken from the union of
// keys, sorted for determinism) or an array of arrays as CSV text.
func jsonToCSV(value any, cfg CSVConfig) (string, error) {
	arr, ok := value.([]any)
	if !ok {
		return "", fmt.Errorf("json2csv: payload must be an array")
	}

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = csvDelimiter(cfg)

	if len(arr) == 0 {
		w.Flush()
		return buf.String(), w.Error()
	}

	if _, isMap := arr[0].(map[string]any); isMap {
		columns := csvColumnUnion(arr)
		if csvHasHeader(cfg) {
			if err := w.Write(columns); err != nil {
				return "", err
			}
		}
		for _, item := range arr {
			m, ok := item.(map[string]any)
			if !ok {
				return "", fmt.Errorf("json2csv: mixed array element types are not supported")
			}
			row := make([]string, len(columns))
			for i, col := range columns {
				row[i] = fmt.Sprint(m[col])
			}
			if err := w.Write(row); err != nil {
				return "", err
			}
		}
	} else {
		for _, item := range arr {
			cells, ok := item.([]any)
			if !ok {
				return "", fmt.Errorf("json2csv: array elements must all be objects or all be arrays")
			}
			row := make([]string, len(cells))
			for i, c := range cells {
				row[i] = fmt.Sprint(c)
			}
			if err := w.Write(row); err != nil {
				return "", err
			}
		}
	}

	w.Flush()
	return buf.String(), w.Error()
}

func csvColumnUnion(arr []any) []string {
	seen := map[string]bool{}
	var columns []string
	for _, item := range arr {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		for k := range m {
			if !seen[k] {
				seen[k] = true
				columns = append(columns, k)
			}
		}
	}
	sort.Strings(columns) // deterministic order regardless of map iteration order
	return columns
}
