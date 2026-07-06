package sqlshared

import (
	"encoding/base64"
	"time"
)

// NormalizeValue converts a value scanned from a database/sql row into a
// JSON-friendly shape (CON-500: "row -> datagram mapping with type
// fidelity"): times become RFC3339 strings (preserving the zone) and byte
// slices become base64 text, matching how the rest of this codebase
// represents binary/temporal data in JSON (Flow-File-Format's own
// portability rules use the same conventions).
func NormalizeValue(v any) any {
	switch t := v.(type) {
	case time.Time:
		return t.Format(time.RFC3339Nano)
	case []byte:
		return base64.StdEncoding.EncodeToString(t)
	default:
		return v
	}
}
