package secsgemshared

import (
	"fmt"
	"sort"

	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

// AnyToItem converts a plain Go value (as decoded from JSON: string, bool,
// float64, []any, map[string]any, or nil) into a secsii.Item, so a sink
// node can build an arbitrary SECS-II message body from a datagram payload
// (CON-220's "Raw SxFy access for non-GEM messages MUST be possible").
// A map is encoded as a List of L(A(key), value) pairs sorted by key —
// SECS-II has no named-field concept, so this is a deliberate, documented
// convention rather than part of the standard.
func AnyToItem(v any) secsii.Item {
	switch val := v.(type) {
	case nil:
		return secsii.L()
	case string:
		return secsii.A(val)
	case bool:
		return secsii.Bool(val)
	case float64:
		return secsii.F8v(val)
	case int:
		return secsii.F8v(float64(val))
	case int64:
		return secsii.F8v(float64(val))
	case []any:
		items := make([]secsii.Item, len(val))
		for i, e := range val {
			items[i] = AnyToItem(e)
		}
		return secsii.L(items...)
	case map[string]any:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		items := make([]secsii.Item, len(keys))
		for i, k := range keys {
			items[i] = secsii.L(secsii.A(k), AnyToItem(val[k]))
		}
		return secsii.L(items...)
	default:
		return secsii.L()
	}
}

// StringMap converts a map[string]any (as decoded from JSON) into a
// map[string]string, stringifying non-string values with fmt's default
// formatting — used for a remote command's CPNAME/CPVAL parameters, which
// SEMI E30 defines as ASCII.
func StringMap(v any) map[string]string {
	m, ok := v.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, val := range m {
		out[k] = stringify(val)
	}
	return out
}

func stringify(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", val)
	}
}
