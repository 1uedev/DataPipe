package expr

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dop251/goja"
)

// bind sets every global an expression may reference (MAP-130): payload,
// header, tags, env, flow, global, plus the standard function library (dt,
// stats, conv, hash). Never binds anything capable of filesystem or network
// I/O (SEC-150).
func bind(vm *goja.Runtime, data Data) error {
	if err := vm.Set("payload", data.Payload); err != nil {
		return fmt.Errorf("expr: binding payload: %w", err)
	}

	headerMap, err := headerToMap(data.Header)
	if err != nil {
		return fmt.Errorf("expr: binding header: %w", err)
	}
	if err := vm.Set("header", headerMap); err != nil {
		return fmt.Errorf("expr: binding header: %w", err)
	}

	tags := data.Header.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	if err := vm.Set("tags", tags); err != nil {
		return fmt.Errorf("expr: binding tags: %w", err)
	}

	env := data.Env
	if env == nil {
		env = map[string]string{}
	}
	if err := vm.Set("env", env); err != nil {
		return fmt.Errorf("expr: binding env: %w", err)
	}

	if err := vm.Set("flow", contextObject(vm, data.FlowGet, data.FlowSet)); err != nil {
		return fmt.Errorf("expr: binding flow: %w", err)
	}
	if err := vm.Set("global", contextObject(vm, data.GlobalGet, data.GlobalSet)); err != nil {
		return fmt.Errorf("expr: binding global: %w", err)
	}

	if err := vm.Set("dt", dtObject()); err != nil {
		return fmt.Errorf("expr: binding dt: %w", err)
	}
	if err := vm.Set("stats", statsObject()); err != nil {
		return fmt.Errorf("expr: binding stats: %w", err)
	}
	if err := vm.Set("conv", convObject()); err != nil {
		return fmt.Errorf("expr: binding conv: %w", err)
	}
	if err := vm.Set("hash", hashObject()); err != nil {
		return fmt.Errorf("expr: binding hash: %w", err)
	}
	return nil
}

func headerToMap(h any) (map[string]any, error) {
	b, err := json.Marshal(h)
	if err != nil {
		return nil, err
	}
	m := map[string]any{}
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// contextObject builds the flow/global binding: get(key) / set(key, value).
// A nil getter always misses; a nil setter throws (rather than silently
// discarding a write the script believes succeeded).
func contextObject(vm *goja.Runtime, get func(string) (any, bool), set func(string, any) error) map[string]any {
	return map[string]any{
		"get": func(key string) any {
			if get == nil {
				return goja.Undefined()
			}
			v, ok := get(key)
			if !ok {
				return goja.Undefined()
			}
			return v
		},
		"set": func(key string, value any) {
			if set == nil {
				panic(vm.NewTypeError("expr: no writable context bound for this scope"))
			}
			if err := set(key, value); err != nil {
				panic(vm.NewGoError(err))
			}
		},
	}
}

// --- dt: date/time with IANA time zone support (MAP-130's "date/time with
// time zones") ---

// dtLayoutTokens maps moment.js-style tokens (the convention documented in
// docs/Expression-Language.md, more approachable to non-Go authors than
// Go's reference-time layout) to Go's time layout, longest tokens first so
// e.g. "SSS" isn't partially matched by a shorter token first.
var dtLayoutTokens = []struct{ token, layout string }{
	{"YYYY", "2006"},
	{"SSS", "000"},
	{"MM", "01"},
	{"DD", "02"},
	{"HH", "15"},
	{"mm", "04"},
	{"ss", "05"},
	{"Z", "Z07:00"},
}

func translateLayout(layout string) string {
	out := layout
	for _, t := range dtLayoutTokens {
		out = strings.ReplaceAll(out, t.token, t.layout)
	}
	return out
}

func dtObject() map[string]any {
	return map[string]any{
		"now":    func() int64 { return time.Now().UnixMilli() },
		"nowISO": func() string { return time.Now().UTC().Format(time.RFC3339Nano) },
		"parseISO": func(s string) (int64, error) {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return 0, fmt.Errorf("expr: dt.parseISO: %w", err)
			}
			return t.UnixMilli(), nil
		},
		"format": func(epochMs int64, layout string, tz string) (string, error) {
			loc := time.UTC
			if tz != "" {
				l, err := time.LoadLocation(tz)
				if err != nil {
					return "", fmt.Errorf("expr: dt.format: unknown time zone %q: %w", tz, err)
				}
				loc = l
			}
			return time.UnixMilli(epochMs).In(loc).Format(translateLayout(layout)), nil
		},
		"addMs":  func(epochMs, deltaMs int64) int64 { return epochMs + deltaMs },
		"diffMs": func(a, b int64) int64 { return a - b },
	}
}

// --- stats: statistical functions (MAP-130 "math ... incl. statistical
// functions") ---

func toFloatSlice(v any) ([]float64, error) {
	arr, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("expr: stats: expected an array")
	}
	out := make([]float64, len(arr))
	for i, item := range arr {
		f, ok := toFloat(item)
		if !ok {
			return nil, fmt.Errorf("expr: stats: element %d is not a number", i)
		}
		out[i] = f
	}
	return out, nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int64:
		return float64(t), true
	case int:
		return float64(t), true
	default:
		return 0, false
	}
}

func statsObject() map[string]any {
	mean := func(v any) (float64, error) {
		xs, err := toFloatSlice(v)
		if err != nil {
			return 0, err
		}
		if len(xs) == 0 {
			return 0, fmt.Errorf("expr: stats.mean: empty array")
		}
		sum := 0.0
		for _, x := range xs {
			sum += x
		}
		return sum / float64(len(xs)), nil
	}
	return map[string]any{
		"sum": func(v any) (float64, error) {
			xs, err := toFloatSlice(v)
			if err != nil {
				return 0, err
			}
			sum := 0.0
			for _, x := range xs {
				sum += x
			}
			return sum, nil
		},
		"mean": mean,
		"min": func(v any) (float64, error) {
			xs, err := toFloatSlice(v)
			if err != nil {
				return 0, err
			}
			if len(xs) == 0 {
				return 0, fmt.Errorf("expr: stats.min: empty array")
			}
			m := xs[0]
			for _, x := range xs[1:] {
				if x < m {
					m = x
				}
			}
			return m, nil
		},
		"max": func(v any) (float64, error) {
			xs, err := toFloatSlice(v)
			if err != nil {
				return 0, err
			}
			if len(xs) == 0 {
				return 0, fmt.Errorf("expr: stats.max: empty array")
			}
			m := xs[0]
			for _, x := range xs[1:] {
				if x > m {
					m = x
				}
			}
			return m, nil
		},
		// stddev is the SAMPLE standard deviation (n-1 denominator), the
		// conventional choice for signal/quality statistics.
		"stddev": func(v any) (float64, error) {
			xs, err := toFloatSlice(v)
			if err != nil {
				return 0, err
			}
			if len(xs) < 2 {
				return 0, fmt.Errorf("expr: stats.stddev: needs at least 2 values")
			}
			m, _ := mean(v)
			sq := 0.0
			for _, x := range xs {
				sq += (x - m) * (x - m)
			}
			return math.Sqrt(sq / float64(len(xs)-1)), nil
		},
		// percentile uses linear interpolation between closest ranks (the
		// common "R-7"/Excel definition).
		"percentile": func(v any, p float64) (float64, error) {
			xs, err := toFloatSlice(v)
			if err != nil {
				return 0, err
			}
			if len(xs) == 0 {
				return 0, fmt.Errorf("expr: stats.percentile: empty array")
			}
			if p < 0 || p > 100 {
				return 0, fmt.Errorf("expr: stats.percentile: p must be 0..100")
			}
			sorted := append([]float64(nil), xs...)
			sort.Float64s(sorted)
			if len(sorted) == 1 {
				return sorted[0], nil
			}
			rank := (p / 100) * float64(len(sorted)-1)
			lo := int(math.Floor(rank))
			hi := int(math.Ceil(rank))
			if lo == hi {
				return sorted[lo], nil
			}
			frac := rank - float64(lo)
			return sorted[lo]*(1-frac) + sorted[hi]*frac, nil
		},
	}
}

// --- conv: explicit, documented type casting (MAP-150) — failures throw
// rather than silently coercing to null/undefined ---

func convObject() map[string]any {
	return map[string]any{
		"toNumber": func(v any) (float64, error) {
			switch t := v.(type) {
			case float64:
				return t, nil
			case string:
				f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
				if err != nil {
					return 0, fmt.Errorf("expr: conv.toNumber: %q is not numeric", t)
				}
				return f, nil
			case bool:
				if t {
					return 1, nil
				}
				return 0, nil
			default:
				return 0, fmt.Errorf("expr: conv.toNumber: cannot convert %T", v)
			}
		},
		"toString": func(v any) string { return stringify(v) },
		"toBool": func(v any) (bool, error) {
			switch t := v.(type) {
			case bool:
				return t, nil
			case float64:
				return t != 0, nil
			case string:
				switch strings.ToLower(strings.TrimSpace(t)) {
				case "true", "1", "yes":
					return true, nil
				case "false", "0", "no", "":
					return false, nil
				}
				return false, fmt.Errorf("expr: conv.toBool: %q is not a recognized boolean", t)
			default:
				return false, fmt.Errorf("expr: conv.toBool: cannot convert %T", v)
			}
		},
		"base64Encode": func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) },
		"base64Decode": func(s string) (string, error) {
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return "", fmt.Errorf("expr: conv.base64Decode: %w", err)
			}
			return string(b), nil
		},
		"epochToISO": func(epochMs int64) string { return time.UnixMilli(epochMs).UTC().Format(time.RFC3339Nano) },
		"isoToEpoch": func(s string) (int64, error) {
			t, err := time.Parse(time.RFC3339, s)
			if err != nil {
				return 0, fmt.Errorf("expr: conv.isoToEpoch: %w", err)
			}
			return t.UnixMilli(), nil
		},
	}
}

// --- hash: hashing functions (MAP-130) ---

func hashObject() map[string]any {
	return map[string]any{
		"md5":    func(s string) string { sum := md5.Sum([]byte(s)); return hex.EncodeToString(sum[:]) },
		"sha1":   func(s string) string { sum := sha1.Sum([]byte(s)); return hex.EncodeToString(sum[:]) },
		"sha256": func(s string) string { sum := sha256.Sum256([]byte(s)); return hex.EncodeToString(sum[:]) },
	}
}
