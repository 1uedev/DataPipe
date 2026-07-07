package expr

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// asFloat tolerates goja's Export() returning int64 for whole-number
// results and float64 otherwise.
func asFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	default:
		t.Fatalf("value %v (%T) is not numeric", v, v)
		return 0
	}
}

func TestMAP130_IsExpressionRecognizesWholeValueConvention(t *testing.T) {
	code, ok := IsExpression(json.RawMessage(`"={{payload.value}}"`))
	if !ok || code != "payload.value" {
		t.Fatalf("code=%q ok=%v", code, ok)
	}
	if _, ok := IsExpression(json.RawMessage(`"just a literal"`)); ok {
		t.Fatal("expected a plain string not to be recognized as an expression")
	}
	if _, ok := IsExpression(json.RawMessage(`42`)); ok {
		t.Fatal("expected a non-string JSON value not to be recognized as an expression")
	}
}

func TestMAP130_EvalReadsPayloadHeaderTags(t *testing.T) {
	d := Data{
		Payload: map[string]any{"value": 21.5},
		Header:  datagram.Header{ID: "d1", Tags: map[string]string{"line": "3"}},
	}
	v, err := Eval(context.Background(), "payload.value * 2", d, 0)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if asFloat(t, v) != 43 {
		t.Errorf("value = %v, want 43", v)
	}

	v, err = Eval(context.Background(), "header.id + '-' + tags.line", d, 0)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if v != "d1-3" {
		t.Errorf("value = %v, want d1-3", v)
	}
}

func TestMAP130_EvalBindsEnv(t *testing.T) {
	d := Data{Env: map[string]string{"FOO": "bar"}}
	v, err := Eval(context.Background(), "env.FOO", d, 0)
	if err != nil {
		t.Fatalf("Eval: %v", err)
	}
	if v != "bar" {
		t.Errorf("value = %v, want bar", v)
	}
}

func TestMAP130_EvalBindsFlowGlobalContext(t *testing.T) {
	store := map[string]any{}
	d := Data{
		FlowGet: func(k string) (any, bool) { v, ok := store[k]; return v, ok },
		FlowSet: func(k string, v any) error { store[k] = v; return nil },
	}
	if _, err := Eval(context.Background(), "flow.set('count', 5)", d, 0); err != nil {
		t.Fatalf("Eval set: %v", err)
	}
	v, err := Eval(context.Background(), "flow.get('count')", d, 0)
	if err != nil {
		t.Fatalf("Eval get: %v", err)
	}
	if asFloat(t, v) != 5 {
		t.Errorf("value = %v, want 5", v)
	}
}

func TestMAP130_EvalWithoutWritableContextThrows(t *testing.T) {
	d := Data{}
	if _, err := Eval(context.Background(), "flow.set('x', 1)", d, 0); err == nil {
		t.Fatal("expected an error: no FlowSet bound")
	}
}

func TestENG150_EvalTimesOutOnInfiniteLoop(t *testing.T) {
	start := time.Now()
	_, err := Eval(context.Background(), "while(true) {}", Data{}, 100*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Eval took %v, expected to be interrupted promptly", elapsed)
	}
}

func TestENG150_EvalRespectsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	start := time.Now()
	_, err := Eval(ctx, "while(true) {}", Data{}, 5*time.Second)
	if err == nil {
		t.Fatal("expected an interruption error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Eval took %v, expected to stop once ctx was cancelled", elapsed)
	}
}

func TestSEC150_NoFilesystemOrNetworkGlobalsBound(t *testing.T) {
	for _, global := range []string{"require", "fetch", "process", "fs"} {
		_, err := Eval(context.Background(), "typeof "+global+" === 'undefined'", Data{}, 0)
		if err != nil {
			t.Fatalf("Eval: %v", err)
		}
	}
}

func TestMAP130_CompileAndRunReusesRuntime(t *testing.T) {
	prog, err := Compile("payload.n + 1")
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	rt := New()
	for i, want := range []float64{2, 3, 4} {
		v, err := rt.Run(context.Background(), prog, Data{Payload: map[string]any{"n": float64(i + 1)}}, 0)
		if err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
		if asFloat(t, v) != want {
			t.Errorf("run %d = %v, want %v", i, v, want)
		}
	}
}

func TestMAP130_ResolveValueLiteralPassesThrough(t *testing.T) {
	v, err := ResolveValue(context.Background(), json.RawMessage(`{"a":1,"b":"x"}`), Data{}, 0)
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	m := v.(map[string]any)
	if asFloat(t, m["a"]) != 1 || m["b"] != "x" {
		t.Errorf("resolved = %+v", m)
	}
}

func TestMAP130_ResolveValueEvaluatesExpression(t *testing.T) {
	v, err := ResolveValue(context.Background(), json.RawMessage(`"={{payload.x + 1}}"`),
		Data{Payload: map[string]any{"x": 41.0}}, 0)
	if err != nil {
		t.Fatalf("ResolveValue: %v", err)
	}
	if asFloat(t, v) != 42 {
		t.Errorf("value = %v, want 42", v)
	}
}

func TestMAP130_ResolveDeepWalksNestedStructure(t *testing.T) {
	raw := json.RawMessage(`{
		"literal": "keep-me",
		"expr": "={{payload.value}}",
		"nested": {"inner": "={{payload.value * 2}}"},
		"list": ["a", "={{payload.value}}"]
	}`)
	v, err := ResolveDeep(context.Background(), raw, Data{Payload: map[string]any{"value": 10.0}}, 0)
	if err != nil {
		t.Fatalf("ResolveDeep: %v", err)
	}
	m := v.(map[string]any)
	if m["literal"] != "keep-me" {
		t.Errorf("literal = %v", m["literal"])
	}
	if asFloat(t, m["expr"]) != 10 {
		t.Errorf("expr = %v", m["expr"])
	}
	if asFloat(t, m["nested"].(map[string]any)["inner"]) != 20 {
		t.Errorf("nested.inner = %v", m["nested"])
	}
	list := m["list"].([]any)
	if list[0] != "a" || asFloat(t, list[1]) != 10 {
		t.Errorf("list = %+v", list)
	}
}

func TestPROC130_RenderTemplateSubstitutesPlaceholders(t *testing.T) {
	out, err := RenderTemplate(context.Background(), "sensor {{tags.line}} = {{payload.value}} at {{payload.value > 20 ? 'high' : 'low'}}",
		Data{Payload: map[string]any{"value": 25.0}, Header: datagram.Header{Tags: map[string]string{"line": "3"}}}, 0)
	if err != nil {
		t.Fatalf("RenderTemplate: %v", err)
	}
	if out != "sensor 3 = 25 at high" {
		t.Errorf("out = %q", out)
	}
}

func TestPROC130_RenderTemplateNoPlaceholdersPassesThrough(t *testing.T) {
	out, err := RenderTemplate(context.Background(), "no placeholders here", Data{}, 0)
	if err != nil {
		t.Fatalf("RenderTemplate: %v", err)
	}
	if out != "no placeholders here" {
		t.Errorf("out = %q", out)
	}
}

func TestPROC130_RenderTemplatePropagatesEvalError(t *testing.T) {
	if _, err := RenderTemplate(context.Background(), "{{ this is not valid js !! }}", Data{}, 0); err == nil {
		t.Fatal("expected an error")
	}
}

func TestMAP130_StdlibDtStatsConvHash(t *testing.T) {
	cases := []struct {
		name string
		code string
		want string
	}{
		{"dt.format", "dt.format(dt.parseISO('2026-01-15T10:30:00Z'), 'YYYY-MM-DD', 'UTC')", "2026-01-15"},
		{"stats.mean", "stats.mean([1,2,3,4]).toString()", "2.5"},
		{"stats.percentile", "stats.percentile([1,2,3,4,5], 50).toString()", "3"},
		{"conv.base64", "conv.base64Decode(conv.base64Encode('hello'))", "hello"},
		{"hash.sha256", "hash.sha256('abc')", "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"[:64]},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v, err := Eval(context.Background(), c.code, Data{}, 0)
			if err != nil {
				t.Fatalf("Eval: %v", err)
			}
			got := stringify(v)
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

func TestMAP150_ConvCastFailureThrowsRatherThanNull(t *testing.T) {
	_, err := Eval(context.Background(), "conv.toNumber('not-a-number')", Data{}, 0)
	if err == nil {
		t.Fatal("expected a cast-failure error, not silent null")
	}
	if !strings.Contains(err.Error(), "not-a-number") {
		t.Errorf("error = %v, want it to mention the offending value", err)
	}
}
