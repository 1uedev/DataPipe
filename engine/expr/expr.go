// Package expr implements MAP-130's "one documented expression syntax
// platform-wide": a sandboxed JavaScript expression evaluator (goja — no
// filesystem or network globals are ever bound in, so a script has no I/O
// capability by construction, per PROC-100/SEC-150) with access to the
// current datagram's payload/header/tags, environment variables, and
// flow-/global-scoped state (bound via callbacks so this package has no
// dependency on engine/ctxstore or engine/flow).
//
// Flow-File-Format.md §2 defines the convention every node config field
// follows: a JSON string value starting with "={{" and ending with "}}" is
// a whole-value expression (MAP-130); everything else is a literal.
// RenderTemplate implements the separate "mixed template" mode used for
// building strings (e.g. a URL or table name) from literal text with
// embedded "{{ expr }}" placeholders (PROC-130).
package expr

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/dop251/goja"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// DefaultTimeout bounds a single expression's execution (ENG-150: "script
// CPU/time limits"; violations must never hang a node goroutine forever).
const DefaultTimeout = 2 * time.Second

// Data is everything an expression can see (MAP-130): the current
// datagram's payload/header/tags, environment variables, and flow-/global-
// scoped context store access. Get/Set are nil-safe: a nil Get always
// misses, a nil Set is a no-op returning an error.
type Data struct {
	Payload any
	Header  datagram.Header
	Env     map[string]string

	FlowGet   func(key string) (value any, found bool)
	FlowSet   func(key string, value any) error
	GlobalGet func(key string) (value any, found bool)
	GlobalSet func(key string, value any) error
}

// exprPattern matches Flow-File-Format.md §2's whole-value expression
// convention.
var exprPattern = regexp.MustCompile(`(?s)^=\{\{(.*)\}\}$`)

// IsExpression reports whether raw (a config field's still-JSON-encoded
// value) is a whole-value expression, returning the inner JS source.
func IsExpression(raw json.RawMessage) (code string, ok bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return isExpressionString(s)
}

func isExpressionString(s string) (code string, ok bool) {
	m := exprPattern.FindStringSubmatch(s)
	if m == nil {
		return "", false
	}
	return strings.TrimSpace(m[1]), true
}

// Runtime wraps one goja.Runtime for reuse across many Eval calls against
// the same or different compiled Programs. Not goroutine-safe — a Runtime
// must be owned by a single goroutine at a time, matching how DataPipe
// already drives one node instance's Process calls serially from its own
// runner goroutine.
type Runtime struct {
	vm *goja.Runtime
}

// New creates a Runtime. Construct one per node instance (in the node's
// factory) and reuse it across every Process call for that instance,
// rather than creating one per datagram.
func New() *Runtime {
	return &Runtime{vm: goja.New()}
}

// VM exposes the underlying goja.Runtime for callers that need to bind
// additional globals beyond Data (e.g. PROC-100's script node adding
// emit()/console/node-scoped state on top of the standard bindings). Most
// callers should use Run instead of driving the VM directly.
func (r *Runtime) VM() *goja.Runtime { return r.vm }

// BindGlobals sets every global described by Data (payload/header/tags/env/
// flow/global/dt/stats/conv/hash) on vm without running anything — exported
// for callers building their own goja.Runtime usage on top of the same
// sandboxed bindings (see VM).
func BindGlobals(vm *goja.Runtime, data Data) error { return bind(vm, data) }

// Program is JavaScript source compiled once by Compile, for repeated
// execution via Runtime.Run without re-parsing on every datagram.
type Program struct {
	prog *goja.Program
	src  string
}

// Compile parses code once. Errors are syntax errors in the expression
// itself (surfaced at node-configuration time, not per-datagram).
func Compile(code string) (*Program, error) {
	prog, err := goja.Compile("expr", code, false)
	if err != nil {
		return nil, fmt.Errorf("expr: compiling %q: %w", code, err)
	}
	return &Program{prog: prog, src: code}, nil
}

// Run executes prog against data, bounded by timeout (DefaultTimeout if
// <= 0). Sandboxed: no filesystem/network globals are ever bound into the
// runtime, so a script has no I/O capability regardless of what it tries to
// call. Interrupted (and returns an error) if ctx is cancelled first.
func (r *Runtime) Run(ctx context.Context, prog *Program, data Data, timeout time.Duration) (any, error) {
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	if err := bind(r.vm, data); err != nil {
		return nil, err
	}

	done := make(chan struct{})
	go func() {
		select {
		case <-time.After(timeout):
			r.vm.Interrupt("expr: execution timed out")
		case <-ctx.Done():
			r.vm.Interrupt("expr: context cancelled")
		case <-done:
		}
	}()
	defer close(done)

	v, err := r.vm.RunProgram(prog.prog)
	if err != nil {
		return nil, fmt.Errorf("expr: evaluating %q: %w", prog.src, err)
	}
	return v.Export(), nil
}

// Eval is a one-shot convenience: compiles code and runs it against a
// throwaway Runtime. Callers that evaluate the SAME code repeatedly (most
// node types, once per datagram) should Compile once at construction and
// reuse a Runtime via Run instead — Eval re-parses code on every call.
func Eval(ctx context.Context, code string, data Data, timeout time.Duration) (any, error) {
	prog, err := Compile(code)
	if err != nil {
		return nil, err
	}
	return New().Run(ctx, prog, data, timeout)
}

// ResolveValue resolves one config field that may be a literal or a
// whole-value expression (MAP-130): if raw is an expression per
// IsExpression, evaluates it; otherwise unmarshals raw as a plain literal.
func ResolveValue(ctx context.Context, raw json.RawMessage, data Data, timeout time.Duration) (any, error) {
	if code, ok := IsExpression(raw); ok {
		return Eval(ctx, code, data, timeout)
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("expr: invalid literal: %w", err)
	}
	return v, nil
}

// ResolveDeep walks raw's JSON tree, resolving every string leaf that
// matches the whole-value expression convention and leaving everything
// else untouched — "every node parameter can be a literal or an
// expression" (MAP-130), applied recursively through nested objects/arrays.
func ResolveDeep(ctx context.Context, raw json.RawMessage, data Data, timeout time.Duration) (any, error) {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil, fmt.Errorf("expr: invalid config: %w", err)
	}
	return resolveDeepValue(ctx, v, data, timeout)
}

func resolveDeepValue(ctx context.Context, v any, data Data, timeout time.Duration) (any, error) {
	switch t := v.(type) {
	case string:
		if code, ok := isExpressionString(t); ok {
			return Eval(ctx, code, data, timeout)
		}
		return t, nil
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			resolved, err := resolveDeepValue(ctx, val, data, timeout)
			if err != nil {
				return nil, err
			}
			out[k] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(t))
		for i, val := range t {
			resolved, err := resolveDeepValue(ctx, val, data, timeout)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	default:
		return v, nil
	}
}

// templatePlaceholder matches "{{ expr }}" occurrences for RenderTemplate's
// mixed-template mode (PROC-130 and similar string-building fields) — a
// distinct, unprefixed convention from IsExpression's whole-value "={{ }}".
var templatePlaceholder = regexp.MustCompile(`\{\{\s*(.+?)\s*\}\}`)

// RenderTemplate substitutes every "{{ expr }}" placeholder in tpl with its
// evaluated (and stringified) result, splicing into the surrounding literal
// text. Non-string results are JSON-stringified; strings are spliced
// verbatim.
func RenderTemplate(ctx context.Context, tpl string, data Data, timeout time.Duration) (string, error) {
	var firstErr error
	result := templatePlaceholder.ReplaceAllStringFunc(tpl, func(match string) string {
		if firstErr != nil {
			return match
		}
		sub := templatePlaceholder.FindStringSubmatch(match)
		v, err := Eval(ctx, sub[1], data, timeout)
		if err != nil {
			firstErr = err
			return match
		}
		return stringify(v)
	})
	if firstErr != nil {
		return "", firstErr
	}
	return result, nil
}

func stringify(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	}
}
