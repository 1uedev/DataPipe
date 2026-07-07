// Package nodeutil holds small helpers shared across the Increment 7
// processor node packages that consume engine/expr (script, calculator,
// switch, filter, merge, template, lookup, delay) — building an
// engine/expr.Data from a running node's ctx is identical work in every one
// of them, so it lives here once rather than as six near-identical copies.
package nodeutil

import (
	"context"
	"os"
	"strings"

	"github.com/1uedev/DataPipe/engine/ctxstore"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/expr"
	"github.com/1uedev/DataPipe/engine/flow"
)

// ExprData builds an engine/expr.Data for evaluating an expression against
// in: Payload/Header come from the datagram, Env from the process
// environment, and the "flow"/"global" bindings (MAP-130) are wired to
// ctx's attached context store (flow.ContextStore) if one is attached —
// e.g. a node unit test calling Process directly without a live Deployment
// gets working Payload/Header/Env but no flow/global access, which is
// harmless (expr.Data's Get/Set are nil-safe).
func ExprData(ctx context.Context, in datagram.Datagram) expr.Data {
	d := expr.Data{
		Payload: in.Payload.Value,
		Header:  in.Header,
		Env:     processEnv(),
	}
	store, ok := flow.ContextStore(ctx)
	if !ok {
		return d
	}
	flowID, _ := flow.CurrentIDs(ctx)
	d.FlowGet = func(key string) (any, bool) {
		v, found, err := store.Get(ctx, ctxstore.Key{Scope: ctxstore.ScopeFlow, FlowID: flowID, Name: key})
		return v, found && err == nil
	}
	d.FlowSet = func(key string, value any) error {
		return store.Set(ctx, ctxstore.Key{Scope: ctxstore.ScopeFlow, FlowID: flowID, Name: key}, value)
	}
	d.GlobalGet = func(key string) (any, bool) {
		v, found, err := store.Get(ctx, ctxstore.Key{Scope: ctxstore.ScopeGlobal, Name: key})
		return v, found && err == nil
	}
	d.GlobalSet = func(key string, value any) error {
		return store.Set(ctx, ctxstore.Key{Scope: ctxstore.ScopeGlobal, Name: key}, value)
	}
	return d
}

func processEnv() map[string]string {
	out := map[string]string{}
	for _, kv := range os.Environ() {
		if i := strings.IndexByte(kv, '='); i >= 0 {
			out[kv[:i]] = kv[i+1:]
		}
	}
	return out
}
