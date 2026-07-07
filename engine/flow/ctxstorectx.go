// Context-store wiring (PROC-410, ENG-120): every node instance can reach
// the same node/flow/global-scoped store Increment 1 built
// (engine/ctxstore) via its ctx, exactly like WithConnection/ResolveConnection
// (Increment 6) — a Deployment owns one store, injected per node at start.
package flow

import (
	"context"

	"github.com/1uedev/DataPipe/engine/ctxstore"
)

type ctxStoreCtxKey struct{}

// WithContextStore attaches store to ctx for a node's Process/Run to read
// via ContextStore.
func WithContextStore(ctx context.Context, store ctxstore.Store) context.Context {
	return context.WithValue(ctx, ctxStoreCtxKey{}, store)
}

// ContextStore retrieves the store attached by WithContextStore, if any.
func ContextStore(ctx context.Context) (ctxstore.Store, bool) {
	store, ok := ctx.Value(ctxStoreCtxKey{}).(ctxstore.Store)
	return store, ok
}
