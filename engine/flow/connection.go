// Connection resolution (Increment 6, CON-110/CON-130/SEC-120): a
// connector node's config references a connection id (Flow-File-Format.md
// §4); the connection itself lives in the control plane and may reference
// a credential. The decrypted credential value is only ever sent to the
// runtime that actually needs it to connect, on demand, over
// ResolveConnection — never embedded in a deploy push or any flow export.
package flow

import (
	"context"
	"encoding/json"
	"fmt"
)

// ConnectionInfo is one connection's live, resolved state: its declared
// type, non-secret config, and (if it references one) the decrypted
// credential value.
type ConnectionInfo struct {
	Type           string
	Config         json.RawMessage
	CredentialJSON json.RawMessage // nil if the connection has no credential
}

// ConnectionResolver resolves a connection id into its live info.
// Implemented by engine/internal/runtimeclient over the ResolveConnection
// RPC; a connector calls flow.ResolveConnection(ctx) rather than using this
// interface directly, so it never needs to know how resolution happens.
type ConnectionResolver interface {
	ResolveConnection(ctx context.Context, connectionID string) (ConnectionInfo, error)
}

type noopConnectionResolver struct{}

func (noopConnectionResolver) ResolveConnection(context.Context, string) (ConnectionInfo, error) {
	return ConnectionInfo{}, fmt.Errorf("flow: connection resolver not configured")
}

// NoopConnectionResolver is the default resolver: every call fails clearly
// rather than panicking, so tests that don't set one up get an honest error
// instead of a nil-pointer crash.
var NoopConnectionResolver ConnectionResolver = noopConnectionResolver{}

type connectionCtx struct {
	resolver     ConnectionResolver
	connectionID string
}

type connectionCtxKey struct{}

// WithConnection attaches a resolver + connection id to ctx so a later
// ResolveConnection(ctx) call resolves against it. Deployment.startNode
// calls this once per node start (mirroring the debug-context injection
// pattern); node packages' own tests also use it directly to exercise
// connection-dependent code paths (e.g. auth) without needing a full
// Deployment.
func WithConnection(ctx context.Context, resolver ConnectionResolver, connectionID string) context.Context {
	return context.WithValue(ctx, connectionCtxKey{}, connectionCtx{resolver: resolver, connectionID: connectionID})
}

// ResolveConnection resolves the calling node's configured connection.
// Re-resolving on every call (rather than caching once at node start) lets
// a connector's own reconnect loop (CON-130) naturally pick up rotated
// credentials without a redeploy.
func ResolveConnection(ctx context.Context) (ConnectionInfo, error) {
	v, ok := ctx.Value(connectionCtxKey{}).(connectionCtx)
	if !ok {
		return ConnectionInfo{}, fmt.Errorf("flow: no connection context (node not started under a Deployment)")
	}
	if v.connectionID == "" {
		return ConnectionInfo{}, fmt.Errorf("flow: this node has no connection configured")
	}
	return v.resolver.ResolveConnection(ctx, v.connectionID)
}
