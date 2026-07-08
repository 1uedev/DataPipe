// Resolved-environment-profile wiring (VCS-140): a Deployment's
// control-plane-resolved env vars are attached per node at start, exactly
// like WithContextStore/WithConnection above.
package flow

import "context"

type resolvedEnvCtxKey struct{}

// WithResolvedEnv attaches env to ctx for a node's Process/Run to read via
// ResolvedEnv.
func WithResolvedEnv(ctx context.Context, env map[string]string) context.Context {
	return context.WithValue(ctx, resolvedEnvCtxKey{}, env)
}

// ResolvedEnv retrieves the map attached by WithResolvedEnv, if any.
func ResolvedEnv(ctx context.Context) (map[string]string, bool) {
	env, ok := ctx.Value(resolvedEnvCtxKey{}).(map[string]string)
	return env, ok
}
