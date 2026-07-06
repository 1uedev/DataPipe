// Package webhook is the runtime-wide shared HTTP server that "http-in"
// nodes (CON-300) register routes on, and the "http-response" node
// (SNK-170) replies through — one process-wide listener rather than one
// per node, since a runtime typically exposes a single public port.
package webhook

import (
	"net/http"
	"strings"
	"sync"
)

type routeKey struct{ method, path string }

// Registry dispatches incoming requests to whichever "http-in" node
// currently owns a (method, path) pair; implements http.Handler so it can
// be used directly as an *http.Server's handler.
type Registry struct {
	mu     sync.RWMutex
	routes map[routeKey]http.HandlerFunc
}

// DefaultRegistry is the process-wide registry every "http-in" node
// registers into and cmd/runtime serves.
var DefaultRegistry = NewRegistry()

func NewRegistry() *Registry {
	return &Registry{routes: map[routeKey]http.HandlerFunc{}}
}

// Register adds a route, replacing any existing handler for the same
// (method, path) — a hot-deploy restart of the same node re-registers
// cleanly. The returned cancel func removes it.
func (r *Registry) Register(method, path string, handler http.HandlerFunc) func() {
	key := routeKey{strings.ToUpper(method), path}
	r.mu.Lock()
	r.routes[key] = handler
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		delete(r.routes, key)
		r.mu.Unlock()
	}
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.mu.RLock()
	h, ok := r.routes[routeKey{req.Method, req.URL.Path}]
	r.mu.RUnlock()
	if !ok {
		http.NotFound(w, req)
		return
	}
	h(w, req)
}
