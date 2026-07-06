// Package health serves the liveness/readiness endpoint every DataPipe
// process exposes (Development-Plan Increment 0: "health endpoint").
package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// Server tracks a single readiness flag and serves it as JSON on /healthz.
type Server struct {
	ready atomic.Bool
}

func NewServer() *Server {
	return &Server{}
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ready := s.ready.Load()
		status := "ok"
		code := http.StatusOK
		if !ready {
			status = "not_ready"
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
	})
	return mux
}
