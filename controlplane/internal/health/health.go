// Package health serves the liveness/readiness endpoint every DataPipe
// process exposes (Development-Plan Increment 0: "health endpoint").
package health

import (
	"encoding/json"
	"net/http"
)

// Server serves /healthz, calling check on every request to decide
// readiness (e.g. a Postgres ping).
type Server struct {
	check func() error
}

func NewServer(check func() error) *Server {
	return &Server{check: check}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		status := "ok"
		code := http.StatusOK
		if err := s.check(); err != nil {
			status = "not_ready"
			code = http.StatusServiceUnavailable
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": status})
	})
	return mux
}
