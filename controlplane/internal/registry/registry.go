// Package registry implements the control-plane side of runtime
// registration (ARC-210, ADR-007): the walking-skeleton slice of what will
// grow into deploy orchestration and fleet management.
package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"github.com/google/uuid"
)

type runtimeState struct {
	kind         runtimev1.RuntimeKind
	version      string
	sessionToken string
	lastSeen     time.Time
}

// Service implements runtimev1.RuntimeRegistryServiceServer with an
// in-memory registry. Persisting fleet state to Postgres is out of scope
// for the Increment 0 walking skeleton.
type Service struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer

	mu       sync.Mutex
	runtimes map[string]*runtimeState
}

func NewService() *Service {
	return &Service{runtimes: make(map[string]*runtimeState)}
}

func (s *Service) Register(ctx context.Context, req *runtimev1.RegisterRequest) (*runtimev1.RegisterResponse, error) {
	if req.GetRuntimeId() == "" {
		return &runtimev1.RegisterResponse{Accepted: false}, fmt.Errorf("runtime_id is required")
	}

	token := uuid.NewString()

	s.mu.Lock()
	s.runtimes[req.GetRuntimeId()] = &runtimeState{
		kind:         req.GetKind(),
		version:      req.GetVersion(),
		sessionToken: token,
		lastSeen:     time.Now(),
	}
	s.mu.Unlock()

	return &runtimev1.RegisterResponse{Accepted: true, SessionToken: token}, nil
}

func (s *Service) Heartbeat(ctx context.Context, req *runtimev1.HeartbeatRequest) (*runtimev1.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rt, ok := s.runtimes[req.GetRuntimeId()]
	if !ok || rt.sessionToken != req.GetSessionToken() {
		return &runtimev1.HeartbeatResponse{Ok: false}, fmt.Errorf("unknown runtime or session")
	}
	rt.lastSeen = time.Now()
	return &runtimev1.HeartbeatResponse{Ok: true}, nil
}

// Count returns the number of currently registered runtimes.
func (s *Service) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runtimes)
}
