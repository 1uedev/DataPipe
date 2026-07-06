// Package registry implements the control-plane side of runtime
// registration (ARC-210, ADR-007) and deploy orchestration (Increment 3):
// runtimes register and heartbeat, then open a DeployStream the control
// plane pushes flow deployments down as REST API deploys happen.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
	"github.com/google/uuid"

	"github.com/1uedev/DataPipe/controlplane/internal/debughub"
)

// ConnectionInfo is a connection's resolved type/config and (if it
// references one) decrypted credential (Increment 6, CON-110/SEC-120).
type ConnectionInfo struct {
	Type           string
	ConfigJSON     json.RawMessage
	CredentialJSON json.RawMessage
}

// ConnectionResolver looks up a connection and decrypts its credential if
// it has one. Implemented by controlplane/internal/api.ConnectionResolver;
// kept as an interface here so registry never depends on api/crypto.
type ConnectionResolver interface {
	ResolveConnection(ctx context.Context, connectionID string) (ConnectionInfo, error)
}

// deployChanBuffer bounds how many pending deploy commands a runtime can
// have queued before DeployFlow reports it as unavailable.
const deployChanBuffer = 8

type runtimeState struct {
	kind         runtimev1.RuntimeKind
	version      string
	sessionToken string
	lastSeen     time.Time
	deployCh     chan *runtimev1.DeployStreamResponse // non-nil once DeployStream is open
}

// Service implements runtimev1.RuntimeRegistryServiceServer with an
// in-memory registry. Persisting fleet state to Postgres is out of scope
// for now (see TODO.md).
type Service struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer

	mu           sync.Mutex
	runtimes     map[string]*runtimeState
	debugHub     *debughub.Hub
	connResolver ConnectionResolver
}

func NewService() *Service {
	s := &Service{runtimes: make(map[string]*runtimeState)}
	s.debugHub = debughub.New(s.validSession)
	return s
}

// DebugHub exposes the live-debugging hub (Increment 5) so the REST/WS
// layer can subscribe browsers and look up cached full payloads.
func (s *Service) DebugHub() *debughub.Hub { return s.debugHub }

// SetConnectionResolver wires in the resolver used by the ResolveConnection
// RPC (Increment 6, CON-110). Must be called before any runtime dials in
// with a node that references a connection.
func (s *Service) SetConnectionResolver(r ConnectionResolver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connResolver = r
}

// ResolveConnection is called by a runtime whenever a connector node needs
// its configured connection (Increment 6, CON-110/SEC-120): the decrypted
// credential, if any, is returned only here, to the runtime that asked —
// never embedded in a deploy push.
func (s *Service) ResolveConnection(ctx context.Context, req *runtimev1.ResolveConnectionRequest) (*runtimev1.ResolveConnectionResponse, error) {
	if !s.validSession(req.GetRuntimeId(), req.GetSessionToken()) {
		return nil, fmt.Errorf("unknown runtime or session")
	}
	s.mu.Lock()
	resolver := s.connResolver
	s.mu.Unlock()
	if resolver == nil {
		return nil, fmt.Errorf("connection resolution not configured")
	}

	info, err := resolver.ResolveConnection(ctx, req.GetConnectionId())
	if err != nil {
		return nil, err
	}
	return &runtimev1.ResolveConnectionResponse{
		Type:           info.Type,
		ConfigJson:     string(info.ConfigJSON),
		CredentialJson: string(info.CredentialJSON),
	}, nil
}

func (s *Service) validSession(runtimeID, sessionToken string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimes[runtimeID]
	return ok && rt.sessionToken == sessionToken
}

// DebugChannel is opened by a runtime once, right after Register, and kept
// open for the runtime's lifetime (Increment 5, DBG-100/110/120/170).
func (s *Service) DebugChannel(stream runtimev1.RuntimeRegistryService_DebugChannelServer) error {
	return s.debugHub.Serve(stream)
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

// DeployStream is opened by a runtime once, right after Register, and kept
// open for the runtime's lifetime; DeployFlow pushes into the channel this
// registers, and every pushed command is forwarded down the stream.
func (s *Service) DeployStream(req *runtimev1.DeployStreamRequest, stream runtimev1.RuntimeRegistryService_DeployStreamServer) error {
	runtimeID := req.GetRuntimeId()

	s.mu.Lock()
	rt, ok := s.runtimes[runtimeID]
	if !ok || rt.sessionToken != req.GetSessionToken() {
		s.mu.Unlock()
		return fmt.Errorf("unknown runtime or session")
	}
	ch := make(chan *runtimev1.DeployStreamResponse, deployChanBuffer)
	rt.deployCh = ch
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if cur, ok := s.runtimes[runtimeID]; ok && cur.deployCh == ch {
			cur.deployCh = nil
		}
		s.mu.Unlock()
	}()

	for {
		select {
		case cmd := <-ch:
			if err := stream.Send(cmd); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// DeployFlow implements controlplane/internal/api.Deployer: it pushes the
// flow to every currently connected runtime with an open DeployStream.
// Runtime-group assignment (Flow-File-Format.md's runtimeAssignment,
// UI-220) is deferred to the fleet-management work of Increment 9 — for
// now every connected runtime receives every deploy.
func (s *Service) DeployFlow(ctx context.Context, flowID string, version int64, flowJSON string) error {
	s.mu.Lock()
	var channels []chan *runtimev1.DeployStreamResponse
	for _, rt := range s.runtimes {
		if rt.deployCh != nil {
			channels = append(channels, rt.deployCh)
		}
	}
	s.mu.Unlock()

	if len(channels) == 0 {
		return fmt.Errorf("no runtime currently connected")
	}

	cmd := &runtimev1.DeployStreamResponse{FlowId: flowID, Version: version, FlowJson: flowJSON}
	for _, ch := range channels {
		select {
		case ch <- cmd:
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fmt.Errorf("runtime deploy queue full")
		}
	}
	return nil
}

// RuntimeSnapshot is a read-only view of one registered runtime, for the
// fleet listing (GET /runtimes).
type RuntimeSnapshot struct {
	RuntimeID string
	Kind      string
	Version   string
	LastSeen  time.Time
}

// ListRuntimes returns every currently registered runtime.
func (s *Service) ListRuntimes() []RuntimeSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	snaps := make([]RuntimeSnapshot, 0, len(s.runtimes))
	for id, rt := range s.runtimes {
		snaps = append(snaps, RuntimeSnapshot{
			RuntimeID: id,
			Kind:      runtimeKindString(rt.kind),
			Version:   rt.version,
			LastSeen:  rt.lastSeen,
		})
	}
	return snaps
}

func runtimeKindString(k runtimev1.RuntimeKind) string {
	if k == runtimev1.RuntimeKind_RUNTIME_KIND_EDGE {
		return "edge"
	}
	return "server"
}

// Count returns the number of currently registered runtimes.
func (s *Service) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runtimes)
}
