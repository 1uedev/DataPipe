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

// ExecutionEvent is one runtime-reported triggered-execution lifecycle
// event (Increment 8, ENG-130/DBG-140), decoupled from the proto message so
// controlplane/internal/api's ExecutionStore implementation doesn't need to
// depend on runtimev1.
type ExecutionEvent struct {
	ExecutionID, FlowID, Phase                            string
	TimeUnixMs                                            int64
	TriggerNodeID, TriggerKind, ReRunOf, SeedDatagramJSON string
	NodeID, Port                                          string
	Attempt                                               int32
	DurationUs                                            int64
	InputJSON, OutputsJSON                                string
	ErrorMessage, ErrorCode, ErrorStack                   string
	Status, Reason                                        string
}

// DeadLetterEvent is one runtime-reported dead-lettered datagram
// (Increment 8, ERR-130).
type DeadLetterEvent struct {
	FlowID, NodeID, Port, Reason, DatagramJSON string
	TimeUnixMs                                 int64
}

// ExecutionStore persists triggered-execution and dead-letter events
// (Increment 8) and marks a disconnected runtime's still-open executions
// crashed (ERR-150). Implemented by controlplane/internal/api.Store; kept
// as an interface here so registry never depends on api/db.
type ExecutionStore interface {
	RecordExecutionEvent(ctx context.Context, runtimeID string, ev ExecutionEvent) error
	RecordDeadLetter(ctx context.Context, runtimeID string, ev DeadLetterEvent) error
	MarkRuntimeExecutionsCrashed(ctx context.Context, runtimeID string) error
}

// DeployedFlowInfo is one currently-deployed flow's canonical content,
// decoupled from controlplane/internal/api.DeployedFlow so registry never
// depends on api/db.
type DeployedFlowInfo struct {
	FlowID           string
	Version          int64
	ContentJSON      string
	DefaultErrorFlow string
}

// DeployedFlowsLister answers "what's currently deployed", so a runtime
// that just (re)registered gets every deployed flow re-pushed automatically
// (ERR-150: "runtime restart restores all deployed flows... automatically").
// Implemented (via a small adapter) by controlplane/internal/api.Store.
type DeployedFlowsLister interface {
	ListDeployedFlows(ctx context.Context) ([]DeployedFlowInfo, error)
}

// deployChanBuffer bounds how many pending deploy commands a runtime can
// have queued before DeployFlow reports it as unavailable.
const deployChanBuffer = 8

// eventChanBuffer bounds how many pending execution/dead-letter commands a
// runtime can have queued before SendExecutionCommand reports it as
// unavailable.
const eventChanBuffer = 8

type runtimeState struct {
	kind         runtimev1.RuntimeKind
	version      string
	sessionToken string
	lastSeen     time.Time
	deployCh     chan *runtimev1.DeployStreamResponse // non-nil once DeployStream is open
	eventCh      chan *runtimev1.EventChannelResponse // non-nil once EventChannel is open
}

// Service implements runtimev1.RuntimeRegistryServiceServer with an
// in-memory registry. Persisting fleet state to Postgres is out of scope
// for now (see TODO.md).
type Service struct {
	runtimev1.UnimplementedRuntimeRegistryServiceServer

	mu             sync.Mutex
	runtimes       map[string]*runtimeState
	debugHub       *debughub.Hub
	connResolver   ConnectionResolver
	executionStore ExecutionStore
	deployedFlows  DeployedFlowsLister
}

func NewService() *Service {
	s := &Service{runtimes: make(map[string]*runtimeState)}
	s.debugHub = debughub.New(s.validSession)
	return s
}

// SetExecutionStore wires in the durable store for triggered-execution and
// dead-letter events (Increment 8). Must be called before any runtime
// opens its EventChannel.
func (s *Service) SetExecutionStore(store ExecutionStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.executionStore = store
}

// SetDeployedFlowsLister wires in the source of truth for "what's currently
// deployed", used to re-push every deployed flow when a runtime's
// DeployStream opens (ERR-150).
func (s *Service) SetDeployedFlowsLister(lister DeployedFlowsLister) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deployedFlows = lister
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
// registers, and every pushed command is forwarded down the stream. Every
// currently-deployed flow is queued immediately, before the main loop
// starts, so a (re)connecting runtime — including one recovering from a
// crash — has all of them re-pushed automatically without waiting for the
// next REST deploy (ERR-150: "runtime restart restores all deployed flows
// and durable state automatically").
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
	lister := s.deployedFlows
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if cur, ok := s.runtimes[runtimeID]; ok && cur.deployCh == ch {
			cur.deployCh = nil
		}
		s.mu.Unlock()
	}()

	if lister != nil {
		flows, err := lister.ListDeployedFlows(stream.Context())
		if err == nil {
			for _, f := range flows {
				select {
				case ch <- &runtimev1.DeployStreamResponse{FlowId: f.FlowID, Version: f.Version, FlowJson: f.ContentJSON, DefaultErrorFlow: f.DefaultErrorFlow}:
				default:
				}
			}
		}
	}

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
// now every connected runtime receives every deploy. defaultErrorFlow is
// the owning project's ERR-120 fallback error-handler flow id.
func (s *Service) DeployFlow(ctx context.Context, flowID string, version int64, flowJSON, defaultErrorFlow string) error {
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

	cmd := &runtimev1.DeployStreamResponse{FlowId: flowID, Version: version, FlowJson: flowJSON, DefaultErrorFlow: defaultErrorFlow}
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

// EventChannel is opened by a runtime once, right after Register, and kept
// open for the runtime's lifetime (Increment 8, ENG-130/DBG-140/ERR-130):
// the runtime durably reports every triggered-execution lifecycle event and
// dead-lettered datagram, and RunExecution/CancelExecution/
// ReinjectDeadLetter push commands back down the same stream. Only one
// goroutine ever calls stream.Send (this loop) and only one calls
// stream.Recv (the uplink goroutine below), matching grpc-go's
// one-goroutine-per-direction safety requirement.
func (s *Service) EventChannel(stream runtimev1.RuntimeRegistryService_EventChannelServer) error {
	hello, err := stream.Recv()
	if err != nil {
		return err
	}
	if !s.validSession(hello.GetRuntimeId(), hello.GetSessionToken()) {
		return fmt.Errorf("unknown runtime or session")
	}
	runtimeID := hello.GetRuntimeId()

	s.mu.Lock()
	rt, ok := s.runtimes[runtimeID]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("unknown runtime")
	}
	ch := make(chan *runtimev1.EventChannelResponse, eventChanBuffer)
	rt.eventCh = ch
	executionStore := s.executionStore
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		if cur, ok := s.runtimes[runtimeID]; ok && cur.eventCh == ch {
			cur.eventCh = nil
		}
		s.mu.Unlock()
		// A runtime whose EventChannel just closed (crash, restart, network
		// loss) can no longer be running whatever executions it last
		// reported as running/waiting (ERR-150).
		if executionStore != nil {
			_ = executionStore.MarkRuntimeExecutionsCrashed(context.Background(), runtimeID)
		}
	}()

	s.handleEventUplink(runtimeID, hello)

	recvErr := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			s.handleEventUplink(runtimeID, req)
		}
	}()

	for {
		select {
		case cmd := <-ch:
			if err := stream.Send(cmd); err != nil {
				return err
			}
		case err := <-recvErr:
			return err
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

func (s *Service) handleEventUplink(runtimeID string, req *runtimev1.EventChannelRequest) {
	s.mu.Lock()
	store := s.executionStore
	s.mu.Unlock()
	if store == nil {
		return
	}
	ctx := context.Background()
	switch p := req.GetPayload().(type) {
	case *runtimev1.EventChannelRequest_ExecutionEvent:
		e := p.ExecutionEvent
		_ = store.RecordExecutionEvent(ctx, runtimeID, ExecutionEvent{
			ExecutionID: e.GetExecutionId(), FlowID: e.GetFlowId(), Phase: e.GetPhase(), TimeUnixMs: e.GetTimeUnixMs(),
			TriggerNodeID: e.GetTriggerNodeId(), TriggerKind: e.GetTriggerKind(), ReRunOf: e.GetReRunOf(), SeedDatagramJSON: e.GetSeedDatagramJson(),
			NodeID: e.GetNodeId(), Port: e.GetPort(), Attempt: e.GetAttempt(), DurationUs: e.GetDurationUs(),
			InputJSON: e.GetInputJson(), OutputsJSON: e.GetOutputsJson(),
			ErrorMessage: e.GetErrorMessage(), ErrorCode: e.GetErrorCode(), ErrorStack: e.GetErrorStack(),
			Status: e.GetStatus(), Reason: e.GetReason(),
		})
	case *runtimev1.EventChannelRequest_DeadLetterEvent:
		e := p.DeadLetterEvent
		_ = store.RecordDeadLetter(ctx, runtimeID, DeadLetterEvent{
			FlowID: e.GetFlowId(), NodeID: e.GetNodeId(), Port: e.GetPort(), Reason: e.GetReason(),
			DatagramJSON: e.GetDatagramJson(), TimeUnixMs: e.GetTimeUnixMs(),
		})
	}
}

// sendExecutionCommand pushes cmd to every runtime with an open
// EventChannel. Runtime-group targeting (like DeployFlow's) is deferred to
// Increment 9 — today every connected runtime receives every command, which
// is harmless since a command referencing a flow/execution/dead-letter id
// that runtime doesn't actually hold simply fails to find it and no-ops.
func (s *Service) sendExecutionCommand(ctx context.Context, cmd *runtimev1.EventChannelResponse) error {
	s.mu.Lock()
	var channels []chan *runtimev1.EventChannelResponse
	for _, rt := range s.runtimes {
		if rt.eventCh != nil {
			channels = append(channels, rt.eventCh)
		}
	}
	s.mu.Unlock()

	if len(channels) == 0 {
		return fmt.Errorf("no runtime currently connected")
	}
	for _, ch := range channels {
		select {
		case ch <- cmd:
		case <-ctx.Done():
			return ctx.Err()
		default:
			return fmt.Errorf("runtime event command queue full")
		}
	}
	return nil
}

// RunExecution implements controlplane/internal/api.ExecutionCommander
// (DBG-140 re-run).
func (s *Service) RunExecution(ctx context.Context, flowID, from, nodeID, port, datagramJSON, reRunOf string) error {
	return s.sendExecutionCommand(ctx, &runtimev1.EventChannelResponse{
		Payload: &runtimev1.EventChannelResponse_RunExecution{RunExecution: &runtimev1.RunExecution{
			FlowId: flowID, From: from, NodeId: nodeID, Port: port, DatagramJson: datagramJSON, ReRunOf: reRunOf,
		}},
	})
}

// CancelExecution implements controlplane/internal/api.ExecutionCommander
// (ENG-130).
func (s *Service) CancelExecution(ctx context.Context, executionID string) error {
	return s.sendExecutionCommand(ctx, &runtimev1.EventChannelResponse{
		Payload: &runtimev1.EventChannelResponse_CancelExecution{CancelExecution: &runtimev1.CancelExecution{ExecutionId: executionID}},
	})
}

// ReinjectDeadLetter implements controlplane/internal/api.ExecutionCommander
// (ERR-130).
func (s *Service) ReinjectDeadLetter(ctx context.Context, flowID, nodeID, port, datagramJSON string) error {
	return s.sendExecutionCommand(ctx, &runtimev1.EventChannelResponse{
		Payload: &runtimev1.EventChannelResponse_ReinjectDeadLetter{ReinjectDeadLetter: &runtimev1.ReinjectDeadLetter{
			FlowId: flowID, NodeId: nodeID, Port: port, DatagramJson: datagramJSON,
		}},
	})
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
