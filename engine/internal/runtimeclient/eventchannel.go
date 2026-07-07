// Triggered-execution and dead-letter transport (Increment 8, ENG-130/
// DBG-140/ERR-130): forwards the engine's execution-lifecycle and
// dead-letter events to the control plane over the EventChannel bidi
// stream, and applies re-run/cancel/re-inject commands the control plane
// pushes down the same stream. Unlike DebugChannel this is never gated by
// subscribe/unsubscribe — the control plane always wants every event.
package runtimeclient

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const eventChannelRetryDelay = 2 * time.Second

// DeploymentTarget is the subset of *flow.Deployment the EventChannel
// downlink handler needs, kept as an interface so this package doesn't
// depend on the concrete type beyond these methods.
type DeploymentTarget interface {
	ReplayOutput(ctx context.Context, nodeID, port string, seed datagram.Datagram, reRunOf string) (string, error)
	ReplayInput(ctx context.Context, nodeID, port string, seed datagram.Datagram, reRunOf string) (string, error)
	ReinjectDeadLetter(ctx context.Context, nodeID, port string, d datagram.Datagram) error
	CancelExecution(executionID string) bool
}

// EventSink implements flow.ExecutionSink and flow.DeadLetterSink by
// forwarding every call to an open EventChannel stream. Safe to attach to a
// flow.Deployment before any connection exists: calls are cheap no-ops
// until attached.
type EventSink struct {
	mu        sync.Mutex
	send      func(*runtimev1.EventChannelRequest) error
	runtimeID string
	token     string
}

// NewEventSink creates a sink with no attached stream yet.
func NewEventSink() *EventSink { return &EventSink{} }

func (s *EventSink) attach(send func(*runtimev1.EventChannelRequest) error, runtimeID, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.send = send
	s.runtimeID = runtimeID
	s.token = token
}

func (s *EventSink) detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.send = nil
}

func (s *EventSink) trySend(build func() *runtimev1.EventChannelRequest) {
	s.mu.Lock()
	send := s.send
	runtimeID, token := s.runtimeID, s.token
	s.mu.Unlock()

	if send == nil {
		return
	}
	req := build()
	req.RuntimeId = runtimeID
	req.SessionToken = token
	if err := send(req); err != nil {
		slog.Debug("event channel send failed", "error", err)
	}
}

// Waiting implements flow.ExecutionSink.
func (s *EventSink) Waiting(flowID, executionID, triggerNodeID string, at time.Time, seed datagram.Datagram) {
	s.trySend(func() *runtimev1.EventChannelRequest {
		return execEventRequest(&runtimev1.ExecutionEvent{
			ExecutionId:      executionID,
			FlowId:           flowID,
			Phase:            "waiting",
			TimeUnixMs:       at.UnixMilli(),
			TriggerNodeId:    triggerNodeID,
			SeedDatagramJson: marshalDatagram(seed),
		})
	})
}

// Started implements flow.ExecutionSink.
func (s *EventSink) Started(flowID, executionID, triggerNodeID, triggerKind, reRunOf string, at time.Time, seed datagram.Datagram) {
	s.trySend(func() *runtimev1.EventChannelRequest {
		return execEventRequest(&runtimev1.ExecutionEvent{
			ExecutionId:      executionID,
			FlowId:           flowID,
			Phase:            "started",
			TimeUnixMs:       at.UnixMilli(),
			TriggerNodeId:    triggerNodeID,
			TriggerKind:      triggerKind,
			ReRunOf:          reRunOf,
			SeedDatagramJson: marshalDatagram(seed),
		})
	})
}

// NodeEvent implements flow.ExecutionSink.
func (s *EventSink) NodeEvent(flowID, executionID string, ev flow.NodeIO) {
	s.trySend(func() *runtimev1.EventChannelRequest {
		e := &runtimev1.ExecutionEvent{
			ExecutionId: executionID,
			FlowId:      flowID,
			Phase:       "node",
			TimeUnixMs:  ev.At.UnixMilli(),
			NodeId:      ev.NodeID,
			Port:        ev.Port,
			Attempt:     int32(ev.Attempt),
			DurationUs:  ev.DurationUs,
			InputJson:   marshalDatagram(ev.Input),
			OutputsJson: marshalOutputs(ev.Outputs),
		}
		if ev.Err != nil {
			e.ErrorMessage = ev.Err.Message
			e.ErrorCode = ev.Err.Code
			e.ErrorStack = ev.Err.Stack
		}
		return execEventRequest(e)
	})
}

// Finished implements flow.ExecutionSink.
func (s *EventSink) Finished(flowID, executionID string, status flow.ExecutionStatus, at time.Time, reason string) {
	s.trySend(func() *runtimev1.EventChannelRequest {
		return execEventRequest(&runtimev1.ExecutionEvent{
			ExecutionId: executionID,
			FlowId:      flowID,
			Phase:       "finished",
			TimeUnixMs:  at.UnixMilli(),
			Status:      string(status),
			Reason:      reason,
		})
	})
}

// Capture implements flow.DeadLetterSink. id is left for the control plane
// to assign (it owns the durable store); the runtime never invents one.
func (s *EventSink) Capture(flowID, nodeID, port, reason string, d datagram.Datagram, at time.Time) {
	s.trySend(func() *runtimev1.EventChannelRequest {
		return &runtimev1.EventChannelRequest{
			Payload: &runtimev1.EventChannelRequest_DeadLetterEvent{DeadLetterEvent: &runtimev1.DeadLetterEvent{
				FlowId:       flowID,
				NodeId:       nodeID,
				Port:         port,
				Reason:       reason,
				DatagramJson: marshalDatagram(d),
				TimeUnixMs:   at.UnixMilli(),
			}},
		}
	})
}

func execEventRequest(e *runtimev1.ExecutionEvent) *runtimev1.EventChannelRequest {
	return &runtimev1.EventChannelRequest{Payload: &runtimev1.EventChannelRequest_ExecutionEvent{ExecutionEvent: e}}
}

func marshalDatagram(d datagram.Datagram) string {
	b, err := json.Marshal(d)
	if err != nil {
		return "null"
	}
	return string(b)
}

func marshalOutputs(outputs []flow.PortDatagram) string {
	b, err := json.Marshal(outputs)
	if err != nil {
		return "null"
	}
	return string(b)
}

// eventChannelLoop keeps an EventChannel open, reconnecting on failure,
// until ctx is cancelled. sink is attached/detached as the stream comes and
// goes; target applies downlink commands (nil means "no live Deployment
// to apply them to", e.g. in tests).
func eventChannelLoop(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, sessionToken string, sink *EventSink, target DeploymentTarget) {
	for ctx.Err() == nil {
		stream, err := client.EventChannel(ctx)
		if err != nil {
			slog.Warn("opening event channel failed, retrying", "error", err)
			if !sleepOrDone(ctx, eventChannelRetryDelay) {
				return
			}
			continue
		}

		if err := stream.Send(&runtimev1.EventChannelRequest{RuntimeId: runtimeID, SessionToken: sessionToken}); err != nil {
			slog.Warn("event channel hello failed, retrying", "error", err)
			if !sleepOrDone(ctx, eventChannelRetryDelay) {
				return
			}
			continue
		}

		sink.attach(stream.Send, runtimeID, sessionToken)
		done := make(chan struct{})
		go func() {
			defer close(done)
			for {
				resp, err := stream.Recv()
				if err != nil {
					if ctx.Err() == nil {
						slog.Warn("event channel ended, reconnecting", "error", err)
					}
					return
				}
				handleEventDownlink(ctx, target, resp)
			}
		}()

		select {
		case <-ctx.Done():
		case <-done:
		}
		sink.detach()

		if !sleepOrDone(ctx, eventChannelRetryDelay) {
			return
		}
	}
}

func handleEventDownlink(ctx context.Context, target DeploymentTarget, resp *runtimev1.EventChannelResponse) {
	if target == nil {
		return
	}
	switch payload := resp.GetPayload().(type) {
	case *runtimev1.EventChannelResponse_RunExecution:
		re := payload.RunExecution
		var seed datagram.Datagram
		if err := json.Unmarshal([]byte(re.GetDatagramJson()), &seed); err != nil {
			slog.Warn("run-execution command: invalid datagram JSON", "error", err)
			return
		}
		var runErr error
		switch re.GetFrom() {
		case "start":
			_, runErr = target.ReplayOutput(ctx, re.GetNodeId(), re.GetPort(), seed, re.GetReRunOf())
		case "node":
			_, runErr = target.ReplayInput(ctx, re.GetNodeId(), re.GetPort(), seed, re.GetReRunOf())
		default:
			slog.Warn("run-execution command: unknown from value", "from", re.GetFrom())
			return
		}
		if runErr != nil {
			slog.Warn("run-execution command failed", "error", runErr)
		}
	case *runtimev1.EventChannelResponse_CancelExecution:
		target.CancelExecution(payload.CancelExecution.GetExecutionId())
	case *runtimev1.EventChannelResponse_ReinjectDeadLetter:
		rd := payload.ReinjectDeadLetter
		var d datagram.Datagram
		if err := json.Unmarshal([]byte(rd.GetDatagramJson()), &d); err != nil {
			slog.Warn("reinject-dead-letter command: invalid datagram JSON", "error", err)
			return
		}
		if err := target.ReinjectDeadLetter(ctx, rd.GetNodeId(), rd.GetPort(), d); err != nil {
			slog.Warn("reinject-dead-letter command failed", "error", err)
		}
	}
}
