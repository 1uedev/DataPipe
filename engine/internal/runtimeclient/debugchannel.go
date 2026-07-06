// Live-debugging transport (Increment 5, DBG-100/110/120/170): forwards the
// engine's rate-limited debug events and wire-metrics snapshots to the
// control plane over the DebugChannel bidi stream, gated by subscribe/
// unsubscribe messages the control plane pushes down the same stream so the
// runtime never captures or sends data for a flow nobody is watching.
package runtimeclient

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"

	"github.com/1uedev/DataPipe/engine/flow"
)

const debugChannelRetryDelay = 2 * time.Second

// RingBufferSource lets the debug channel loop replay a flow's current
// per-node history immediately on subscribe (DBG-100: "inspection works
// without redeploy"), without the runtimeclient package depending on the
// concrete *flow.Deployment type beyond this one method.
type RingBufferSource interface {
	FlowDebugSnapshot(flowID string) []flow.DebugEvent
}

// DebugSink implements flow.DebugSink by forwarding to an open DebugChannel
// stream, but only for flows the control plane has subscribed to. It is
// safe to attach to a flow.Deployment before any connection exists: Capture
// and WireMetrics are cheap no-ops until subscribed.
type DebugSink struct {
	mu         sync.Mutex
	subscribed map[string]bool
	send       func(*runtimev1.DebugChannelRequest) error
	runtimeID  string
	token      string
}

// NewDebugSink creates a sink with no active subscriptions and no attached
// stream yet.
func NewDebugSink() *DebugSink {
	return &DebugSink{subscribed: map[string]bool{}}
}

func (s *DebugSink) attach(send func(*runtimev1.DebugChannelRequest) error, runtimeID, token string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.send = send
	s.runtimeID = runtimeID
	s.token = token
}

func (s *DebugSink) detach() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.send = nil
}

func (s *DebugSink) setSubscribed(flowID string, on bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if on {
		s.subscribed[flowID] = true
	} else {
		delete(s.subscribed, flowID)
	}
}

func (s *DebugSink) trySend(flowID string, build func() *runtimev1.DebugChannelRequest) {
	s.mu.Lock()
	send := s.send
	subscribed := s.subscribed[flowID]
	runtimeID, token := s.runtimeID, s.token
	s.mu.Unlock()

	if send == nil || !subscribed {
		return
	}
	req := build()
	req.RuntimeId = runtimeID
	req.SessionToken = token
	if err := send(req); err != nil {
		slog.Debug("debug channel send failed", "error", err)
	}
}

// Capture implements flow.DebugSink.
func (s *DebugSink) Capture(e flow.DebugEvent) {
	s.trySend(e.FlowID, func() *runtimev1.DebugChannelRequest {
		return &runtimev1.DebugChannelRequest{
			Payload: &runtimev1.DebugChannelRequest_Event{Event: toProtoDebugEvent(e)},
		}
	})
}

// WireMetrics implements flow.DebugSink.
func (s *DebugSink) WireMetrics(m flow.WireMetricsSample) {
	s.trySend(m.FlowID, func() *runtimev1.DebugChannelRequest {
		return &runtimev1.DebugChannelRequest{
			Payload: &runtimev1.DebugChannelRequest_WireMetrics{WireMetrics: &runtimev1.WireMetricsSnapshot{
				FlowId:    m.FlowID,
				FromNode:  m.FromNode,
				FromPort:  m.FromPort,
				ToNode:    m.ToNode,
				ToPort:    m.ToPort,
				Delivered: m.Delivered,
				Dropped:   m.Dropped,
			}},
		}
	})
}

func toProtoDebugEvent(e flow.DebugEvent) *runtimev1.DebugEvent {
	valueJSON, err := json.Marshal(e.Value)
	if err != nil {
		valueJSON = []byte("null")
	}
	return &runtimev1.DebugEvent{
		Id:            e.ID,
		FlowId:        e.FlowID,
		NodeId:        e.NodeID,
		Port:          e.Port,
		Direction:     string(e.Direction),
		Label:         e.Label,
		TimeUnixMs:    e.Time.UnixMilli(),
		DatagramId:    e.DatagramID,
		CorrelationId: e.CorrelationID,
		CausationId:   e.CausationID,
		Quality:       e.Quality,
		ValueJson:     string(valueJSON),
	}
}

// debugChannelLoop keeps a DebugChannel open, reconnecting on failure, until
// ctx is cancelled. sink is attached/detached as the stream comes and goes;
// rb supplies the ring-buffer replay on subscribe.
func debugChannelLoop(ctx context.Context, client runtimev1.RuntimeRegistryServiceClient, runtimeID, sessionToken string, sink *DebugSink, rb RingBufferSource) {
	for ctx.Err() == nil {
		stream, err := client.DebugChannel(ctx)
		if err != nil {
			slog.Warn("opening debug channel failed, retrying", "error", err)
			if !sleepOrDone(ctx, debugChannelRetryDelay) {
				return
			}
			continue
		}

		// Announce presence immediately: the hub only learns a runtime is
		// attached from its first received message (Hub.Serve), and a
		// browser might subscribe to a flow before this runtime has any
		// other reason to send an uplink (nothing is forwarded pre-
		// subscription, DBG-170) — without this, the hub would have no
		// attached runtime to send that first Subscribe downlink to.
		if err := stream.Send(&runtimev1.DebugChannelRequest{RuntimeId: runtimeID, SessionToken: sessionToken}); err != nil {
			slog.Warn("debug channel hello failed, retrying", "error", err)
			if !sleepOrDone(ctx, debugChannelRetryDelay) {
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
						slog.Warn("debug channel ended, reconnecting", "error", err)
					}
					return
				}
				handleDebugDownlink(sink, rb, resp)
			}
		}()

		select {
		case <-ctx.Done():
		case <-done:
		}
		sink.detach()

		if !sleepOrDone(ctx, debugChannelRetryDelay) {
			return
		}
	}
}

func handleDebugDownlink(sink *DebugSink, rb RingBufferSource, resp *runtimev1.DebugChannelResponse) {
	switch payload := resp.GetPayload().(type) {
	case *runtimev1.DebugChannelResponse_Subscribe:
		flowID := payload.Subscribe.GetFlowId()
		sink.setSubscribed(flowID, true)
		if rb == nil {
			return
		}
		for _, e := range rb.FlowDebugSnapshot(flowID) {
			sink.Capture(e) // subscribed is now true, so this actually sends
		}
	case *runtimev1.DebugChannelResponse_Unsubscribe:
		sink.setSubscribed(payload.Unsubscribe.GetFlowId(), false)
	}
}
