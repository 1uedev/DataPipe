package flow

import (
	"context"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// debugCtx bundles the same ring buffer/limiter/sink a node's own runner
// uses, so a node type can push its own named sidebar events (DBG-110)
// through the identical ring-buffer-then-rate-limited-forward path as the
// generic node-boundary capture (DBG-170 applies equally to both).
type debugCtx struct {
	ring    *ringBuffer
	limiter *rateLimiter
	sink    DebugSink
	flowID  string
	nodeID  string
}

type debugCtxKey struct{}

// withDebugContext attaches a node's debug plumbing to ctx so its Process
// method can call SidebarEvent.
func withDebugContext(ctx context.Context, ring *ringBuffer, limiter *rateLimiter, sink DebugSink, flowID, nodeID string) context.Context {
	return context.WithValue(ctx, debugCtxKey{}, debugCtx{ring: ring, limiter: limiter, sink: sink, flowID: flowID, nodeID: nodeID})
}

// CurrentIDs returns the flow/node id a node is running under, for node
// types that need to key their own state (e.g. PROC-410 ctxstore scoping,
// engine/expr's flow/global bindings). Empty strings if ctx carries no
// debug context (e.g. a node unit test calling Process directly without a
// live Deployment).
func CurrentIDs(ctx context.Context) (flowID, nodeID string) {
	dc, ok := ctx.Value(debugCtxKey{}).(debugCtx)
	if !ok {
		return "", ""
	}
	return dc.flowID, dc.nodeID
}

// SidebarEvent pushes a named debug/sidebar event (DBG-110's "explicit node
// printing selected expressions to a global debug sidebar"). source is the
// datagram the value was derived from (its id/correlation/quality are
// carried along); value is whatever expression the node type chose to
// capture. A no-op if ctx carries no debug context (e.g. a node unit test
// calling Process directly without a live Deployment).
func SidebarEvent(ctx context.Context, label string, source datagram.Datagram, value any) {
	dc, ok := ctx.Value(debugCtxKey{}).(debugCtx)
	if !ok || dc.ring == nil {
		return
	}
	e := DebugEvent{
		ID:            newDebugEventID(),
		FlowID:        dc.flowID,
		NodeID:        dc.nodeID,
		Direction:     DirSidebar,
		Label:         label,
		Time:          time.Now().UTC(),
		DatagramID:    source.Header.ID,
		CorrelationID: source.Header.CorrelationID,
		CausationID:   source.Header.CausationID,
		Quality:       string(source.Header.Quality),
		Value:         value,
	}
	dc.ring.push(e)
	if dc.limiter.allow() {
		dc.sink.Capture(e)
	}
}
