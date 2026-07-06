// Package debughub is the control-plane side of Increment 5's live
// debugging channel (DBG-100/110/120/170): it terminates the runtime's
// DebugChannel gRPC stream, ref-counts per-flow browser subscriptions
// (sending Subscribe/Unsubscribe downlinks only when the first subscriber
// joins / the last one leaves, so a runtime never bothers capturing or
// sending data for a flow nobody is watching), keeps a small bounded cache
// per flow for late-joining subscribers and "load full payload" lookups,
// and truncates large payloads before they reach a browser.
package debughub

import (
	"fmt"
	"sync"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"
)

// MaxInlinePayloadBytes bounds how much of an event's value JSON is sent
// inline to a browser (DBG-110: "payload size truncation with 'load full'
// on demand"); the full value stays in the hub's cache regardless.
const MaxInlinePayloadBytes = 4096

// CacheSize is how many recent events per flow the hub keeps for
// late-joining subscribers and "load full" lookups, independent of what
// each engine node's own ring buffer (flow.DefaultRingBufferSize) holds.
const CacheSize = 200

// SubscriberBuffer bounds each subscriber's channel; the debug view is a
// lossy, best-effort observability stream by design (DBG-170), so a slow
// consumer drops its own oldest unread items rather than blocking the hub.
const SubscriberBuffer = 256

// Event is one relayed, browser-facing debug event; ValueJSON may be
// truncated (see Truncated/FullLength).
type Event struct {
	ID            string
	FlowID        string
	NodeID        string
	Port          string
	Direction     string
	Label         string
	TimeUnixMs    int64
	DatagramID    string
	CorrelationID string
	CausationID   string
	Quality       string
	ValueJSON     string
	Truncated     bool
	FullLength    int
}

// WireMetric is one relayed wire-metrics snapshot (DBG-120).
type WireMetric struct {
	FlowID    string
	FromNode  string
	FromPort  string
	ToNode    string
	ToPort    string
	Delivered uint64
	Dropped   uint64
}

// Item is one item delivered to a subscriber: exactly one of Event/Metric is
// set.
type Item struct {
	Event  *Event
	Metric *WireMetric
}

type runtimeHandle struct {
	send func(*runtimev1.DebugChannelResponse) error
}

type flowState struct {
	refCount    int
	subscribers map[chan Item]struct{}
	cache       []Event           // bounded ring, oldest first
	cacheIDs    map[string]string // event id -> full (untruncated) value json
	cacheOrder  []string          // eviction order matching cache's ring rotation
}

// Hub is the control-plane singleton coordinating every connected runtime's
// DebugChannel stream and every browser subscriber.
type Hub struct {
	mu       sync.Mutex
	runtimes map[string]*runtimeHandle
	flows    map[string]*flowState
	validate func(runtimeID, sessionToken string) bool
}

// New creates a Hub. validate authenticates a (runtimeId, sessionToken)
// pair against the caller's runtime registry (kept out of this package so
// debughub has no dependency on registry's internal state).
func New(validate func(runtimeID, sessionToken string) bool) *Hub {
	return &Hub{
		runtimes: map[string]*runtimeHandle{},
		flows:    map[string]*flowState{},
		validate: validate,
	}
}

// Serve terminates one runtime's DebugChannel stream: reads uplinks
// (events/wire-metrics) until the stream ends, dispatching each to
// subscribers, and lets Subscribe (see below) push Subscribe/Unsubscribe
// downlinks back down this same stream.
func (h *Hub) Serve(stream runtimev1.RuntimeRegistryService_DebugChannelServer) error {
	var runtimeID string
	handle := &runtimeHandle{send: stream.Send}
	defer func() {
		if runtimeID != "" {
			h.detachRuntime(runtimeID, handle)
		}
	}()

	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		if !h.validate(req.GetRuntimeId(), req.GetSessionToken()) {
			return fmt.Errorf("debughub: unknown runtime or session")
		}
		if runtimeID == "" {
			runtimeID = req.GetRuntimeId()
			h.attachRuntime(runtimeID, handle)
		}
		h.handleUplink(req)
	}
}

func (h *Hub) attachRuntime(runtimeID string, handle *runtimeHandle) {
	h.mu.Lock()
	h.runtimes[runtimeID] = handle
	var subscribedFlows []string
	for flowID, fs := range h.flows {
		if fs.refCount > 0 {
			subscribedFlows = append(subscribedFlows, flowID)
		}
	}
	h.mu.Unlock()

	// A runtime that just (re)connected might host a flow that already has
	// active browser subscribers; tell it so without waiting for a new
	// Subscribe call.
	for _, flowID := range subscribedFlows {
		_ = handle.send(&runtimev1.DebugChannelResponse{
			Payload: &runtimev1.DebugChannelResponse_Subscribe{Subscribe: &runtimev1.SubscribeFlow{FlowId: flowID}},
		})
	}
}

func (h *Hub) detachRuntime(runtimeID string, handle *runtimeHandle) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.runtimes[runtimeID]; ok && cur == handle {
		delete(h.runtimes, runtimeID)
	}
}

func (h *Hub) handleUplink(req *runtimev1.DebugChannelRequest) {
	switch p := req.GetPayload().(type) {
	case *runtimev1.DebugChannelRequest_Event:
		h.dispatchEvent(p.Event)
	case *runtimev1.DebugChannelRequest_WireMetrics:
		h.dispatchWireMetrics(p.WireMetrics)
	}
}

func (h *Hub) dispatchEvent(e *runtimev1.DebugEvent) {
	full := e.GetValueJson()
	ev := Event{
		ID:            e.GetId(),
		FlowID:        e.GetFlowId(),
		NodeID:        e.GetNodeId(),
		Port:          e.GetPort(),
		Direction:     e.GetDirection(),
		Label:         e.GetLabel(),
		TimeUnixMs:    e.GetTimeUnixMs(),
		DatagramID:    e.GetDatagramId(),
		CorrelationID: e.GetCorrelationId(),
		CausationID:   e.GetCausationId(),
		Quality:       e.GetQuality(),
		ValueJSON:     full,
	}
	if len(full) > MaxInlinePayloadBytes {
		ev.ValueJSON = full[:MaxInlinePayloadBytes]
		ev.Truncated = true
		ev.FullLength = len(full)
	}

	h.mu.Lock()
	fs := h.flowStateLocked(ev.FlowID)
	fs.pushCache(ev, full)
	subs := make([]chan Item, 0, len(fs.subscribers))
	for ch := range fs.subscribers {
		subs = append(subs, ch)
	}
	h.mu.Unlock()

	item := Item{Event: &ev}
	for _, ch := range subs {
		nonBlockingSend(ch, item)
	}
}

func (h *Hub) dispatchWireMetrics(m *runtimev1.WireMetricsSnapshot) {
	wm := WireMetric{
		FlowID:    m.GetFlowId(),
		FromNode:  m.GetFromNode(),
		FromPort:  m.GetFromPort(),
		ToNode:    m.GetToNode(),
		ToPort:    m.GetToPort(),
		Delivered: m.GetDelivered(),
		Dropped:   m.GetDropped(),
	}

	h.mu.Lock()
	fs := h.flowStateLocked(wm.FlowID)
	subs := make([]chan Item, 0, len(fs.subscribers))
	for ch := range fs.subscribers {
		subs = append(subs, ch)
	}
	h.mu.Unlock()

	item := Item{Metric: &wm}
	for _, ch := range subs {
		nonBlockingSend(ch, item)
	}
}

// flowStateLocked returns (creating if needed) the flowState for flowID.
// Caller must hold h.mu.
func (h *Hub) flowStateLocked(flowID string) *flowState {
	fs, ok := h.flows[flowID]
	if !ok {
		fs = &flowState{subscribers: map[chan Item]struct{}{}, cacheIDs: map[string]string{}}
		h.flows[flowID] = fs
	}
	return fs
}

func (fs *flowState) pushCache(ev Event, fullValueJSON string) {
	if len(fs.cache) >= CacheSize {
		oldest := fs.cacheOrder[0]
		fs.cacheOrder = fs.cacheOrder[1:]
		fs.cache = fs.cache[1:]
		delete(fs.cacheIDs, oldest)
	}
	fs.cache = append(fs.cache, ev)
	fs.cacheOrder = append(fs.cacheOrder, ev.ID)
	fs.cacheIDs[ev.ID] = fullValueJSON
}

// Subscribe registers a new browser-facing subscriber for flowID. The
// returned channel immediately receives a replay of the flow's cached
// history (oldest first); cancel unsubscribes and, once the last subscriber
// for flowID is gone, tells every connected runtime to stop forwarding it.
func (h *Hub) Subscribe(flowID string) (events <-chan Item, cancel func()) {
	h.mu.Lock()
	fs := h.flowStateLocked(flowID)
	ch := make(chan Item, SubscriberBuffer)
	fs.subscribers[ch] = struct{}{}
	fs.refCount++
	firstSubscriber := fs.refCount == 1
	replay := append([]Event(nil), fs.cache...)
	var runtimeHandles []*runtimeHandle
	if firstSubscriber {
		for _, rt := range h.runtimes {
			runtimeHandles = append(runtimeHandles, rt)
		}
	}
	h.mu.Unlock()

	for _, e := range replay {
		e := e
		nonBlockingSend(ch, Item{Event: &e})
	}
	if firstSubscriber {
		for _, rt := range runtimeHandles {
			_ = rt.send(&runtimev1.DebugChannelResponse{
				Payload: &runtimev1.DebugChannelResponse_Subscribe{Subscribe: &runtimev1.SubscribeFlow{FlowId: flowID}},
			})
		}
	}

	cancel = func() { h.unsubscribe(flowID, ch) }
	return ch, cancel
}

func (h *Hub) unsubscribe(flowID string, ch chan Item) {
	h.mu.Lock()
	fs, ok := h.flows[flowID]
	if !ok {
		h.mu.Unlock()
		return
	}
	if _, present := fs.subscribers[ch]; !present {
		h.mu.Unlock()
		return
	}
	delete(fs.subscribers, ch)
	fs.refCount--
	lastSubscriber := fs.refCount == 0
	var runtimeHandles []*runtimeHandle
	if lastSubscriber {
		for _, rt := range h.runtimes {
			runtimeHandles = append(runtimeHandles, rt)
		}
	}
	h.mu.Unlock()

	if lastSubscriber {
		for _, rt := range runtimeHandles {
			_ = rt.send(&runtimev1.DebugChannelResponse{
				Payload: &runtimev1.DebugChannelResponse_Unsubscribe{Unsubscribe: &runtimev1.UnsubscribeFlow{FlowId: flowID}},
			})
		}
	}
}

// LoadFull returns the untruncated value JSON for a previously-seen event
// id in flowID, if it's still in the cache (DBG-110 "load full on demand").
func (h *Hub) LoadFull(flowID, eventID string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	fs, ok := h.flows[flowID]
	if !ok {
		return "", false
	}
	full, ok := fs.cacheIDs[eventID]
	return full, ok
}

func nonBlockingSend(ch chan Item, item Item) {
	select {
	case ch <- item:
	default:
		// Subscriber is behind; drop the oldest queued item to make room
		// rather than blocking the hub (DBG-170: this stream is
		// best-effort/lossy by design).
		select {
		case <-ch:
		default:
		}
		select {
		case ch <- item:
		default:
		}
	}
}
