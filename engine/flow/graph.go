// Graph instantiation and hot deploy (ENG-140): Deploy wires a FlowFile's
// nodes together over engine/bus and starts them, restarting only the
// nodes whose own definition or incident wiring changed since the last
// deploy — everything else (and the bus.Wire objects between unaffected
// nodes) keeps running untouched.
package flow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
)

// DefaultWireCapacity bounds every inbox queue (ENG-150: "max queue sizes").
// The flow file format does not yet expose a per-node capacity field
// (Flow-File-Format.md §2 only has "overflow"), so this is an engine-level
// default rather than an invented file field.
const DefaultWireCapacity = 1024

// DefaultDrainTimeout bounds how long Deploy waits for a replaced or
// removed node's goroutine to finish its in-flight work (ENG-140: "in-flight
// datagrams of affected nodes are drained ... with timeout default").
const DefaultDrainTimeout = 5 * time.Second

type nodePort struct {
	node string
	port string
}

type runningNode struct {
	cancel      context.CancelFunc
	done        chan struct{}
	metrics     *NodeMetrics
	fingerprint string
	startCount  int
}

// Deployment is a live, running instance of a deployed flow.
type Deployment struct {
	mu      sync.Mutex
	logger  *slog.Logger
	nodes   map[string]*runningNode
	inboxes map[nodePort]*bus.Wire
	inboxFP map[nodePort]string
	// outputTargets maps (nodeID, outputPort) -> the inbox keys it feeds,
	// rebuilt fresh from the wire list on every Deploy.
	outputTargets map[nodePort][]nodePort

	// Live-debugging state (Increment 5, DBG-100/110/120/170). Only one
	// flow runs per Deployment today, so flowID/wires are simple fields
	// rather than a per-flow map.
	flowID      string
	wires       []Wire
	ringBuffers map[string]*ringBuffer
	limiters    map[string]*rateLimiter
	debugSink   DebugSink
	metricsStop chan struct{}
	stopOnce    sync.Once
}

// NewDeployment creates an empty deployment ready for Deploy. A nil logger uses
// slog.Default().
func NewDeployment(logger *slog.Logger) *Deployment {
	if logger == nil {
		logger = slog.Default()
	}
	d := &Deployment{
		logger:        logger,
		nodes:         map[string]*runningNode{},
		inboxes:       map[nodePort]*bus.Wire{},
		inboxFP:       map[nodePort]string{},
		outputTargets: map[nodePort][]nodePort{},
		ringBuffers:   map[string]*ringBuffer{},
		limiters:      map[string]*rateLimiter{},
		debugSink:     NoopDebugSink,
		metricsStop:   make(chan struct{}),
	}
	go d.pollWireMetrics()
	return d
}

// SetDebugSink attaches (or detaches, with nil) the live-debugging sink that
// receives rate-limited node/sidebar events and periodic wire-metrics
// snapshots (DBG-170). Safe to call at any time, including while nodes are
// running.
func (g *Deployment) SetDebugSink(sink DebugSink) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if sink == nil {
		sink = NoopDebugSink
	}
	g.debugSink = sink
}

// NodeDebugSnapshot returns the current ring-buffer contents (oldest first)
// for one node, for a browser opening the inspector to see history that
// predates its subscription (DBG-100: "inspection works without redeploy").
func (g *Deployment) NodeDebugSnapshot(nodeID string) []DebugEvent {
	g.mu.Lock()
	rb, ok := g.ringBuffers[nodeID]
	g.mu.Unlock()
	if !ok {
		return nil
	}
	return rb.snapshot()
}

// FlowDebugSnapshot returns every currently-running node's ring-buffer
// contents for flowID, or nil if flowID isn't the deployment's current
// flow. Used to replay history immediately when the control plane
// subscribes to a flow (DBG-100).
func (g *Deployment) FlowDebugSnapshot(flowID string) []DebugEvent {
	g.mu.Lock()
	if g.flowID != flowID {
		g.mu.Unlock()
		return nil
	}
	buffers := make([]*ringBuffer, 0, len(g.ringBuffers))
	for _, rb := range g.ringBuffers {
		buffers = append(buffers, rb)
	}
	g.mu.Unlock()

	var out []DebugEvent
	for _, rb := range buffers {
		out = append(out, rb.snapshot()...)
	}
	return out
}

// pollWireMetrics periodically reports every wire's cumulative
// delivered/dropped counters (DBG-120's live counters/rates) — a fixed-rate
// snapshot rather than per-datagram, so it stays cheap at any throughput.
func (g *Deployment) pollWireMetrics() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-g.metricsStop:
			return
		case <-ticker.C:
			g.reportWireMetrics()
		}
	}
}

type wireBusPair struct {
	wire Wire
	bus  *bus.Wire
}

func (g *Deployment) reportWireMetrics() {
	g.mu.Lock()
	flowID := g.flowID
	sink := g.debugSink
	pairs := make([]wireBusPair, 0, len(g.wires))
	for _, w := range g.wires {
		if busWire, ok := g.inboxes[nodePort{w.To.Node, w.To.Port}]; ok {
			pairs = append(pairs, wireBusPair{wire: w, bus: busWire})
		}
	}
	g.mu.Unlock()

	for _, p := range pairs {
		m := p.bus.Metrics()
		sink.WireMetrics(WireMetricsSample{
			FlowID:    flowID,
			FromNode:  p.wire.From.Node,
			FromPort:  p.wire.From.Port,
			ToNode:    p.wire.To.Node,
			ToPort:    p.wire.To.Port,
			Delivered: m.Delivered,
			Dropped:   m.Dropped,
		})
	}
}

// NodeStats reports whether a node is currently running and how many times
// it has been (re)started, for hot-deploy observability and tests.
type NodeStats struct {
	Running    bool
	StartCount int
	Metrics    MetricsSnapshot
}

func (g *Deployment) NodeStats(nodeID string) (NodeStats, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	rn, ok := g.nodes[nodeID]
	if !ok {
		return NodeStats{}, false
	}
	return NodeStats{Running: true, StartCount: rn.startCount, Metrics: rn.metrics.Snapshot()}, true
}

// Deploy validates f and reconciles the running graph to match it,
// restarting only affected nodes (ENG-140).
func (g *Deployment) Deploy(ctx context.Context, f *FlowFile) error {
	if err := Validate(f); err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	g.flowID = f.ID
	g.wires = append([]Wire(nil), f.Graph.Wires...)

	nodeByID := make(map[string]*Node, len(f.Graph.Nodes))
	infoByID := make(map[string]NodeTypeInfo, len(f.Graph.Nodes))
	for i := range f.Graph.Nodes {
		n := &f.Graph.Nodes[i]
		nodeByID[n.ID] = n
		info, _, _ := Lookup(n.Type) // existence already checked by Validate
		infoByID[n.ID] = info
	}

	wiresByNode := make(map[string][]Wire)
	for _, w := range f.Graph.Wires {
		wiresByNode[w.From.Node] = append(wiresByNode[w.From.Node], w)
		wiresByNode[w.To.Node] = append(wiresByNode[w.To.Node], w)
	}

	newInboxFP := g.computeInboxFingerprints(f, nodeByID)
	g.reconcileInboxes(nodeByID, infoByID, newInboxFP)

	outputTargets := make(map[nodePort][]nodePort)
	for _, w := range f.Graph.Wires {
		from := nodePort{w.From.Node, w.From.Port}
		to := nodePort{w.To.Node, w.To.Port}
		outputTargets[from] = append(outputTargets[from], to)
	}
	g.outputTargets = outputTargets

	newNodeFP := make(map[string]string, len(f.Graph.Nodes))
	for id, n := range nodeByID {
		newNodeFP[id] = nodeFingerprint(n, wiresByNode[id])
	}

	// Stop nodes that no longer exist or whose fingerprint changed, keeping
	// their start count so a restarted node's counter increments rather than
	// resetting.
	priorStartCount := make(map[string]int)
	var stopWg sync.WaitGroup
	for id, rn := range g.nodes {
		if newNodeFP[id] == rn.fingerprint {
			continue // unaffected: keep running untouched
		}
		priorStartCount[id] = rn.startCount
		stopWg.Add(1)
		go func(rn *runningNode) {
			defer stopWg.Done()
			g.drainStop(rn)
		}(rn)
		delete(g.nodes, id)
	}
	stopWg.Wait()

	// A node's ring buffer/limiter is only dropped once it's actually
	// removed from the flow, not on every hot-deploy restart, so history
	// survives config-only redeploys of the same node (DBG-100).
	for id := range g.ringBuffers {
		if _, stillExists := nodeByID[id]; !stillExists {
			delete(g.ringBuffers, id)
			delete(g.limiters, id)
		}
	}

	// Start every node that isn't already running with the current fingerprint.
	for id, n := range nodeByID {
		if _, ok := g.nodes[id]; ok {
			continue // kept
		}
		if err := g.startNode(ctx, n, infoByID[id], newNodeFP[id], priorStartCount[id]); err != nil {
			return fmt.Errorf("flow: starting node %q: %w", id, err)
		}
	}

	return nil
}

// computeInboxFingerprints derives, for every (node, input port), a hash of
// what feeds it (sorted source endpoints) and its overflow policy — the
// identity that decides whether the underlying bus.Wire is reused.
func (g *Deployment) computeInboxFingerprints(f *FlowFile, nodeByID map[string]*Node) map[nodePort]string {
	sources := make(map[nodePort][]string)
	for _, w := range f.Graph.Wires {
		key := nodePort{w.To.Node, w.To.Port}
		sources[key] = append(sources[key], w.From.Node+":"+w.From.Port)
	}

	result := make(map[nodePort]string)
	for i := range f.Graph.Nodes {
		n := &f.Graph.Nodes[i]
		info, _, _ := Lookup(n.Type)
		for _, port := range info.Inputs {
			key := nodePort{n.ID, port}
			srcs := append([]string(nil), sources[key]...)
			sort.Strings(srcs)
			h := sha256.New()
			h.Write([]byte(n.Overflow))
			h.Write([]byte(strings.Join(srcs, ",")))
			result[key] = hex.EncodeToString(h.Sum(nil))
		}
	}
	return result
}

// reconcileInboxes creates fresh bus.Wire objects only where the inbox
// fingerprint changed (or the inbox is new); unaffected wires are reused as
// the same object so nodes that keep running never see a dangling target.
func (g *Deployment) reconcileInboxes(nodeByID map[string]*Node, infoByID map[string]NodeTypeInfo, newFP map[nodePort]string) {
	for key := range g.inboxes {
		if _, stillExists := newFP[key]; !stillExists {
			g.inboxes[key].Close()
			delete(g.inboxes, key)
			delete(g.inboxFP, key)
		}
	}
	for key, fp := range newFP {
		if existingFP, ok := g.inboxFP[key]; ok && existingFP == fp {
			continue // reuse
		}
		if old, ok := g.inboxes[key]; ok {
			old.Close()
		}
		overflow, sampleEvery := parseOverflow(nodeByID[key.node].Overflow)
		g.inboxes[key] = bus.NewWire(bus.WireConfig{Capacity: DefaultWireCapacity, Overflow: overflow, SampleEvery: sampleEvery})
		g.inboxFP[key] = fp
	}
}

// nodeFingerprint captures everything that would require restarting a node:
// its own definition plus every wire touching it (by id/endpoints, not
// object identity — object identity is handled separately per inbox).
func nodeFingerprint(n *Node, touchingWires []Wire) string {
	sorted := append([]Wire(nil), touchingWires...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	h := sha256.New()
	_ = json.NewEncoder(h).Encode(struct {
		Type        string
		TypeVersion int
		Config      json.RawMessage
		ErrorPolicy *ErrorPolicy
		Overflow    string
		Connection  string
		Disabled    bool
		Wires       []Wire
	}{n.Type, n.TypeVersion, n.Config, n.ErrorPolicy, n.Overflow, n.Connection, n.Disabled, sorted})
	return hex.EncodeToString(h.Sum(nil))
}

// startNode builds the node's output fan-outs from the current (possibly
// reused, possibly fresh) inbox wires, instantiates it, and starts its
// runner goroutine.
func (g *Deployment) startNode(ctx context.Context, n *Node, info NodeTypeInfo, fingerprint string, priorStartCount int) error {
	_, factory, ok := Lookup(n.Type)
	if !ok {
		return fmt.Errorf("unknown node type %q", n.Type)
	}
	instance, err := factory(n.Config)
	if err != nil {
		return fmt.Errorf("configuring node: %w", err)
	}

	outputs := make(map[string]*bus.FanOut)
	effectiveOutputs := append([]string(nil), info.Outputs...)
	if n.ErrorPolicy != nil && n.ErrorPolicy.OnError == "errorPort" {
		effectiveOutputs = append(effectiveOutputs, "error")
	}
	for _, port := range effectiveOutputs {
		// Resolved against the live inbox map (already reconciled for this
		// Deploy call), so a kept downstream node's reused wire is always
		// the target — no dangling references across a hot swap.
		outputs[port] = bus.NewFanOut(datagram.DefaultBinaryRefThreshold, g.inboxesForOutput(n.ID, port)...)
	}

	ring, ok := g.ringBuffers[n.ID]
	if !ok {
		ring = newRingBuffer(DefaultRingBufferSize)
		g.ringBuffers[n.ID] = ring
	}
	limiter, ok := g.limiters[n.ID]
	if !ok {
		limiter = newRateLimiter(DefaultDebugRateLimit)
		g.limiters[n.ID] = limiter
	}

	inputPort := ""
	if len(info.Inputs) > 0 {
		inputPort = info.Inputs[0]
	}

	nodeCtx, cancel := context.WithCancel(ctx)
	nodeCtx = withDebugContext(nodeCtx, ring, limiter, g.debugSink, g.flowID, n.ID)
	metrics := &NodeMetrics{}
	runner := &nodeRunner{
		id:          n.ID,
		flowID:      g.flowID,
		inputPort:   inputPort,
		errorPolicy: n.ErrorPolicy,
		outputs:     outputs,
		logger:      g.logger,
		metrics:     metrics,
		ring:        ring,
		limiter:     limiter,
		sink:        g.debugSink,
	}

	done := make(chan struct{})

	switch info.Kind {
	case KindSource:
		src, ok := instance.(Source)
		if !ok {
			cancel()
			return fmt.Errorf("node type %q factory did not return a Source", n.Type)
		}
		go func() {
			defer close(done)
			runner.runSource(nodeCtx, src)
		}()
	case KindProcessor:
		proc, ok := instance.(Processor)
		if !ok {
			cancel()
			return fmt.Errorf("node type %q factory did not return a Processor", n.Type)
		}
		if len(info.Inputs) == 0 {
			cancel()
			return fmt.Errorf("node type %q declares Kind=Processor but no input ports", n.Type)
		}
		inbox := g.inboxes[nodePort{n.ID, info.Inputs[0]}]
		go func() {
			defer close(done)
			runner.runProcessor(nodeCtx, proc, inbox)
		}()
	}

	g.nodes[n.ID] = &runningNode{cancel: cancel, done: done, metrics: metrics, fingerprint: fingerprint, startCount: priorStartCount + 1}
	return nil
}

// inboxesForOutput resolves (nodeID, port)'s wired-to targets against the
// current inbox map (already reconciled for this Deploy call in
// reconcileInboxes, so reused wires are returned as the same object).
func (g *Deployment) inboxesForOutput(nodeID, port string) []*bus.Wire {
	targets := g.outputTargets[nodePort{nodeID, port}]
	wires := make([]*bus.Wire, 0, len(targets))
	for _, key := range targets {
		if w, ok := g.inboxes[key]; ok {
			wires = append(wires, w)
		}
	}
	return wires
}

func parseOverflow(spec string) (bus.OverflowPolicy, int) {
	if strings.HasPrefix(spec, "sample:") {
		n, err := strconv.Atoi(strings.TrimPrefix(spec, "sample:"))
		if err != nil || n < 1 {
			n = 1
		}
		return bus.OverflowSample, n
	}
	switch spec {
	case "dropOldest":
		return bus.OverflowDropOldest, 0
	case "dropNewest":
		return bus.OverflowDropNewest, 0
	default: // "block" or unset
		return bus.OverflowBlock, 0
	}
}

// drainStop cancels a node's context and waits up to DefaultDrainTimeout for
// its goroutine to exit (ENG-140: "drained ... with timeout default").
func (g *Deployment) drainStop(rn *runningNode) {
	rn.cancel()
	select {
	case <-rn.done:
	case <-time.After(DefaultDrainTimeout):
		g.logger.Warn("node did not drain within timeout")
	}
}

// Stop tears down every running node.
func (g *Deployment) Stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	var wg sync.WaitGroup
	for _, rn := range g.nodes {
		wg.Add(1)
		go func(rn *runningNode) {
			defer wg.Done()
			g.drainStop(rn)
		}(rn)
	}
	wg.Wait()
	for _, w := range g.inboxes {
		w.Close()
	}
	g.nodes = map[string]*runningNode{}
	g.inboxes = map[nodePort]*bus.Wire{}
	g.inboxFP = map[nodePort]string{}
	g.stopOnce.Do(func() { close(g.metricsStop) })
}
