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
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/ctxstore"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/storeforward"
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

	// connResolver resolves a node's configured connection id (Increment 6,
	// CON-110); defaults to NoopConnectionResolver so nodes that need one
	// fail with a clear error rather than a nil-pointer panic.
	connResolver ConnectionResolver

	// ctxStore backs PROC-410 (node/flow/global state) and the "flow"/
	// "global" bindings in engine/expr; defaults to an in-process
	// MemoryStore so a runtime with no durable backend configured still
	// works (state just doesn't survive a runtime restart).
	ctxStore ctxstore.Store

	// Triggered-execution tracking (Increment 8, ENG-130/DBG-140). execSink
	// is the attached reporter (default Noop); execTracker enforces
	// concurrency/timeout and is (re)built from the deployed flow's
	// settings the first time Deploy runs, or whenever no execution is
	// currently in flight — reconfiguring a live tracker mid-execution
	// would risk losing in-progress accounting, so a later settings change
	// while executions are active is applied on the next idle deploy
	// rather than immediately (documented limitation).
	execSink       ExecutionSink
	execTracker    *Tracker
	deadLetterSink DeadLetterSink
	flowSettings   Settings
	// defaultErrorFlow is the owning project's ERR-120 fallback error
	// handler flow id, used when the deployed flow has no settings.errorFlow
	// of its own. Set by the control plane at deploy time (Deployment has
	// no notion of "project" itself).
	defaultErrorFlow string

	// dataDir is where onError:"storeForward" (EDGE-130) durable per-node
	// queues live on disk, e.g. "<dataDir>/storeforward/<flowID>/<nodeID>".
	// "" (the default) disables durable queuing entirely — a node
	// configured with storeForward then degrades to "fail" instead of
	// silently dropping data (see nodeRunner.storeForward).
	dataDir string

	// resolvedEnv is VCS-140's environment-profile resolution result: the
	// flow's declared env vars (Flow-File-Format's env[]), each resolved
	// against whichever profile the control plane selected at deploy time
	// (profile value, falling back to the declaration's own default) —
	// computed and validated (missing-variable check) control-plane side,
	// since Deployment has no notion of "project" or "profile" itself, same
	// reasoning as defaultErrorFlow above. Exposed to expressions (MAP-130)
	// as the "env" global, merged over (taking precedence over) the
	// process's own OS environment variables.
	resolvedEnv map[string]string
}

// NewDeployment creates an empty deployment ready for Deploy. A nil logger uses
// slog.Default().
func NewDeployment(logger *slog.Logger) *Deployment {
	if logger == nil {
		logger = slog.Default()
	}
	d := &Deployment{
		logger:         logger,
		nodes:          map[string]*runningNode{},
		inboxes:        map[nodePort]*bus.Wire{},
		inboxFP:        map[nodePort]string{},
		outputTargets:  map[nodePort][]nodePort{},
		ringBuffers:    map[string]*ringBuffer{},
		limiters:       map[string]*rateLimiter{},
		debugSink:      NoopDebugSink,
		metricsStop:    make(chan struct{}),
		connResolver:   NoopConnectionResolver,
		ctxStore:       ctxstore.NewMemoryStore(),
		execSink:       NoopExecutionSink,
		execTracker:    NewTracker(0, false, 0, NoopExecutionSink),
		deadLetterSink: NoopDeadLetterSink,
	}
	go d.pollWireMetrics()
	return d
}

// SetContextStore attaches the node/flow/global state backend (PROC-410).
// Safe to call at any time, including while nodes are running; a nil store
// resets to a fresh in-process MemoryStore.
func (g *Deployment) SetContextStore(store ctxstore.Store) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if store == nil {
		store = ctxstore.NewMemoryStore()
	}
	g.ctxStore = store
}

// SetConnectionResolver attaches (or detaches, with nil) the resolver used
// to look up a node's configured connection (Increment 6, CON-110). Safe to
// call at any time, including while nodes are running.
func (g *Deployment) SetConnectionResolver(resolver ConnectionResolver) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if resolver == nil {
		resolver = NoopConnectionResolver
	}
	g.connResolver = resolver
}

// FlowID returns the currently deployed flow's id, or "" if none has been
// deployed yet. Exposed for fleet health reporting (Increment 9, EDGE-120
// "flow status") — today's engine reconciles one flow per Deployment (see
// TODO.md), so this is always at most a single flow.
func (g *Deployment) FlowID() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.flowID
}

// SetDataDir sets the base directory for onError:"storeForward" durable
// queues (Increment 9, EDGE-130). Must be called before Deploy for it to
// take effect on that deploy's nodes; "" disables durable queuing.
func (g *Deployment) SetDataDir(dir string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.dataDir = dir
}

// SetExecutionSink attaches (or detaches, with nil) the triggered-execution
// reporter (Increment 8, ENG-130/DBG-140). Safe to call at any time.
func (g *Deployment) SetExecutionSink(sink ExecutionSink) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if sink == nil {
		sink = NoopExecutionSink
	}
	g.execSink = sink
	g.execTracker.SetSink(sink)
}

// SetDeadLetterSink attaches (or detaches, with nil) the ERR-130 dead-letter
// reporter. Safe to call at any time.
func (g *Deployment) SetDeadLetterSink(sink DeadLetterSink) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if sink == nil {
		sink = NoopDeadLetterSink
	}
	g.deadLetterSink = sink
}

// SetDefaultErrorFlow sets the owning project's ERR-120 fallback
// error-handler flow id, used when a deployed flow has no settings.errorFlow
// of its own. Deployment has no notion of "project" itself, so the control
// plane supplies this at deploy time.
func (g *Deployment) SetDefaultErrorFlow(flowID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.defaultErrorFlow = flowID
}

// SetResolvedEnv sets VCS-140's deploy-time-resolved environment-profile
// variables, made available to expressions as the "env" global. Deployment
// has no notion of "profile" itself; the control plane resolves and
// validates (missing-variable check) before pushing here. A nil map clears
// it back to OS-env-only.
func (g *Deployment) SetResolvedEnv(env map[string]string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.resolvedEnv = env
}

// ErrorFlowTarget returns the flow id (ERR-120's designated error handler)
// unhandled node errors should be published for, or "" if none is
// configured at either the flow or project level.
func (g *Deployment) ErrorFlowTarget() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.flowSettings.ErrorFlow != "" {
		return g.flowSettings.ErrorFlow
	}
	return g.defaultErrorFlow
}

// ErrorFlowTopic is the topics.Broker topic an unhandled node error is
// published to for a given target flow id (ERR-120), and the pattern an
// "error-trigger" node subscribes to consume it: a specific flow id for a
// flow-level override, or "*" for the project-wide default handler
// (translated to the "#" wildcard, matching every flow's errors). The
// "$errors/" prefix is reserved, mirroring MQTT's own "$SYS/" convention,
// so it can never collide with a user-chosen bus-in/bus-out topic name.
func ErrorFlowTopic(flowID string) string {
	if flowID == "*" {
		return "$errors/#"
	}
	return "$errors/" + flowID
}

// reconfigureTrackerLocked applies settings to g.execTracker IN PLACE
// (Tracker.Reconfigure), never replacing the object itself: every already-
// running node's runner captured a pointer to this Tracker at startNode
// time and keeps it for its whole lifetime, so a node whose own
// fingerprint didn't change (and so isn't restarted by this Deploy call)
// would silently report to an orphaned tracker forever if g.execTracker
// were ever swapped out from under it. Called with g.mu held. Skipped
// (keeping the existing settings as-is) when an execution is currently in
// flight, since resizing the concurrency semaphore mid-execution would
// lose that execution's accounting — settings changed by a redeploy while
// executions are running take effect on the next idle deploy instead
// (documented limitation).
func (g *Deployment) reconfigureTrackerLocked(s Settings) {
	if !g.execTracker.Idle() {
		return
	}
	maxConcurrency := 0
	if s.MaxConcurrency != nil {
		maxConcurrency = *s.MaxConcurrency
	}
	timeout := time.Duration(0)
	if s.ExecutionTimeoutMs != nil {
		timeout = time.Duration(*s.ExecutionTimeoutMs) * time.Millisecond
	}
	g.execTracker.Reconfigure(maxConcurrency, s.ConcurrencyPolicy == "reject", timeout)
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

// WireStats is one wire's queue-depth/delivered/dropped snapshot
// (OBS-100's per-node "queue depth" and throughput, at the granularity the
// engine actually tracks it: per wire, i.e. per (fromNode,fromPort) ->
// (toNode,toPort) edge).
type WireStats struct {
	FromNode string
	FromPort string
	ToNode   string
	ToPort   string
	Depth    int
	Capacity int
	Metrics  bus.Metrics
}

// DeploymentMetrics is everything OBS-100 needs about one running
// deployment: every node's counters/histogram and every wire's queue
// depth/throughput, snapshotted together under one lock so a scrape sees a
// consistent-ish view.
type DeploymentMetrics struct {
	FlowID string
	Nodes  map[string]NodeStats
	Wires  []WireStats
}

// MetricsSnapshot returns a full OBS-100 snapshot of this deployment: every
// currently-running node's counters and every wire's queue depth/
// delivered/dropped counters, for a Prometheus-format exporter
// (engine/internal/obsmetrics) to format.
func (g *Deployment) MetricsSnapshot() DeploymentMetrics {
	g.mu.Lock()
	defer g.mu.Unlock()

	nodes := make(map[string]NodeStats, len(g.nodes))
	for id, rn := range g.nodes {
		nodes[id] = NodeStats{Running: true, StartCount: rn.startCount, Metrics: rn.metrics.Snapshot()}
	}

	wires := make([]WireStats, 0, len(g.wires))
	for _, w := range g.wires {
		busWire, ok := g.inboxes[nodePort{w.To.Node, w.To.Port}]
		if !ok {
			continue
		}
		wires = append(wires, WireStats{
			FromNode: w.From.Node, FromPort: w.From.Port,
			ToNode: w.To.Node, ToPort: w.To.Port,
			Depth: busWire.Depth(), Capacity: busWire.Capacity(), Metrics: busWire.Metrics(),
		})
	}

	return DeploymentMetrics{FlowID: g.flowID, Nodes: nodes, Wires: wires}
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
	g.flowSettings = f.Settings
	g.reconfigureTrackerLocked(f.Settings)

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
	baseOutputs := info.Outputs
	if dyn, ok := instance.(DynamicOutputs); ok {
		baseOutputs = dyn.OutputPorts()
	}
	effectiveOutputs := append([]string(nil), baseOutputs...)
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
	nodeCtx = WithConnection(nodeCtx, g.connResolver, n.Connection)
	nodeCtx = WithContextStore(nodeCtx, g.ctxStore)
	nodeCtx = WithResolvedEnv(nodeCtx, g.resolvedEnv)
	metrics := newNodeMetrics()

	// EDGE-130 store-and-forward: only meaningful for a Processor with
	// onError:"storeForward" configured, and only if a data directory is
	// set — g.dataDir/g.dataDir "" degrades to nil (nodeRunner.storeForward
	// falls back to "fail" rather than losing data silently).
	var sfQueue *storeforward.Queue
	if n.ErrorPolicy != nil && n.ErrorPolicy.OnError == "storeForward" && g.dataDir != "" {
		var maxSizeBytes int64
		var maxAge time.Duration
		if sf := n.ErrorPolicy.StoreForward; sf != nil {
			maxSizeBytes = int64(sf.MaxSizeMb) * 1024 * 1024
			maxAge = time.Duration(sf.MaxAgeSec) * time.Second
		}
		dir := filepath.Join(g.dataDir, "storeforward", g.flowID, n.ID)
		q, err := storeforward.Open(dir, maxSizeBytes, maxAge)
		if err != nil {
			cancel()
			return fmt.Errorf("node %q: opening store-forward queue: %w", n.ID, err)
		}
		sfQueue = q
	} else if n.ErrorPolicy != nil && n.ErrorPolicy.OnError == "storeForward" {
		g.logger.Warn("node configured with onError:storeForward but no data directory set; will fall back to fail", "node", n.ID)
	}

	runner := &nodeRunner{
		id:              n.ID,
		flowID:          g.flowID,
		inputPort:       inputPort,
		errorPolicy:     n.ErrorPolicy,
		outputs:         outputs,
		logger:          g.logger,
		metrics:         metrics,
		ring:            ring,
		limiter:         limiter,
		sink:            g.debugSink,
		isTrigger:       info.Kind == KindSource && info.Trigger,
		execTracker:     g.execTracker,
		deadLetterSink:  g.deadLetterSink,
		errorFlowTarget: g.ErrorFlowTarget,
		sfQueue:         sfQueue,
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
		if len(info.Inputs) == 0 {
			cancel()
			return fmt.Errorf("node type %q declares Kind=Processor but no input ports", n.Type)
		}
		var wg sync.WaitGroup
		if mproc, ok := instance.(MultiInputProcessor); ok {
			if sfQueue != nil {
				wg.Add(1)
				go func() {
					defer wg.Done()
					runner.runStoreForwardDrain(nodeCtx, func(ctx context.Context, port string, in datagram.Datagram) ([]PortDatagram, error) {
						return mproc.ProcessPort(ctx, port, in)
					})
				}()
			}
			for _, port := range info.Inputs {
				inbox := g.inboxes[nodePort{n.ID, port}]
				wg.Add(1)
				go func(port string, inbox *bus.Wire) {
					defer wg.Done()
					runner.runMultiProcessor(nodeCtx, mproc, port, inbox)
				}(port, inbox)
			}
		} else {
			proc, ok := instance.(Processor)
			if !ok {
				cancel()
				return fmt.Errorf("node type %q factory did not return a Processor", n.Type)
			}
			if sfQueue != nil {
				wg.Add(1)
				go func() {
					defer wg.Done()
					runner.runStoreForwardDrain(nodeCtx, func(ctx context.Context, _ string, in datagram.Datagram) ([]PortDatagram, error) {
						return proc.Process(ctx, in)
					})
				}()
			}
			inbox := g.inboxes[nodePort{n.ID, info.Inputs[0]}]
			wg.Add(1)
			go func() {
				defer wg.Done()
				runner.runProcessor(nodeCtx, proc, inbox)
			}()
		}
		go func() {
			defer close(done)
			wg.Wait()
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

// ReplayOutput re-injects seed — a previously recorded datagram emitted by
// (nodeID, port) — into that output's current downstream inbox target(s),
// exactly as if the node had just produced it again (DBG-140 "re-run from
// start": the trigger node's own recorded emission is replayed this way).
// Starts a fresh tracked execution (Increment 8) tagged reRunOf. Returns an
// error without starting anything if that output has no current downstream
// wire (e.g. the flow changed since the original run).
func (g *Deployment) ReplayOutput(ctx context.Context, nodeID, port string, seed datagram.Datagram, reRunOf string) (executionID string, err error) {
	g.mu.Lock()
	wires := g.inboxesForOutput(nodeID, port)
	tracker, flowID := g.execTracker, g.flowID
	g.mu.Unlock()

	if len(wires) == 0 {
		return "", fmt.Errorf("flow: node %q port %q has no current downstream wire to replay into", nodeID, port)
	}

	fresh := freshRootFrom(seed)
	executionID, err = tracker.Start(ctx, flowID, nodeID, "rerun", reRunOf, fresh)
	if err != nil {
		return "", err
	}
	fo := bus.NewFanOut(datagram.DefaultBinaryRefThreshold, wires...)
	if err := fo.Send(ctx, fresh); err != nil {
		return executionID, fmt.Errorf("flow: replaying output: %w", err)
	}
	return executionID, nil
}

// ReplayInput re-injects seed — a previously recorded input to (nodeID,
// port) — directly into that node's own inbox, as if it had just arrived
// normally (DBG-140 "re-run from failed node": that node and everything
// downstream of it re-executes; nothing upstream does). Starts a fresh
// tracked execution (Increment 8) tagged reRunOf.
func (g *Deployment) ReplayInput(ctx context.Context, nodeID, port string, seed datagram.Datagram, reRunOf string) (executionID string, err error) {
	g.mu.Lock()
	w, ok := g.inboxes[nodePort{nodeID, port}]
	tracker, flowID := g.execTracker, g.flowID
	g.mu.Unlock()

	if !ok {
		return "", fmt.Errorf("flow: node %q has no current inbox on port %q to replay into", nodeID, port)
	}

	fresh := freshRootFrom(seed)
	executionID, err = tracker.Start(ctx, flowID, nodeID, "rerun", reRunOf, fresh)
	if err != nil {
		return "", err
	}
	if _, err := w.Send(ctx, fresh); err != nil {
		return executionID, fmt.Errorf("flow: replaying input: %w", err)
	}
	return executionID, nil
}

// freshRootFrom builds a brand-new correlation chain (datagram.New) carrying
// seed's source/payload/tags/quality, for re-run: the replay is a genuinely
// new execution, not a continuation of the original one's lineage.
func freshRootFrom(seed datagram.Datagram) datagram.Datagram {
	fresh := datagram.New(seed.Header.Source, seed.Payload)
	fresh.Header.Tags = seed.Header.Tags
	fresh.Header.Quality = seed.Header.Quality
	fresh.Header.SchemaRef = seed.Header.SchemaRef
	fresh.Header.ContentType = seed.Header.ContentType
	return fresh
}

// CancelExecution requests cancellation of a currently tracked execution
// (ENG-130). Reports false if executionID isn't currently tracked.
func (g *Deployment) CancelExecution(executionID string) bool {
	g.mu.Lock()
	tracker, flowID := g.execTracker, g.flowID
	g.mu.Unlock()
	return tracker.Cancel(flowID, executionID)
}

// ReinjectDeadLetter (ERR-130) delivers d — a previously dead-lettered
// datagram — back into (nodeID, port)'s own inbox exactly as it was
// captured (unlike ReplayInput, this does NOT start a new tracked
// execution: a dead letter may belong to an ordinary streaming flow, and
// resuming it should look identical to normal delivery, not a fresh
// execution).
func (g *Deployment) ReinjectDeadLetter(ctx context.Context, nodeID, port string, d datagram.Datagram) error {
	g.mu.Lock()
	w, ok := g.inboxes[nodePort{nodeID, port}]
	g.mu.Unlock()
	if !ok {
		return fmt.Errorf("flow: node %q has no current inbox on port %q to re-inject into", nodeID, port)
	}
	if _, err := w.Send(ctx, d); err != nil {
		return fmt.Errorf("flow: re-injecting dead letter: %w", err)
	}
	return nil
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
