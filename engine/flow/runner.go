// Node execution: panic recovery at the node boundary (ARC-150: "a
// panicking node must never take down the runtime") and the uniform
// per-node error policy (ERR-100: fail | retry | errorPort | discard).
package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
	"github.com/1uedev/DataPipe/engine/storeforward"
	"github.com/1uedev/DataPipe/engine/topics"
)

// storeForwardDrainIdlePoll is how often a node's store-and-forward
// drainer checks an empty queue (and the starting point for its retry
// backoff on delivery failure — see storeforward.Drain).
const storeForwardDrainIdlePoll = 500 * time.Millisecond

// NodeMetrics are the per-node counters exposed for observability.
type NodeMetrics struct {
	Processed atomic.Uint64
	Errors    atomic.Uint64
	Retries   atomic.Uint64
}

// MetricsSnapshot is a plain-value copy of NodeMetrics safe to return by
// value (NodeMetrics itself is not copyable: it embeds atomics).
type MetricsSnapshot struct {
	Processed uint64
	Errors    uint64
	Retries   uint64
}

func (m *NodeMetrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		Processed: m.Processed.Load(),
		Errors:    m.Errors.Load(),
		Retries:   m.Retries.Load(),
	}
}

// TriggerKindProvider is implemented by a trigger node instance that knows a
// more specific ENG-130 trigger-kind label than the generic default (e.g.
// "webhook" for http-in). Optional.
type TriggerKindProvider interface {
	TriggerKind() string
}

// nodeRunner drives one node instance: a Source in its own goroutine, or a
// Processor's receive-process-send loop over its inbox wire.
type nodeRunner struct {
	id          string
	flowID      string
	inputPort   string
	errorPolicy *ErrorPolicy
	outputs     map[string]*bus.FanOut
	logger      *slog.Logger
	metrics     *NodeMetrics
	ring        *ringBuffer
	limiter     *rateLimiter
	sink        DebugSink

	// Increment 8: isTrigger marks this a trigger-node instance (ENG-100),
	// so runSource starts a tracked execution (ENG-130) for each fresh
	// datagram it emits. execTracker/deadLetterSink are nil for a
	// nodeRunner built directly in a test (as opposed to via
	// Deployment.startNode) — use the tracker()/dlq() accessors below,
	// never these fields directly, so that case degrades to a no-op
	// instead of a nil-pointer panic.
	isTrigger      bool
	execTracker    *Tracker
	deadLetterSink DeadLetterSink

	// errorFlowTarget resolves ERR-120's designated error-handler flow id
	// (flow-level override or project default) at the moment it's needed,
	// since it can change after a redeploy or SetDefaultErrorFlow call
	// without restarting this node. nil (e.g. a directly test-built
	// runner) means "no error flow configured".
	errorFlowTarget func() string

	// sfQueue is this node's EDGE-130 store-and-forward durable queue, set
	// only when errorPolicy.onError == "storeForward" and a data directory
	// is configured (Deployment.dataDir); nil otherwise, in which case a
	// "storeForward" policy degrades to "fail" rather than silently
	// dropping data. startNode runs exactly one drain goroutine for it
	// (see runStoreForwardDrain), regardless of node kind.
	sfQueue *storeforward.Queue
}

// noopTracker is shared by every nodeRunner with no execTracker configured
// (e.g. built directly in a unit test); since such a runner's isTrigger is
// also always false, Start is never called on it and NodeEvent always
// no-ops (untracked correlation id), so sharing one instance is safe.
var noopTracker = NewTracker(0, false, 0, NoopExecutionSink)

func (r *nodeRunner) tracker() *Tracker {
	if r.execTracker == nil {
		return noopTracker
	}
	return r.execTracker
}

func (r *nodeRunner) dlq() DeadLetterSink {
	if r.deadLetterSink == nil {
		return NoopDeadLetterSink
	}
	return r.deadLetterSink
}

// captureDebug records a datagram observed at a node boundary into its ring
// buffer (DBG-100, always) and — if the per-node rate limiter allows it —
// forwards it live to the attached DebugSink (DBG-170). r.ring is nil for
// runners built outside Deployment.startNode (e.g. direct unit tests), in
// which case this is a no-op.
func (r *nodeRunner) captureDebug(dir DebugDirection, port, label string, d datagram.Datagram) {
	if r.ring == nil {
		return
	}
	e := newDebugEvent(r.flowID, r.id, port, dir, label, d)
	r.ring.push(e)
	if r.limiter.allow() {
		r.sink.Capture(e)
	}
}

func defaultErrorPolicy() ErrorPolicy { return ErrorPolicy{OnError: "fail"} }

func (r *nodeRunner) policy() ErrorPolicy {
	if r.errorPolicy == nil {
		return defaultErrorPolicy()
	}
	return *r.errorPolicy
}

// runSource drives a Source node until ctx is cancelled or Run returns; a
// panic inside Run is recovered here so it cannot take down the runtime.
func (r *nodeRunner) runSource(ctx context.Context, src Source) {
	defer func() {
		if rec := recover(); rec != nil {
			r.logger.Error("source node panicked", "node", r.id, "panic", rec, "stack", string(debug.Stack()))
		}
	}()

	triggerKind := "trigger"
	if tk, ok := src.(TriggerKindProvider); ok {
		triggerKind = tk.TriggerKind()
	}

	emit := func(port string, d datagram.Datagram) error {
		fo, ok := r.outputs[port]
		if !ok {
			return fmt.Errorf("node %s: no such output port %q", r.id, port)
		}
		// A trigger node's fresh datagram (CorrelationID == ID, DGM-160)
		// starts a new tracked execution (ENG-100/ENG-130) before it is
		// allowed onto the wire, so concurrency limiting/queueing happens
		// ahead of any downstream work.
		if r.isTrigger && d.Header.CorrelationID == d.Header.ID {
			if _, err := r.tracker().Start(ctx, r.flowID, r.id, triggerKind, "", d); err != nil {
				return err
			}
		}
		r.captureDebug(DirOut, port, "", d)
		return fo.Send(ctx, d)
	}

	if err := src.Run(ctx, emit); err != nil && ctx.Err() == nil {
		r.logger.Error("source node exited with error", "node", r.id, "error", err)
	}
}

// runProcessor loops receiving from inbox and handling each datagram until
// ctx is cancelled or the wire is closed.
func (r *nodeRunner) runProcessor(ctx context.Context, proc Processor, inbox *bus.Wire) {
	for {
		in, err := inbox.Receive(ctx)
		if err != nil {
			return
		}
		r.captureDebug(DirIn, r.inputPort, "", in)
		if r.dropExpired(in, r.inputPort) {
			continue
		}
		r.handle(ctx, r.inputPort, in, proc.Process)
	}
}

// runMultiProcessor is runProcessor's counterpart for MultiInputProcessor
// node instances (e.g. merge/join): one goroutine per named input port,
// each looping over its own inbox wire and tagging every invocation with
// which port it arrived on.
func (r *nodeRunner) runMultiProcessor(ctx context.Context, proc MultiInputProcessor, port string, inbox *bus.Wire) {
	for {
		in, err := inbox.Receive(ctx)
		if err != nil {
			return
		}
		r.captureDebug(DirIn, port, "", in)
		if r.dropExpired(in, port) {
			continue
		}
		r.handle(ctx, port, in, func(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
			return proc.ProcessPort(ctx, port, in)
		})
	}
}

// dropExpired dead-letters and reports a terminal failure for a datagram
// whose TTL (DGM-100 header.ttl) has already passed (ERR-130 "expired
// datagrams go to a per-flow dead-letter topic"), instead of processing it.
// Reports true if in was expired (and thus already handled).
func (r *nodeRunner) dropExpired(in datagram.Datagram, port string) bool {
	if !in.Header.Expired(time.Now()) {
		return false
	}
	now := time.Now().UTC()
	r.logger.Warn("datagram TTL expired before processing; dead-lettering", "node", r.id, "port", port)
	r.dlq().Capture(r.flowID, r.id, port, "ttl_expired", in, now)
	r.tracker().NodeEvent(r.flowID, in, NodeIO{NodeID: r.id, Port: port, Attempt: 1, At: now, Input: in}, "fail")
	return true
}

// handle runs one datagram through invoke (either a Processor's Process or
// a MultiInputProcessor's ProcessPort bound to its port), applying
// ERR-100's error policy on failure. Panics inside invoke are recovered
// (ARC-150). port is whichever input port in arrived on (needed for
// Increment 8's per-node execution/dead-letter reporting).
func (r *nodeRunner) handle(ctx context.Context, port string, in datagram.Datagram, invoke func(context.Context, datagram.Datagram) ([]PortDatagram, error)) {
	policy := r.policy()
	attempt := 1
	var bo *backoff.Backoff
	if policy.OnError == "retry" && policy.Retry != nil {
		bo = backoff.New(
			time.Duration(policy.Retry.BackoffMs)*time.Millisecond,
			time.Duration(policy.Retry.MaxBackoffMs)*time.Millisecond,
			2,
		)
	}

	for {
		start := time.Now()
		results, err := invokeWithRecover(ctx, invoke, in)
		if err == nil {
			r.metrics.Processed.Add(1)
			r.dispatch(ctx, results)
			r.reportNode(in, port, attempt, start, results, nil, "")
			return
		}

		r.metrics.Errors.Add(1)
		nodeErr := AsNodeError(err, r.id, attempt)

		if policy.OnError == "retry" {
			max := 0
			if policy.Retry != nil {
				max = policy.Retry.Max
			}
			if attempt <= max {
				r.metrics.Retries.Add(1)
				select {
				case <-time.After(bo.Next()):
				case <-ctx.Done():
					return
				}
				attempt++
				continue
			}
			r.logger.Error("node processing failed after retries", "node", r.id, "error", nodeErr)
			// Retries exhausted: the datagram is ultimately undelivered,
			// so it is reported/dead-lettered exactly like the default
			// "fail" policy (ERR-100/ERR-130).
			r.reportNode(in, port, attempt, start, nil, nodeErr, "fail")
			return
		}

		r.applyTerminalPolicy(ctx, policy, port, attempt, start, in, nodeErr)
		return
	}
}

func (r *nodeRunner) dispatch(ctx context.Context, results []PortDatagram) {
	for _, res := range results {
		fo, ok := r.outputs[res.Port]
		if !ok {
			r.logger.Warn("no such output port", "node", r.id, "port", res.Port)
			continue
		}
		r.captureDebug(DirOut, res.Port, "", res.Datagram)
		if err := fo.Send(ctx, res.Datagram); err != nil && ctx.Err() == nil {
			r.logger.Error("send failed", "node", r.id, "port", res.Port, "error", err)
		}
	}
}

// applyTerminalPolicy handles the non-retry outcomes: errorPort routes an
// error datagram; discard drops silently; fail (the default) drops and logs.
func (r *nodeRunner) applyTerminalPolicy(ctx context.Context, policy ErrorPolicy, port string, attempt int, start time.Time, in datagram.Datagram, nodeErr *NodeError) {
	switch policy.OnError {
	case "errorPort":
		fo, ok := r.outputs["error"]
		if !ok {
			r.logger.Error("errorPort policy but no error output wired", "node", r.id, "error", nodeErr)
			r.reportNode(in, port, attempt, start, nil, nodeErr, "fail")
			return
		}
		errDgm := BuildErrorDatagram(in, nodeErr)
		r.captureDebug(DirOut, "error", "", errDgm)
		if err := fo.Send(ctx, errDgm); err != nil && ctx.Err() == nil {
			r.logger.Error("failed to send to error port", "node", r.id, "error", err)
		}
		r.reportNode(in, port, attempt, start, []PortDatagram{{Port: "error", Datagram: errDgm}}, nodeErr, "errorPort")
	case "discard":
		r.logger.Debug("node processing failed, discarding", "node", r.id, "error", nodeErr)
		r.reportNode(in, port, attempt, start, nil, nodeErr, "discard")
	case "storeForward":
		r.storeForward(port, attempt, start, in, nodeErr)
	default: // "fail"
		r.logger.Error("node processing failed", "node", r.id, "error", nodeErr)
		r.reportNode(in, port, attempt, start, nil, nodeErr, "fail")
	}
}

// storeForwardEntry is what's actually persisted in a node's durable queue:
// the datagram plus which input port it arrived on (needed by
// MultiInputProcessor nodes, which share one queue across several ports).
type storeForwardEntry struct {
	Port     string            `json:"port"`
	Datagram datagram.Datagram `json:"datagram"`
}

// storeForward implements EDGE-130's onError:"storeForward" policy: instead
// of failing/discarding/dead-lettering, the datagram is durably queued to
// local disk for the background drainer (startStoreForwardDrain) to keep
// retrying until the remote destination is reachable again. If no queue is
// available (no data directory configured, e.g. a directly test-built
// runner) this degrades to the ordinary "fail" policy rather than silently
// losing the datagram.
func (r *nodeRunner) storeForward(port string, attempt int, start time.Time, in datagram.Datagram, nodeErr *NodeError) {
	if r.sfQueue == nil {
		r.logger.Error("storeForward configured but no durable queue available; treating as fail", "node", r.id, "error", nodeErr)
		r.reportNode(in, port, attempt, start, nil, nodeErr, "fail")
		return
	}
	data, err := json.Marshal(storeForwardEntry{Port: port, Datagram: in})
	if err != nil {
		r.logger.Error("storeForward: failed to marshal entry, dead-lettering instead", "node", r.id, "error", err)
		r.reportNode(in, port, attempt, start, nil, nodeErr, "fail")
		return
	}
	dropped, err := r.sfQueue.Enqueue(data, time.Now())
	if err != nil {
		r.logger.Error("storeForward: failed to enqueue durably, dead-lettering instead", "node", r.id, "error", err)
		r.reportNode(in, port, attempt, start, nil, nodeErr, "fail")
		return
	}
	if dropped > 0 {
		r.logger.Warn("storeForward queue bounds exceeded, oldest queued entries dropped", "node", r.id, "dropped", dropped)
	}
	r.reportNode(in, port, attempt, start, nil, nodeErr, "storeForward")
}

// runStoreForwardDrain blocks, retrying the head of this node's durable
// queue in order, until ctx is cancelled — call it in its own goroutine,
// joined by the same WaitGroup as the node's ordinary receive loop(s) so a
// hot-redeploy restart of this node (ENG-140) never opens a second,
// independent Queue over the same on-disk directory while this one might
// still be mid-delivery (startNode owns that sequencing via drainStop).
// invoke is the node's own Process/ProcessPort call — delivery succeeding
// here is exactly the node itself succeeding on a re-attempt, so a
// successful drain dispatches outputs and reports success exactly like an
// ordinary handle() call would. r.sfQueue == nil (no data directory
// configured) makes this a no-op.
func (r *nodeRunner) runStoreForwardDrain(ctx context.Context, invoke func(context.Context, string, datagram.Datagram) ([]PortDatagram, error)) {
	if r.sfQueue == nil {
		return
	}
	storeforward.Drain(ctx, r.sfQueue, func(payload []byte, _ time.Time) error {
		var entry storeForwardEntry
		if err := json.Unmarshal(payload, &entry); err != nil {
			r.logger.Error("storeForward: dropping corrupt queued entry", "node", r.id, "error", err)
			return nil // can't ever deliver this one; drop it rather than retry forever
		}
		start := time.Now()
		results, err := invokeWithRecover(ctx, func(ctx context.Context, d datagram.Datagram) ([]PortDatagram, error) {
			return invoke(ctx, entry.Port, d)
		}, entry.Datagram)
		if err != nil {
			return err // still unreachable: leave queued, Drain will retry with backoff
		}
		r.metrics.Processed.Add(1)
		r.dispatch(ctx, results)
		r.reportNode(entry.Datagram, entry.Port, 1, start, results, nil, "")
		return nil
	}, storeForwardDrainIdlePoll)
}

// reportNode forwards one node's outcome to the attached Tracker (Increment
// 8, ENG-130/DBG-140) and, when the datagram is now undeliverable ("fail" or
// "discard"), to the attached DeadLetterSink (ERR-130). onError == "fail"
// (ERR-100's default, meaning genuinely unhandled) additionally publishes
// the error datagram to the flow-level error handler, if one is configured
// (ERR-120) — "discard" and "errorPort" are the flow author's own
// deliberate, acknowledged handling and do not. A no-op for tracking/DLQ
// purposes when in doesn't belong to a tracked execution — see
// Tracker.NodeEvent.
func (r *nodeRunner) reportNode(in datagram.Datagram, port string, attempt int, start time.Time, outputs []PortDatagram, nodeErr *NodeError, onError string) {
	ev := NodeIO{
		NodeID:     r.id,
		Port:       port,
		Attempt:    attempt,
		At:         start.UTC(),
		DurationUs: time.Since(start).Microseconds(),
		Input:      in,
		Outputs:    outputs,
		Err:        nodeErr,
	}
	r.tracker().NodeEvent(r.flowID, in, ev, onError)
	if nodeErr == nil {
		return
	}
	if onError == "fail" || onError == "discard" {
		r.dlq().Capture(r.flowID, r.id, port, "node_error", in, time.Now().UTC())
	}
	if onError == "fail" && r.errorFlowTarget != nil {
		if target := r.errorFlowTarget(); target != "" {
			topics.DefaultBroker.Publish(context.Background(), ErrorFlowTopic(target), nil, BuildErrorDatagram(in, nodeErr))
		}
	}
}

// invokeWithRecover calls invoke, converting any panic into a *NodeError
// (ARC-150).
func invokeWithRecover(ctx context.Context, invoke func(context.Context, datagram.Datagram) ([]PortDatagram, error), in datagram.Datagram) (results []PortDatagram, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = &NodeError{Message: fmt.Sprint(rec), Stack: string(debug.Stack())}
		}
	}()
	return invoke(ctx, in)
}

// AsNodeError converts err into a *NodeError (tagging Node/Attempt),
// reusing its fields as-is if it already is one. Exported so node types
// implementing their own error-scope semantics (e.g. PROC-370's try-catch)
// can produce the identical ERR-100 shape the built-in errorPort policy uses.
func AsNodeError(err error, nodeID string, attempt int) *NodeError {
	var ne *NodeError
	if errors.As(err, &ne) {
		ne.Node = nodeID
		ne.Attempt = attempt
		return ne
	}
	return &NodeError{Message: err.Error(), Node: nodeID, Attempt: attempt}
}

// BuildErrorDatagram carries the original datagram plus the ERR-100 error
// object — the exact shape errorPort routes, exported for reuse (e.g.
// PROC-370's try-catch node).
func BuildErrorDatagram(original datagram.Datagram, nodeErr *NodeError) datagram.Datagram {
	d := datagram.NewCaused(original, datagram.Source{NodeID: nodeErr.Node}, datagram.Payload{
		Value: map[string]any{
			"original": original,
			"error": map[string]any{
				"message": nodeErr.Message,
				"code":    nodeErr.Code,
				"node":    nodeErr.Node,
				"stack":   nodeErr.Stack,
				"attempt": nodeErr.Attempt,
			},
		},
	})
	d.Header.Quality = datagram.QualityBad
	return d
}
