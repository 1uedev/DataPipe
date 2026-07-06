// Node execution: panic recovery at the node boundary (ARC-150: "a
// panicking node must never take down the runtime") and the uniform
// per-node error policy (ERR-100: fail | retry | errorPort | discard).
package flow

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync/atomic"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

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

	emit := func(port string, d datagram.Datagram) error {
		fo, ok := r.outputs[port]
		if !ok {
			return fmt.Errorf("node %s: no such output port %q", r.id, port)
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
		r.handle(ctx, proc, in)
	}
}

// handle runs one datagram through proc, applying ERR-100's error policy on
// failure. Panics inside Process are recovered (ARC-150).
func (r *nodeRunner) handle(ctx context.Context, proc Processor, in datagram.Datagram) {
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
		results, err := invokeWithRecover(ctx, proc, in)
		if err == nil {
			r.metrics.Processed.Add(1)
			r.dispatch(ctx, results)
			return
		}

		r.metrics.Errors.Add(1)
		nodeErr := asNodeError(err, r.id, attempt)

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
			return
		}

		r.applyTerminalPolicy(ctx, policy, in, nodeErr)
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
func (r *nodeRunner) applyTerminalPolicy(ctx context.Context, policy ErrorPolicy, in datagram.Datagram, nodeErr *NodeError) {
	switch policy.OnError {
	case "errorPort":
		fo, ok := r.outputs["error"]
		if !ok {
			r.logger.Error("errorPort policy but no error output wired", "node", r.id, "error", nodeErr)
			return
		}
		errDgm := buildErrorDatagram(in, nodeErr)
		r.captureDebug(DirOut, "error", "", errDgm)
		if err := fo.Send(ctx, errDgm); err != nil && ctx.Err() == nil {
			r.logger.Error("failed to send to error port", "node", r.id, "error", err)
		}
	case "discard":
		r.logger.Debug("node processing failed, discarding", "node", r.id, "error", nodeErr)
	default: // "fail"
		r.logger.Error("node processing failed", "node", r.id, "error", nodeErr)
	}
}

// invokeWithRecover calls proc.Process, converting any panic into a
// *NodeError (ARC-150).
func invokeWithRecover(ctx context.Context, proc Processor, in datagram.Datagram) (results []PortDatagram, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = &NodeError{Message: fmt.Sprint(rec), Stack: string(debug.Stack())}
		}
	}()
	return proc.Process(ctx, in)
}

func asNodeError(err error, nodeID string, attempt int) *NodeError {
	var ne *NodeError
	if errors.As(err, &ne) {
		ne.Node = nodeID
		ne.Attempt = attempt
		return ne
	}
	return &NodeError{Message: err.Error(), Node: nodeID, Attempt: attempt}
}

// buildErrorDatagram carries the original datagram plus the ERR-100 error
// object.
func buildErrorDatagram(original datagram.Datagram, nodeErr *NodeError) datagram.Datagram {
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
