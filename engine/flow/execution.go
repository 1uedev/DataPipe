// Execution tracking (Increment 8, ENG-130/DBG-140): a triggered flow's
// entry (Trigger) node starts a durably tracked execution, keyed by the
// root datagram's correlation id (DGM-160, Flow-File-Format.md's "Trigger
// nodes and execution ids"); every node's outcome and the execution's final
// status are reported to an attached ExecutionSink. Unlike DebugSink
// (DBG-170: sampled/rate-limited), ExecutionSink is called for every event
// unconditionally — DBG-140 requires "every execution recorded".
package flow

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// ExecutionStatus is ENG-130's durable status enum, plus "crashed" (ERR-150:
// "triggered executions interrupted mid-run are marked crashed").
type ExecutionStatus string

const (
	ExecutionRunning   ExecutionStatus = "running"
	ExecutionWaiting   ExecutionStatus = "waiting"
	ExecutionSuccess   ExecutionStatus = "success"
	ExecutionFailed    ExecutionStatus = "failed"
	ExecutionCancelled ExecutionStatus = "cancelled"
	ExecutionCrashed   ExecutionStatus = "crashed"
)

// NodeIO is one node's contribution to an execution's trace (DBG-140
// "per-node in/out data").
type NodeIO struct {
	NodeID     string
	Port       string
	Attempt    int
	At         time.Time
	DurationUs int64
	Input      datagram.Datagram
	Outputs    []PortDatagram
	Err        *NodeError
}

// ExecutionSink receives execution lifecycle events. Deployment calls it
// unconditionally; a real sink is expected to persist every call (DBG-140),
// not sample it.
type ExecutionSink interface {
	// Waiting reports an execution queued behind a concurrency limit
	// (ENG-130 "queue" concurrencyPolicy), before a slot is available.
	Waiting(flowID, executionID, triggerNodeID string, at time.Time, seed datagram.Datagram)
	// Started reports an execution beginning to run (a fresh trigger fire,
	// or a queued one that just acquired its concurrency slot).
	Started(flowID, executionID, triggerNodeID, triggerKind, reRunOf string, at time.Time, seed datagram.Datagram)
	NodeEvent(flowID, executionID string, ev NodeIO)
	Finished(flowID, executionID string, status ExecutionStatus, at time.Time, reason string)
}

type noopExecutionSink struct{}

func (noopExecutionSink) Waiting(string, string, string, time.Time, datagram.Datagram) {}
func (noopExecutionSink) Started(string, string, string, string, string, time.Time, datagram.Datagram) {
}
func (noopExecutionSink) NodeEvent(string, string, NodeIO)                            {}
func (noopExecutionSink) Finished(string, string, ExecutionStatus, time.Time, string) {}

// NoopExecutionSink is the default sink: a Tracker still enforces
// concurrency/timeout without it, it just reports nothing.
var NoopExecutionSink ExecutionSink = noopExecutionSink{}

// ErrConcurrencyRejected is returned by Tracker.Start when maxConcurrency is
// reached and the flow's concurrencyPolicy is "reject" (ENG-130). Trigger
// node types (e.g. http-in) can check errors.Is against it to answer with a
// too-many-requests-style response instead of a generic failure.
var ErrConcurrencyRejected = errors.New("flow: execution concurrency limit reached")

// execState is one in-flight tracked execution: pending counts datagrams
// still owed a terminal outcome (starts at 1 for the trigger's own seed
// datagram; each node event adds len(Outputs)-1 for the one it consumed).
// The execution finishes when pending reaches zero.
type execState struct {
	pending int
	failed  bool
	reason  string
}

// Tracker implements ENG-130: per-flow concurrency limiting (queue or
// reject), execution timeout, and completion detection via the pending-
// descendant count above, keyed by execution id (root correlation id).
// One Tracker per Deployment — today's engine reconciles one flow per
// Deployment (see TODO.md), so a single flow-wide limit is sufficient.
type Tracker struct {
	mu      sync.Mutex
	reject  bool // concurrencyPolicy == "reject"; false = "queue" (default)
	timeout time.Duration
	sink    ExecutionSink
	sem     chan struct{} // nil when maxConcurrency <= 0 (unlimited)
	active  map[string]*execState
	timers  map[string]*time.Timer
}

// NewTracker builds a Tracker for one flow's settings. maxConcurrency <= 0
// means unlimited. reject selects ENG-130's "reject" concurrencyPolicy;
// false selects the default "queue" (Start blocks until a slot frees).
// timeout <= 0 means no execution timeout. A nil sink defaults to
// NoopExecutionSink.
func NewTracker(maxConcurrency int, reject bool, timeout time.Duration, sink ExecutionSink) *Tracker {
	if sink == nil {
		sink = NoopExecutionSink
	}
	t := &Tracker{
		reject:  reject,
		timeout: timeout,
		sink:    sink,
		active:  map[string]*execState{},
		timers:  map[string]*time.Timer{},
	}
	if maxConcurrency > 0 {
		t.sem = make(chan struct{}, maxConcurrency)
	}
	return t
}

// Start begins tracking a new execution rooted at seed (whose CorrelationID
// == ID identifies it as a fresh root, DGM-160), acquiring a concurrency
// slot per ENG-130's queue/reject policy. Under "queue", ctx cancellation
// while waiting aborts the acquire and returns ctx.Err(); under "reject", a
// full slot returns ErrConcurrencyRejected immediately without recording
// anything (the execution was never accepted). triggerKind is e.g.
// "webhook" or "rerun"; reRunOf is the original execution id being
// replayed, or "".
func (t *Tracker) Start(ctx context.Context, flowID, triggerNodeID, triggerKind, reRunOf string, seed datagram.Datagram) (executionID string, err error) {
	executionID = seed.Header.CorrelationID

	if t.sem != nil {
		select {
		case t.sem <- struct{}{}:
		default:
			if t.reject {
				return "", ErrConcurrencyRejected
			}
			t.sink.Waiting(flowID, executionID, triggerNodeID, time.Now().UTC(), seed)
			select {
			case t.sem <- struct{}{}:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
	}

	now := time.Now().UTC()
	t.mu.Lock()
	t.active[executionID] = &execState{pending: 1}
	if t.timeout > 0 {
		t.timers[executionID] = time.AfterFunc(t.timeout, func() { t.timeoutExecution(flowID, executionID) })
	}
	t.mu.Unlock()

	t.sink.Started(flowID, executionID, triggerNodeID, triggerKind, reRunOf, now, seed)
	return executionID, nil
}

func (t *Tracker) timeoutExecution(flowID, executionID string) {
	t.mu.Lock()
	_, ok := t.active[executionID]
	if ok {
		delete(t.active, executionID)
		delete(t.timers, executionID)
	}
	t.mu.Unlock()
	if !ok {
		return
	}
	t.releaseSlot()
	t.sink.Finished(flowID, executionID, ExecutionFailed, time.Now().UTC(), "timeout")
}

func (t *Tracker) releaseSlot() {
	if t.sem != nil {
		<-t.sem
	}
}

// NodeEvent reports one node's outcome for the datagram it just processed.
// onError is the effective ERR-100 policy outcome ("" on success, or
// "fail"/"retry"/"errorPort"/"discard" — retries-exhausted-under-"retry"
// should be reported as "fail", since the datagram is ultimately
// undelivered). A no-op if in's correlation id isn't a tracked execution
// (e.g. a streaming flow, or a datagram no Start ever recorded) — every
// nodeRunner unconditionally calls this so node code stays oblivious to
// whether it's part of a tracked execution.
func (t *Tracker) NodeEvent(flowID string, in datagram.Datagram, ev NodeIO, onError string) {
	executionID := in.Header.CorrelationID
	t.mu.Lock()
	st, ok := t.active[executionID]
	if !ok {
		t.mu.Unlock()
		return // untracked: an ordinary streaming datagram, or a straggler after timeout/cancel already finished this execution
	}
	st.pending += len(ev.Outputs) - 1
	if ev.Err != nil && onError == "fail" {
		st.failed = true
		st.reason = ev.Err.Message
	}
	done := st.pending <= 0
	if done {
		delete(t.active, executionID)
		if timer, ok := t.timers[executionID]; ok {
			timer.Stop()
			delete(t.timers, executionID)
		}
	}
	t.mu.Unlock()

	t.sink.NodeEvent(flowID, executionID, ev)

	if done {
		t.releaseSlot()
		status, reason := ExecutionSuccess, ""
		if st.failed {
			status, reason = ExecutionFailed, st.reason
		}
		t.sink.Finished(flowID, executionID, status, time.Now().UTC(), reason)
	}
}

// Cancel marks a running execution cancelled (ENG-130), freeing its
// concurrency slot. Does not forcibly interrupt in-flight node processing
// — a documented limitation shared with the timeout path above, since the
// engine has no per-execution cancellation context, only per-node/
// per-deployment ones. Reports false if executionID isn't currently
// tracked (already finished, or unknown).
func (t *Tracker) Cancel(flowID, executionID string) bool {
	t.mu.Lock()
	_, ok := t.active[executionID]
	if ok {
		delete(t.active, executionID)
		if timer, ok := t.timers[executionID]; ok {
			timer.Stop()
			delete(t.timers, executionID)
		}
	}
	t.mu.Unlock()
	if !ok {
		return false
	}
	t.releaseSlot()
	t.sink.Finished(flowID, executionID, ExecutionCancelled, time.Now().UTC(), "cancelled")
	return true
}

// Tracking reports whether executionID is currently active, for callers
// that need to distinguish "already finished" from "never existed" (e.g. a
// re-run request racing a still-running original).
func (t *Tracker) Tracking(executionID string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	_, ok := t.active[executionID]
	return ok
}

// Idle reports whether no execution is currently active — Deployment uses
// this to decide whether it's safe to Reconfigure (resizing the semaphore
// mid-execution would lose accounting for whatever's in flight).
func (t *Tracker) Idle() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.active) == 0
}

// Reconfigure updates concurrency/timeout settings in place, preserving the
// Tracker's own identity. This matters: every already-running node's
// nodeRunner captured a pointer to this exact Tracker at Deployment.
// startNode time and keeps it for its whole lifetime (the same pattern as
// DebugSink/ConnectionResolver) — a node whose own fingerprint didn't
// change and so isn't restarted by a given Deploy call would report to a
// stale, orphaned Tracker forever if Deployment ever replaced g.execTracker
// wholesale instead of mutating this one. Callers must only call this when
// Idle() (see Deployment.reconfigureTrackerLocked).
func (t *Tracker) Reconfigure(maxConcurrency int, reject bool, timeout time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reject = reject
	t.timeout = timeout
	if maxConcurrency > 0 {
		t.sem = make(chan struct{}, maxConcurrency)
	} else {
		t.sem = nil
	}
}

// SetSink replaces the attached ExecutionSink. A nil sink resets to
// NoopExecutionSink.
func (t *Tracker) SetSink(sink ExecutionSink) {
	if sink == nil {
		sink = NoopExecutionSink
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sink = sink
}
