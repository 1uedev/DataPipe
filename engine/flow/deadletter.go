// Dead letters (Increment 8, ERR-130): a datagram that would otherwise be
// silently lost — a node error that resolves to "fail" or "discard" (after
// any retries), or a TTL-expired datagram — is captured here instead,
// durable and re-injectable after the underlying issue is fixed.
package flow

import (
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// DeadLetterSink receives one dead-lettered datagram at a time. Deployment
// calls it unconditionally; a real sink is expected to persist every call.
type DeadLetterSink interface {
	Capture(flowID, nodeID, port, reason string, d datagram.Datagram, at time.Time)
}

type noopDeadLetterSink struct{}

func (noopDeadLetterSink) Capture(string, string, string, string, datagram.Datagram, time.Time) {}

// NoopDeadLetterSink is the default sink: capturing costs nothing.
var NoopDeadLetterSink DeadLetterSink = noopDeadLetterSink{}
