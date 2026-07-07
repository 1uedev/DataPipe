package storeforward

import (
	"context"
	"time"

	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

// Deliver attempts to deliver one queued entry's payload. Returning an
// error means "destination still unreachable, try this same entry again
// later" — unlike the ordinary per-node retry policy (ERR-100), Drain never
// gives up and moves on; it keeps retrying the head of the queue forever
// (or until ctx is cancelled), which is exactly what "store-and-forward
// until reconnect" (EDGE-130) means.
type Deliver func(payload []byte, enqueuedAt time.Time) error

// Drain runs until ctx is cancelled, repeatedly delivering the queue's
// oldest entry and removing it on success before moving to the next one.
// When the queue is empty it polls at idlePoll; when a delivery fails it
// waits with the shared reconnect backoff (CON-130) before retrying the
// same entry.
func Drain(ctx context.Context, q *Queue, deliver Deliver, idlePoll time.Duration) {
	b := backoff.New(idlePoll, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			return
		}
		entry, ok := q.Peek()
		if !ok {
			if !sleep(ctx, idlePoll) {
				return
			}
			continue
		}
		if err := deliver(entry.Payload, entry.EnqueuedAt); err != nil {
			if !sleep(ctx, b.Next()) {
				return
			}
			continue
		}
		b.Reset()
		q.Remove(entry.ID)
	}
}

// sleep waits for d or ctx cancellation, returning false if ctx was
// cancelled first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
