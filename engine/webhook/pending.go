package webhook

import "sync"

// Response is what an "http-response" node (SNK-170) hands back to a
// pending "http-in" request.
type Response struct {
	Status  int
	Headers map[string]string
	Body    []byte
}

// PendingResponses correlates an in-flight webhook request (keyed by its
// triggering datagram's header id, which becomes every downstream
// datagram's correlation id per DGM-160) with the goroutine blocked
// waiting to write its HTTP response.
type PendingResponses struct {
	mu sync.Mutex
	m  map[string]chan Response
}

// Default is the process-wide instance every "http-in"/"http-response" node
// pair uses.
var Default = NewPendingResponses()

func NewPendingResponses() *PendingResponses {
	return &PendingResponses{m: map[string]chan Response{}}
}

// Await registers correlationID as awaiting a response; the returned cancel
// func must be called once the caller stops waiting (success or timeout),
// to prevent unbounded growth of the map (BUS-110's "every queue has a
// bound" applies here too, just expressed as prompt cleanup instead of a
// fixed capacity).
func (p *PendingResponses) Await(correlationID string) (<-chan Response, func()) {
	ch := make(chan Response, 1)
	p.mu.Lock()
	p.m[correlationID] = ch
	p.mu.Unlock()
	return ch, func() {
		p.mu.Lock()
		delete(p.m, correlationID)
		p.mu.Unlock()
	}
}

// Reply delivers resp to whoever is awaiting correlationID, if anyone.
// Reports false if nothing is (or is no longer) waiting — e.g. the request
// already timed out.
func (p *PendingResponses) Reply(correlationID string, resp Response) bool {
	p.mu.Lock()
	ch, ok := p.m[correlationID]
	p.mu.Unlock()
	if !ok {
		return false
	}
	select {
	case ch <- resp:
		return true
	default:
		return false
	}
}
