package datagram

import "time"

// BatchHeader is the header shared by every datagram in a Batch (DGM-130).
type BatchHeader struct {
	ID        string    `json:"id"`
	Timestamp time.Time `json:"timestamp"`
	Source    Source    `json:"source"`
}

// Batch is an ordered list of datagrams sharing a batch header (DGM-130).
// Datagram-at-a-time nodes have batches unwrapped for them automatically by
// the engine (Increment 2); batch-aware nodes (windowing, aggregation, bulk
// insert) receive the Batch directly.
type Batch struct {
	Header    BatchHeader `json:"header"`
	Datagrams []Datagram  `json:"datagrams"`
}

// NewBatch wraps datagrams into a batch with a fresh header.
func NewBatch(source Source, datagrams []Datagram) Batch {
	return Batch{
		Header: BatchHeader{
			ID:        newID(),
			Timestamp: time.Now().UTC(),
			Source:    source,
		},
		Datagrams: datagrams,
	}
}

// Quality returns the combined (worst-of) quality across every datagram in
// the batch (DGM-140).
func (b Batch) Quality() Quality {
	qualities := make([]Quality, len(b.Datagrams))
	for i, d := range b.Datagrams {
		qualities[i] = d.Header.Quality
	}
	return Combine(qualities...)
}
