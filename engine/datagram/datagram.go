// Package datagram implements the envelope defined in
// DataPipe-Requirements-Specification.md §6 (DGM-100..170): the single,
// mandatory message format carried on the internal bus between connectors,
// processors, and sinks.
package datagram

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// DefaultBinaryRefThreshold is the size (bytes) above which Clone shares a
// binary payload's backing array by reference instead of copying it
// (DGM-120: "large blobs do not travel through every node copy").
const DefaultBinaryRefThreshold = 256 * 1024

// Source identifies where a datagram originated (DGM-100 header.source).
type Source struct {
	FlowID    string `json:"flowId"`
	NodeID    string `json:"nodeId"`
	RuntimeID string `json:"runtimeId"`
	Connector string `json:"connector"`
	Origin    string `json:"origin"`
}

// Header carries the envelope metadata (DGM-100). ID, Timestamp, and Source
// are set once by New/NewCaused (DGM-110: "set by the runtime; nodes cannot
// forge them but MAY read them") — node code must treat them as read-only
// once a datagram exists; enforcing that boundary at the node-execution
// level is an Increment 2+ concern. Every other field is freely
// readable/writable by processing nodes.
type Header struct {
	ID              string            `json:"id"`
	CorrelationID   string            `json:"correlationId"`
	CausationID     string            `json:"causationId,omitempty"`
	Timestamp       time.Time         `json:"timestamp"`
	SourceTimestamp *time.Time        `json:"sourceTimestamp,omitempty"`
	Source          Source            `json:"source"`
	SchemaRef       string            `json:"schemaRef,omitempty"`
	ContentType     string            `json:"contentType,omitempty"`
	Quality         Quality           `json:"quality"`
	Priority        int               `json:"priority"`
	TTLMillis       *int64            `json:"ttl"`
	Tags            map[string]string `json:"tags,omitempty"`
}

// ExpiresAt returns the absolute expiry time if a TTL is set (DGM-100:
// "optional expiry (ms); expired datagrams are dropped and counted").
func (h Header) ExpiresAt() (time.Time, bool) {
	if h.TTLMillis == nil {
		return time.Time{}, false
	}
	return h.Timestamp.Add(time.Duration(*h.TTLMillis) * time.Millisecond), true
}

// Expired reports whether the datagram has passed its TTL as of now.
func (h Header) Expired(now time.Time) bool {
	exp, ok := h.ExpiresAt()
	return ok && now.After(exp)
}

// Payload holds either a JSON-compatible value or a binary blob (DGM-120).
// Exactly one of Value/Binary is populated; Binary takes precedence when set.
type Payload struct {
	Value  any    `json:"value,omitempty"`
	Binary []byte `json:"binary,omitempty"`
}

// IsBinary reports whether this payload carries a binary blob.
func (p Payload) IsBinary() bool { return p.Binary != nil }

// Size returns the binary payload length, or 0 for JSON-value payloads (only
// binary size matters for the DGM-120 by-reference threshold).
func (p Payload) Size() int { return len(p.Binary) }

// TraceEntry records one node's processing of a datagram (DGM-100 "trace"),
// only populated in debug mode (DBG-100).
type TraceEntry struct {
	NodeID     string `json:"nodeId"`
	In         string `json:"in,omitempty"`
	Out        string `json:"out,omitempty"`
	DurationUs int64  `json:"durationUs"`
}

// Datagram is the envelope defined by DGM-100.
type Datagram struct {
	Header  Header       `json:"header"`
	Payload Payload      `json:"payload"`
	Trace   []TraceEntry `json:"trace,omitempty"`
}

var (
	idMu      sync.Mutex
	idEntropy = ulid.Monotonic(rand.Reader, 0)
)

// newID returns a unique, sortable ULID (DGM-100 header.id).
func newID() string {
	idMu.Lock()
	defer idMu.Unlock()
	return ulid.MustNew(ulid.Timestamp(time.Now()), idEntropy).String()
}

// New creates a fresh, uncaused datagram: a new correlation chain starts
// here, so CorrelationID equals the new ID (DGM-160 lineage).
func New(source Source, payload Payload) Datagram {
	id := newID()
	return Datagram{
		Header: Header{
			ID:            id,
			CorrelationID: id,
			Timestamp:     time.Now().UTC(),
			Source:        source,
			Quality:       QualityGood,
			Priority:      4,
		},
		Payload: payload,
	}
}

// NewCaused creates a datagram produced from processing parent, inheriting
// its correlation id and recording parent's id as the causation id
// (DGM-160: "correlationId/causationId allow reconstructing which input
// datagram produced which outputs across the whole flow").
func NewCaused(parent Datagram, source Source, payload Payload) Datagram {
	d := New(source, payload)
	d.Header.CorrelationID = parent.Header.CorrelationID
	d.Header.CausationID = parent.Header.ID
	d.Header.Quality = parent.Header.Quality
	return d
}

// Clone returns an independent copy suitable for fan-out delivery (BUS-140:
// "delivers an independent copy"). Tags are deep-copied so branches can't
// mutate each other's headers. Binary payloads at or above threshold are
// shared by reference (not copied) per DGM-120; below threshold they are
// defensively copied. Pass DefaultBinaryRefThreshold unless a node config
// overrides it.
func (d Datagram) Clone(threshold int) Datagram {
	clone := d
	if d.Header.Tags != nil {
		tags := make(map[string]string, len(d.Header.Tags))
		for k, v := range d.Header.Tags {
			tags[k] = v
		}
		clone.Header.Tags = tags
	}
	if d.Header.SourceTimestamp != nil {
		ts := *d.Header.SourceTimestamp
		clone.Header.SourceTimestamp = &ts
	}
	if d.Header.TTLMillis != nil {
		ttl := *d.Header.TTLMillis
		clone.Header.TTLMillis = &ttl
	}
	if d.Payload.IsBinary() && d.Payload.Size() < threshold {
		buf := make([]byte, len(d.Payload.Binary))
		copy(buf, d.Payload.Binary)
		clone.Payload.Binary = buf
	}
	if len(d.Trace) > 0 {
		trace := make([]TraceEntry, len(d.Trace))
		copy(trace, d.Trace)
		clone.Trace = trace
	}
	return clone
}
