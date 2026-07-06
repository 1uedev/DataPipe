package datagram

import (
	"encoding/json"
	"testing"
	"time"
)

func testSource() Source {
	return Source{FlowID: "flow-abc", NodeID: "node-17", RuntimeID: "server-1", Connector: "opcua", Origin: "ns=2;s=Line3.Temperature"}
}

func TestDGM100_StructureRoundTrip(t *testing.T) {
	d := New(testSource(), Payload{Value: map[string]any{"temp": 42.5}})
	d.Header.Tags = map[string]string{"site": "fab2", "line": "3"}
	d.Header.ContentType = "application/json"

	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var got Datagram
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if got.Header.ID != d.Header.ID {
		t.Errorf("id: got %q want %q", got.Header.ID, d.Header.ID)
	}
	if got.Header.Source != d.Header.Source {
		t.Errorf("source: got %+v want %+v", got.Header.Source, d.Header.Source)
	}
	if got.Header.Tags["site"] != "fab2" {
		t.Errorf("tags not round-tripped: %+v", got.Header.Tags)
	}

	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("Unmarshal top level: %v", err)
	}
	for _, want := range []string{"header", "payload"} {
		if _, ok := fields[want]; !ok {
			t.Errorf("missing top-level field %q in %s", want, raw)
		}
	}
}

func TestDGM110_RuntimeAssignedFieldsSetOnCreation(t *testing.T) {
	before := time.Now().UTC()
	d := New(testSource(), Payload{Value: 1})
	after := time.Now().UTC()

	if d.Header.ID == "" {
		t.Error("ID must be set by New")
	}
	if d.Header.Timestamp.Before(before) || d.Header.Timestamp.After(after) {
		t.Errorf("Timestamp %v not within [%v, %v]", d.Header.Timestamp, before, after)
	}
	if d.Header.Source != testSource() {
		t.Errorf("Source not set as given: %+v", d.Header.Source)
	}
}

func TestDGM120_JSONPayload(t *testing.T) {
	d := New(testSource(), Payload{Value: map[string]any{"a": 1.0, "b": "s"}})
	if d.Payload.IsBinary() {
		t.Error("JSON-value payload should not report IsBinary")
	}
	if d.Payload.Size() != 0 {
		t.Errorf("Size() for JSON payload = %d, want 0", d.Payload.Size())
	}
}

func TestDGM120_BinaryPayloadByReferenceAboveThreshold(t *testing.T) {
	threshold := 1024
	big := make([]byte, threshold+1)
	big[0] = 0xAB

	d := New(testSource(), Payload{Binary: big})
	clone := d.Clone(threshold)

	if !clone.Payload.IsBinary() {
		t.Fatal("clone should still be binary")
	}
	if &clone.Payload.Binary[0] != &d.Payload.Binary[0] {
		t.Error("binary payload at/above threshold must be shared by reference, not copied")
	}
}

func TestDGM120_BinaryPayloadCopiedBelowThreshold(t *testing.T) {
	threshold := 1024
	small := make([]byte, threshold-1)
	small[0] = 0xAB

	d := New(testSource(), Payload{Binary: small})
	clone := d.Clone(threshold)

	if &clone.Payload.Binary[0] == &small[0] {
		t.Error("binary payload below threshold should be defensively copied")
	}
	clone.Payload.Binary[0] = 0xFF
	if d.Payload.Binary[0] != 0xAB {
		t.Error("mutating the clone must not affect the original below threshold")
	}
}

func TestDGM130_BatchOrderingAndQuality(t *testing.T) {
	source := testSource()
	d1 := New(source, Payload{Value: 1})
	d1.Header.Quality = QualityGood
	d2 := New(source, Payload{Value: 2})
	d2.Header.Quality = QualityBad
	d3 := New(source, Payload{Value: 3})
	d3.Header.Quality = QualityUncertain

	b := NewBatch(source, []Datagram{d1, d2, d3})

	if len(b.Datagrams) != 3 {
		t.Fatalf("batch has %d datagrams, want 3", len(b.Datagrams))
	}
	for i, want := range []int{1, 2, 3} {
		got, _ := b.Datagrams[i].Payload.Value.(int)
		if got != want {
			t.Errorf("batch order broken at %d: got %v want %v", i, b.Datagrams[i].Payload.Value, want)
		}
	}
	if b.Quality() != QualityBad {
		t.Errorf("batch quality = %v, want worst-of = BAD", b.Quality())
	}
}

func TestDGM140_QualityCombine(t *testing.T) {
	cases := []struct {
		name string
		in   []Quality
		want Quality
	}{
		{"empty", nil, QualityGood},
		{"all good", []Quality{QualityGood, QualityGood}, QualityGood},
		{"good and stale", []Quality{QualityGood, QualityStale}, QualityStale},
		{"uncertain and bad", []Quality{QualityUncertain, QualityBad}, QualityBad},
		{"order independent", []Quality{QualityBad, QualityGood, QualityStale}, QualityBad},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Combine(tc.in...); got != tc.want {
				t.Errorf("Combine(%v) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestDGM160_LineageCausation(t *testing.T) {
	parent := New(testSource(), Payload{Value: "in"})
	child := NewCaused(parent, testSource(), Payload{Value: "out"})

	if child.Header.CorrelationID != parent.Header.CorrelationID {
		t.Errorf("child correlation id = %q, want inherited %q", child.Header.CorrelationID, parent.Header.CorrelationID)
	}
	if child.Header.CausationID != parent.Header.ID {
		t.Errorf("child causation id = %q, want parent id %q", child.Header.CausationID, parent.Header.ID)
	}
	if child.Header.ID == parent.Header.ID {
		t.Error("child must get its own id")
	}

	grandchild := NewCaused(child, testSource(), Payload{Value: "out2"})
	if grandchild.Header.CorrelationID != parent.Header.CorrelationID {
		t.Error("correlation id must propagate transitively across the whole chain")
	}
	if grandchild.Header.CausationID != child.Header.ID {
		t.Error("causation id must point to the immediate parent, not the root")
	}
}

func TestDGM160_SelfCorrelatedRoot(t *testing.T) {
	d := New(testSource(), Payload{Value: nil})
	if d.Header.CorrelationID != d.Header.ID {
		t.Errorf("a root datagram must be self-correlated: id=%q correlationId=%q", d.Header.ID, d.Header.CorrelationID)
	}
	if d.Header.CausationID != "" {
		t.Errorf("a root datagram must have no causation id, got %q", d.Header.CausationID)
	}
}

func TestDGM100_TTLExpiry(t *testing.T) {
	d := New(testSource(), Payload{Value: 1})
	ttl := int64(10)
	d.Header.TTLMillis = &ttl

	if d.Header.Expired(d.Header.Timestamp.Add(5 * time.Millisecond)) {
		t.Error("must not be expired before TTL elapses")
	}
	if !d.Header.Expired(d.Header.Timestamp.Add(11 * time.Millisecond)) {
		t.Error("must be expired after TTL elapses")
	}
}

func TestDGM140_CausedInheritsParentQualityByDefault(t *testing.T) {
	parent := New(testSource(), Payload{Value: 1})
	parent.Header.Quality = QualityUncertain

	child := NewCaused(parent, testSource(), Payload{Value: 2})
	if child.Header.Quality != QualityUncertain {
		t.Errorf("child quality = %v, want inherited %v", child.Header.Quality, QualityUncertain)
	}
}

func TestClone_TagsAreIndependent(t *testing.T) {
	d := New(testSource(), Payload{Value: 1})
	d.Header.Tags = map[string]string{"k": "v"}

	clone := d.Clone(DefaultBinaryRefThreshold)
	clone.Header.Tags["k"] = "changed"

	if d.Header.Tags["k"] != "v" {
		t.Error("mutating a clone's tags must not affect the original (BUS-140 independent copies)")
	}
}
