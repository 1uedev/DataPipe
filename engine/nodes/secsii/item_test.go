package secsii

import (
	"reflect"
	"testing"
)

func roundTrip(t *testing.T, it Item) Item {
	t.Helper()
	encoded := Encode(it)
	decoded, remaining, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(remaining) != 0 {
		t.Errorf("Decode left %d unconsumed bytes", len(remaining))
	}
	return decoded
}

func TestCON220_ScalarRoundTrip(t *testing.T) {
	cases := []Item{
		A("HELLO"),
		JIS8("world"),
		B(0x01, 0x02, 0xFF),
		Bool(true, false, true),
		I1v(-1, 0, 127),
		I2v(-32000, 0, 32000),
		I4v(-2000000000, 0, 2000000000),
		I8v(-9000000000000000000, 0, 9000000000000000000),
		U1v(0, 128, 255),
		U2v(0, 40000, 65535),
		U4v(0, 3000000000, 4294967295),
		U8v(0, 1, 18000000000000000000),
		F4v(1.5, -2.25),
		F8v(3.14159, -2.71828),
	}
	for _, it := range cases {
		got := roundTrip(t, it)
		if !reflect.DeepEqual(got, it) {
			t.Errorf("round-trip %s: got %+v, want %+v", it.Format, got, it)
		}
	}
}

// TestCON220_NestedListRoundTrip mirrors the shape a real GEM message takes
// (e.g. S2F33 Define Report: RPTID + list of VIDs, itself inside a list of
// reports) — a list containing scalars and sub-lists at multiple depths.
func TestCON220_NestedListRoundTrip(t *testing.T) {
	it := L(
		U4v(1001), // RPTID
		L( // list of VIDs in this report
			U4v(2001),
			U4v(2002),
			U4v(2003),
		),
	)
	outer := L(it, L(U4v(1002), L(U4v(2004))))

	got := roundTrip(t, outer)
	if !reflect.DeepEqual(got, outer) {
		t.Errorf("nested round-trip mismatch:\ngot  %+v\nwant %+v", got, outer)
	}
}

func TestCON220_EmptyListRoundTrip(t *testing.T) {
	it := L()
	got := roundTrip(t, it)
	if got.Format != FormatList || len(got.List) != 0 {
		t.Errorf("got %+v, want empty list", got)
	}
}

func TestCON220_LargeListUsesMultiByteLength(t *testing.T) {
	items := make([]Item, 300) // > 255, forces a 2-byte length
	for i := range items {
		items[i] = U1v(byte(i % 256))
	}
	it := L(items...)
	encoded := Encode(it)
	if encoded[0]&0x03 != 2 {
		t.Errorf("format byte length-bytes-count = %d, want 2 for a 300-element list", encoded[0]&0x03)
	}
	got := roundTrip(t, it)
	if len(got.List) != 300 {
		t.Errorf("got %d elements, want 300", len(got.List))
	}
}

func TestCON220_ValueConversion(t *testing.T) {
	cases := []struct {
		item Item
		want any
	}{
		{A("hi"), "hi"},
		{U4v(42), uint32(42)},
		{U4v(1, 2, 3), []uint32{1, 2, 3}},
		{F8v(1.5), float64(1.5)},
		{Bool(true), true},
		{B(0x01, 0x02), []byte{0x01, 0x02}},
	}
	for _, c := range cases {
		got := c.item.Value()
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("Value() of %s = %#v, want %#v", c.item.Format, got, c.want)
		}
	}

	list := L(A("x"), U4v(7))
	gotList, ok := list.Value().([]any)
	if !ok || len(gotList) != 2 {
		t.Fatalf("list Value() = %#v, want a 2-element []any", list.Value())
	}
}

func TestCON220_Int64Helper(t *testing.T) {
	cases := []Item{U1v(5), U2v(5), U4v(5), U8v(5), I1v(5), I2v(5), I4v(5), I8v(5)}
	for _, it := range cases {
		got, ok := it.Int64()
		if !ok || got != 5 {
			t.Errorf("%s.Int64() = (%d, %v), want (5, true)", it.Format, got, ok)
		}
	}
	if _, ok := A("x").Int64(); ok {
		t.Error("ASCII item's Int64() should return ok=false")
	}
	if _, ok := (Item{Format: FormatU4}).Int64(); ok {
		t.Error("empty U4 item's Int64() should return ok=false")
	}
}

func TestCON220_DecodeErrors(t *testing.T) {
	if _, _, err := Decode(nil); err == nil {
		t.Error("expected error decoding empty data")
	}
	if _, _, err := Decode([]byte{0x00}); err == nil {
		t.Error("expected error: format byte with 0 length-bytes-count is invalid")
	}
	// FormatU4 (0x2C<<2|1 = 0xB1) claiming 4 bytes but only 2 present.
	if _, _, err := Decode([]byte{0xB1, 4, 0x00, 0x00}); err == nil {
		t.Error("expected error decoding a truncated U4 item")
	}
}
