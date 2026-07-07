package convert

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func newTestNode(t *testing.T, cfg Config) *node {
	t.Helper()
	raw, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	instance, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return instance.(*node)
}

func process(t *testing.T, n *node, value any) any {
	t.Helper()
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: value})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 output, got %d", len(results))
	}
	return results[0].Datagram.Payload.Value
}

func TestPROC120_NewRequiresValidMode(t *testing.T) {
	if _, err := New(json.RawMessage(`{"mode":"not-a-mode"}`)); err == nil {
		t.Fatal("expected an error for an unknown mode")
	}
}

func TestPROC120_Base64EncodeDecodeRoundTrip(t *testing.T) {
	enc := newTestNode(t, Config{Mode: "base64encode"})
	encoded := process(t, enc, "hello world")
	if encoded != base64.StdEncoding.EncodeToString([]byte("hello world")) {
		t.Errorf("encoded = %v", encoded)
	}

	dec := newTestNode(t, Config{Mode: "base64decode"})
	decoded := process(t, dec, encoded)
	if decoded != "hello world" {
		t.Errorf("decoded = %v", decoded)
	}
}

func TestPROC120_GzipCompressDecompressRoundTrip(t *testing.T) {
	comp := newTestNode(t, Config{Mode: "gzipCompress"})
	compressed := process(t, comp, "the quick brown fox")

	decomp := newTestNode(t, Config{Mode: "gzipDecompress"})
	out := process(t, decomp, compressed)
	if out != "the quick brown fox" {
		t.Errorf("decompressed = %v", out)
	}
}

func TestPROC120_EncodingConvertLatin1RoundTrip(t *testing.T) {
	toLatin1 := newTestNode(t, Config{Mode: "encodingConvert", Encoding: &EncodingConfig{From: "utf8", To: "latin1"}})
	latin1 := process(t, toLatin1, "café")

	toUTF8 := newTestNode(t, Config{Mode: "encodingConvert", Encoding: &EncodingConfig{From: "latin1", To: "utf8"}})
	back := process(t, toUTF8, latin1)
	if back != "café" {
		t.Errorf("round-tripped = %q, want %q", back, "café")
	}
}

func TestMAP150_EncodingConvertDropsUnrepresentableCharsWithError(t *testing.T) {
	n := newTestNode(t, Config{Mode: "encodingConvert", Encoding: &EncodingConfig{From: "utf8", To: "latin1"}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "emoji \U0001F600"})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an error rather than silently dropping the character")
	}
}

func TestPROC120_CSVToJSONWithHeader(t *testing.T) {
	n := newTestNode(t, Config{Mode: "csv2json"})
	out := process(t, n, "name,temp\nroom1,21.5\nroom2,19")
	arr, ok := out.([]any)
	if !ok || len(arr) != 2 {
		t.Fatalf("out = %+v", out)
	}
	row0 := arr[0].(map[string]any)
	if row0["name"] != "room1" || row0["temp"] != 21.5 {
		t.Errorf("row0 = %+v", row0)
	}
}

func TestPROC120_JSONToCSVWithHeader(t *testing.T) {
	n := newTestNode(t, Config{Mode: "json2csv"})
	out := process(t, n, []any{
		map[string]any{"name": "room1", "temp": 21.5},
		map[string]any{"name": "room2", "temp": 19.0},
	})
	want := "name,temp\nroom1,21.5\nroom2,19\n"
	if out != want {
		t.Errorf("out = %q, want %q", out, want)
	}
}

func TestPROC120_CSVJSONRoundTrip(t *testing.T) {
	toJSON := newTestNode(t, Config{Mode: "csv2json"})
	records := process(t, toJSON, "a,b\n1,2\n3,4")

	toCSV := newTestNode(t, Config{Mode: "json2csv"})
	csvOut := process(t, toCSV, records)
	if csvOut != "a,b\n1,2\n3,4\n" {
		t.Errorf("round-tripped csv = %q", csvOut)
	}
}

func TestPROC120_JSONToXMLBasicStructure(t *testing.T) {
	n := newTestNode(t, Config{Mode: "json2xml"})
	out := process(t, n, map[string]any{"name": "room1", "temp": 21.5})
	s := out.(string)
	if !containsAll(s, "<root>", "<name>room1</name>", "<temp>21.5</temp>", "</root>") {
		t.Errorf("xml = %q", s)
	}
}

func TestPROC120_JSONToXMLArrayBecomesRepeatedElements(t *testing.T) {
	n := newTestNode(t, Config{Mode: "json2xml", XML: XMLConfig{RootElement: "readings"}})
	out := process(t, n, map[string]any{"reading": []any{"a", "b"}})
	s := out.(string)
	if !containsAll(s, "<readings>", "<reading>a</reading>", "<reading>b</reading>", "</readings>") {
		t.Errorf("xml = %q", s)
	}
}

func TestPROC120_JSONToXMLBareArrayUsesItemElement(t *testing.T) {
	n := newTestNode(t, Config{Mode: "json2xml", XML: XMLConfig{RootElement: "readings", ItemElement: "reading"}})
	out := process(t, n, []any{"a", "b"})
	s := out.(string)
	if !containsAll(s, "<readings>", "<reading>a</reading>", "<reading>b</reading>", "</readings>") {
		t.Errorf("xml = %q", s)
	}
}

func TestPROC120_XMLToJSONBasicStructure(t *testing.T) {
	n := newTestNode(t, Config{Mode: "xml2json"})
	out := process(t, n, "<root><name>room1</name><temp>21.5</temp></root>")
	m := out.(map[string]any)
	if m["name"] != "room1" || m["temp"] != 21.5 {
		t.Errorf("out = %+v", m)
	}
}

func TestPROC120_XMLJSONRoundTrip(t *testing.T) {
	toXML := newTestNode(t, Config{Mode: "json2xml"})
	xmlOut := process(t, toXML, map[string]any{"name": "room1", "temp": 21.5})

	toJSON := newTestNode(t, Config{Mode: "xml2json"})
	back := process(t, toJSON, xmlOut)
	m := back.(map[string]any)
	if m["name"] != "room1" || m["temp"] != 21.5 {
		t.Errorf("round-tripped = %+v", m)
	}
}

func TestPROC120_BinaryParseExtractsScalarFields(t *testing.T) {
	// uint16 big-endian 0x0102 at offset 0, int8 -1 at offset 2, float32 at offset 3.
	raw := []byte{0x01, 0x02, 0xFF, 0, 0, 0, 0}
	// Encode a float32(1.5) big-endian at offset 3: 0x3FC00000
	raw[3], raw[4], raw[5], raw[6] = 0x3F, 0xC0, 0x00, 0x00

	n := newTestNode(t, Config{Mode: "binaryParse", Fields: []Field{
		{Name: "a", Offset: 0, Type: "uint16"},
		{Name: "b", Offset: 2, Type: "int8"},
		{Name: "c", Offset: 3, Type: "float32"},
	}})
	out := process(t, n, base64.StdEncoding.EncodeToString(raw))
	m := out.(map[string]any)
	if m["a"] != float64(0x0102) {
		t.Errorf("a = %v", m["a"])
	}
	if m["b"] != float64(-1) {
		t.Errorf("b = %v", m["b"])
	}
	if m["c"] != float64(1.5) {
		t.Errorf("c = %v", m["c"])
	}
}

func TestPROC120_BinarySerializeParseRoundTrip(t *testing.T) {
	fields := []Field{
		{Name: "id", Offset: 0, Type: "uint32"},
		{Name: "value", Offset: 4, Type: "float64"},
		{Name: "code", Offset: 12, Type: "string", Length: 4},
	}
	serialize := newTestNode(t, Config{Mode: "binarySerialize", Fields: fields})
	b64 := process(t, serialize, map[string]any{"id": 42.0, "value": 3.25, "code": "OKOK"})

	parse := newTestNode(t, Config{Mode: "binaryParse", Fields: fields})
	out := process(t, parse, b64)
	m := out.(map[string]any)
	if m["id"] != float64(42) || m["value"] != 3.25 || m["code"] != "OKOK" {
		t.Errorf("round-tripped = %+v", m)
	}
}

func TestPROC120_BinaryParseRejectsOutOfBoundsOffset(t *testing.T) {
	n := newTestNode(t, Config{Mode: "binaryParse", Fields: []Field{{Name: "a", Offset: 10, Type: "uint32"}}})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: base64.StdEncoding.EncodeToString([]byte{1, 2})})
	if _, err := n.Process(context.Background(), in); err == nil {
		t.Fatal("expected an out-of-bounds error")
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
