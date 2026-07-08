package kafkain

import (
	"encoding/json"
	"testing"
)

func TestCON260_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"missing topic", Config{GroupID: "g"}, true},
		{"missing groupID", Config{Topic: "t"}, true},
		{"valid defaults", Config{Topic: "t", GroupID: "g"}, false},
		{"valid earliest", Config{Topic: "t", GroupID: "g", StartOffset: "earliest"}, false},
		{"invalid startOffset", Config{Topic: "t", GroupID: "g", StartOffset: "committed"}, true},
		{"invalid valueFormat", Config{Topic: "t", GroupID: "g", ValueFormat: "avro"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw, err := json.Marshal(c.cfg)
			if err != nil {
				t.Fatal(err)
			}
			_, err = New(raw)
			if (err != nil) != c.wantErr {
				t.Errorf("New(%+v) error = %v, wantErr %v", c.cfg, err, c.wantErr)
			}
		})
	}
}

func TestCON260_DecodeValueFormats(t *testing.T) {
	n := &node{cfg: Config{ValueFormat: "string"}}
	v, err := n.decodeValue([]byte("hello"))
	if err != nil || v != "hello" {
		t.Errorf("decodeValue(string) = %v, %v", v, err)
	}

	n = &node{cfg: Config{ValueFormat: "json"}}
	v, err = n.decodeValue([]byte(`{"a":1}`))
	if err != nil {
		t.Fatal(err)
	}
	m, ok := v.(map[string]any)
	if !ok || m["a"] != float64(1) {
		t.Errorf("decodeValue(json) = %+v", v)
	}

	n = &node{cfg: Config{ValueFormat: "binary"}}
	v, err = n.decodeValue([]byte{0x01, 0x02})
	if err != nil || v != "AQI=" {
		t.Errorf("decodeValue(binary) = %v, %v", v, err)
	}
}
