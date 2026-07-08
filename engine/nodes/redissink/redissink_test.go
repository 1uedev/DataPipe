package redissink

import (
	"encoding/json"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestSNK200_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"set missing key", Config{Mode: "set"}, true},
		{"set valid", Config{Mode: "set", Key: "k"}, false},
		{"publish missing channel", Config{Mode: "publish"}, true},
		{"publish valid", Config{Mode: "publish", Channel: "events"}, false},
		{"streamAdd missing key", Config{Mode: "streamAdd"}, true},
		{"streamAdd valid", Config{Mode: "streamAdd", StreamKey: "readings"}, false},
		{"unknown mode", Config{Mode: "bogus"}, true},
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

func TestSNK200_StringValueEncodesNonStringAsJSON(t *testing.T) {
	n := &node{cfg: Config{Mode: "publish", Channel: "c"}}
	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"a": 1}})
	got := n.stringValue(d)
	if got != `{"a":1}` {
		t.Errorf("stringValue = %q, want %q", got, `{"a":1}`)
	}

	d2 := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: "plain"})
	if got := n.stringValue(d2); got != "plain" {
		t.Errorf("stringValue = %q, want %q", got, "plain")
	}
}

func TestSNK200_StringValueExtractsValueField(t *testing.T) {
	n := &node{cfg: Config{Mode: "set", Key: "k", ValueField: "temp"}}
	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"temp": "21.5"}})
	if got := n.stringValue(d); got != "21.5" {
		t.Errorf("stringValue = %q, want %q", got, "21.5")
	}
}
