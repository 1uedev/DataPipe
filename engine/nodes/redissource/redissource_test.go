package redissource

import (
	"encoding/json"
	"testing"
)

func TestCON520_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"poll missing key", Config{Mode: "poll", IntervalMs: 1000}, true},
		{"poll missing interval", Config{Mode: "poll", Key: "k"}, true},
		{"poll valid key", Config{Mode: "poll", Key: "k", IntervalMs: 1000}, false},
		{"poll valid pattern", Config{Mode: "poll", KeyPattern: "sensor:*", IntervalMs: 1000}, false},
		{"subscribe missing channel", Config{Mode: "subscribe"}, true},
		{"subscribe valid", Config{Mode: "subscribe", Channel: "events"}, false},
		{"stream missing key", Config{Mode: "stream"}, true},
		{"stream valid", Config{Mode: "stream", StreamKey: "readings"}, false},
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
