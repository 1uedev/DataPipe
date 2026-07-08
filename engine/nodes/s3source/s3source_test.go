package s3source

import (
	"encoding/json"
	"testing"
)

func TestCON400_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"once valid", Config{Mode: "once", Format: "json"}, false},
		{"periodic missing interval", Config{Mode: "periodic", Format: "json"}, true},
		{"periodic valid", Config{Mode: "periodic", Format: "json", IntervalMs: 1000}, false},
		{"unknown mode", Config{Mode: "bogus", Format: "json"}, true},
		{"unknown format", Config{Mode: "once", Format: "parquet"}, true},
		{"move missing moveToPrefix", Config{Mode: "once", Format: "json", PostAction: "move"}, true},
		{"move valid", Config{Mode: "once", Format: "json", PostAction: "move", MoveToPrefix: "archive/"}, false},
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
