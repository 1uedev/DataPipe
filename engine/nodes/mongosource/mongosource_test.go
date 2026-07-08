package mongosource

import "encoding/json"

import "testing"

func TestCON520_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{"missing collection", Config{Mode: "once"}, true},
		{"once valid", Config{Mode: "once", Collection: "readings"}, false},
		{"periodic missing interval", Config{Mode: "periodic", Collection: "readings"}, true},
		{"periodic valid", Config{Mode: "periodic", Collection: "readings", IntervalMs: 1000}, false},
		{"unknown mode", Config{Mode: "bogus", Collection: "readings"}, true},
		{"unknown operation", Config{Mode: "once", Collection: "readings", Operation: "delete"}, true},
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

func TestCON520_DecodeFilterDefaultsToEmptyMatch(t *testing.T) {
	m, err := decodeFilter(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(m) != 0 {
		t.Errorf("expected empty filter, got %+v", m)
	}
}
