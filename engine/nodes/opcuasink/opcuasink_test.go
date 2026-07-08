package opcuasink

import (
	"encoding/json"
	"testing"
)

func TestCON210_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"write missing nodeId", `{"mode":"write"}`, true},
		{"write invalid nodeId", `{"mode":"write","nodeId":"ns=abc;s=x"}`, true},
		{"write valid", `{"mode":"write","nodeId":"ns=2;s=Setpoint"}`, false},
		{"call missing ids", `{"mode":"call"}`, true},
		{"call valid", `{"mode":"call","objectNodeId":"ns=2;s=Obj","methodNodeId":"ns=2;s=Reset"}`, false},
		{"unknown mode", `{"mode":"bogus"}`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := New(json.RawMessage(c.raw))
			if (err != nil) != c.wantErr {
				t.Errorf("New(%s) error = %v, wantErr %v", c.raw, err, c.wantErr)
			}
		})
	}
}

func TestCON210_FieldOrWhole(t *testing.T) {
	if got := fieldOrWhole(42.0, ""); got != 42.0 {
		t.Errorf("fieldOrWhole(no field) = %v", got)
	}
	m := map[string]any{"setpoint": 21.5}
	if got := fieldOrWhole(m, "setpoint"); got != 21.5 {
		t.Errorf("fieldOrWhole(field) = %v", got)
	}
}
