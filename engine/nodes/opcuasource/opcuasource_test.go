package opcuasource

import (
	"encoding/json"
	"testing"

	"github.com/gopcua/opcua/ua"
)

func TestCON210_NewValidatesConfig(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantErr bool
	}{
		{"no nodes", `{"mode":"polled","intervalMs":1000,"nodes":[]}`, true},
		{"invalid nodeId", `{"mode":"polled","intervalMs":1000,"nodes":[{"name":"t","nodeId":"ns=abc;s=x"}]}`, true},
		{"polled missing interval", `{"mode":"polled","nodes":[{"name":"t","nodeId":"ns=2;s=Temp"}]}`, true},
		{"polled valid", `{"mode":"polled","intervalMs":1000,"nodes":[{"name":"t","nodeId":"ns=2;s=Temp"}]}`, false},
		{"subscribe valid", `{"mode":"subscribe","nodes":[{"name":"t","nodeId":"ns=2;s=Temp"}]}`, false},
		{"unknown mode", `{"mode":"bogus","nodes":[{"name":"t","nodeId":"ns=2;s=Temp"}]}`, true},
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

func TestCON210_ValueDatagramCarriesTagAndQuality(t *testing.T) {
	dv := &ua.DataValue{Value: ua.MustVariant(21.5), Status: ua.StatusOK}
	d := valueDatagram("temp", dv)
	if d.Payload.Value != 21.5 {
		t.Errorf("payload = %v", d.Payload.Value)
	}
	if d.Header.Tags["opcua.node"] != "temp" {
		t.Errorf("tags = %+v", d.Header.Tags)
	}
	if d.Header.Quality != "GOOD" {
		t.Errorf("quality = %v", d.Header.Quality)
	}
}
