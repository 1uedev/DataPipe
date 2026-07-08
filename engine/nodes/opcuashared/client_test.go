package opcuashared

import (
	"testing"

	"github.com/gopcua/opcua/ua"

	"github.com/1uedev/DataPipe/engine/datagram"
)

func TestCON210_QualityOfMapsSeverityBits(t *testing.T) {
	cases := []struct {
		name string
		code ua.StatusCode
		want datagram.Quality
	}{
		{"good", ua.StatusOK, datagram.QualityGood},
		{"uncertain", ua.StatusCode(0x40000000), datagram.QualityUncertain},
		{"bad", ua.StatusCode(0x80000000), datagram.QualityBad},
		{"bad with subcode", ua.StatusCode(0x80AB0000), datagram.QualityBad},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := QualityOf(c.code); got != c.want {
				t.Errorf("QualityOf(%v) = %v, want %v", c.code, got, c.want)
			}
		})
	}
}

func TestCON210_OrDefault(t *testing.T) {
	if got := orDefault("", "None"); got != "None" {
		t.Errorf("orDefault(\"\", \"None\") = %q", got)
	}
	if got := orDefault("Basic256Sha256", "None"); got != "Basic256Sha256" {
		t.Errorf("orDefault preserved value = %q", got)
	}
}
