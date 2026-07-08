package wsshared

import "testing"

func TestCON320_HubForReturnsSameHubForSamePath(t *testing.T) {
	a := HubFor("/ws/a")
	b := HubFor("/ws/a")
	if a != b {
		t.Error("expected the same Hub instance for the same path")
	}
	c := HubFor("/ws/b")
	if a == c {
		t.Error("expected a different Hub instance for a different path")
	}
}
