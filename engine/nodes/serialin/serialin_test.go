package serialin

import "testing"

func TestCON290_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{}`)); err == nil {
		t.Error("expected error for missing port")
	}
	if _, err := New([]byte(`{"port":"/dev/ttyUSB0","framing":{"mode":"bogus"}}`)); err == nil {
		t.Error("expected error for invalid framing config")
	}
	n, err := New([]byte(`{"port":"/dev/ttyUSB0","framing":{"mode":"delimiter","delimiter":"\n"}}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	nd := n.(*node)
	if nd.cfg.BaudRate != 9600 || nd.cfg.DataBits != 8 {
		t.Errorf("defaults not applied: %+v", nd.cfg)
	}
}

func TestCON290_ParseParityAndStopBits(t *testing.T) {
	if _, err := parseParity("bogus"); err == nil {
		t.Error("expected error for unknown parity")
	}
	if _, err := parseStopBits("bogus"); err == nil {
		t.Error("expected error for unknown stopBits")
	}
	for _, p := range []string{"", "none", "odd", "even", "mark", "space"} {
		if _, err := parseParity(p); err != nil {
			t.Errorf("parseParity(%q): %v", p, err)
		}
	}
	for _, s := range []string{"", "1", "1.5", "2"} {
		if _, err := parseStopBits(s); err != nil {
			t.Errorf("parseStopBits(%q): %v", s, err)
		}
	}
}
