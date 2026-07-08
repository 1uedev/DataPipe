package modbusshared

import (
	"encoding/binary"
	"testing"
)

func regBytes(words ...uint16) []byte {
	b := make([]byte, len(words)*2)
	for i, w := range words {
		binary.BigEndian.PutUint16(b[i*2:], w)
	}
	return b
}

func TestCON230_DecodeUint16AndInt16(t *testing.T) {
	raw := regBytes(1234, 0xFFFE) // 0xFFFE = -2 as int16
	out, err := DecodeRegisters(raw, []Field{
		{Name: "u", Register: 0, Type: "uint16"},
		{Name: "i", Register: 1, Type: "int16"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out["u"] != float64(1234) || out["i"] != float64(-2) {
		t.Errorf("out = %+v", out)
	}
}

func TestCON230_DecodeUint32BigEndianWordOrder(t *testing.T) {
	raw := regBytes(0x0001, 0x0000) // high word first = 0x00010000 = 65536
	out, err := DecodeRegisters(raw, []Field{{Name: "v", Register: 0, Type: "uint32"}})
	if err != nil {
		t.Fatal(err)
	}
	if out["v"] != float64(65536) {
		t.Errorf("v = %v, want 65536", out["v"])
	}
}

func TestCON230_DecodeUint32LittleWordOrder(t *testing.T) {
	raw := regBytes(0x0000, 0x0001) // low word first; wordOrder little swaps to high-first
	out, err := DecodeRegisters(raw, []Field{{Name: "v", Register: 0, Type: "uint32", WordOrder: "little"}})
	if err != nil {
		t.Fatal(err)
	}
	if out["v"] != float64(65536) {
		t.Errorf("v = %v, want 65536", out["v"])
	}
}

func TestCON230_DecodeFloat32RoundTripsWithEncode(t *testing.T) {
	f := Field{Name: "temp", Register: 0, Type: "float32"}
	words, err := EncodeField(f, 21.5)
	if err != nil {
		t.Fatal(err)
	}
	raw := regBytes(words...)
	out, err := DecodeRegisters(raw, []Field{f})
	if err != nil {
		t.Fatal(err)
	}
	if got := out["temp"].(float64); got < 21.49 || got > 21.51 {
		t.Errorf("temp = %v, want ~21.5", got)
	}
}

func TestCON230_DecodeStringTrimsTrailingNulls(t *testing.T) {
	raw := regBytes(uint16('H')<<8|uint16('I'), 0x0000)
	out, err := DecodeRegisters(raw, []Field{{Name: "s", Register: 0, Type: "string", Length: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if out["s"] != "HI" {
		t.Errorf("s = %q, want %q", out["s"], "HI")
	}
}

func TestCON230_DecodeBitField(t *testing.T) {
	raw := regBytes(0b0000_0000_0000_0101) // bits 0 and 2 set
	out, err := DecodeRegisters(raw, []Field{
		{Name: "b0", Register: 0, Type: "bit", BitOffset: 0},
		{Name: "b1", Register: 0, Type: "bit", BitOffset: 1},
		{Name: "b2", Register: 0, Type: "bit", BitOffset: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if out["b0"] != true || out["b1"] != false || out["b2"] != true {
		t.Errorf("out = %+v", out)
	}
}

func TestCON230_DecodeUint64RoundTripsWithEncode(t *testing.T) {
	f := Field{Name: "counter", Register: 0, Type: "uint64"}
	words, err := EncodeField(f, float64(4294967296)) // 2^32
	if err != nil {
		t.Fatal(err)
	}
	raw := regBytes(words...)
	out, err := DecodeRegisters(raw, []Field{f})
	if err != nil {
		t.Fatal(err)
	}
	if out["counter"] != float64(4294967296) {
		t.Errorf("counter = %v", out["counter"])
	}
}

func TestCON230_DecodeRejectsOutOfRangeRegister(t *testing.T) {
	raw := regBytes(1, 2)
	if _, err := DecodeRegisters(raw, []Field{{Name: "x", Register: 5, Type: "uint16"}}); err == nil {
		t.Error("expected an out-of-range error")
	}
}
