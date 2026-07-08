// decode.go implements CON-230's "data type decoding (int16/32/64, float,
// string, bit fields, byte/word order options)": Field describes one value
// within a block of registers read from a Modbus device, and
// DecodeRegisters/EncodeField convert between that block's raw bytes and
// Go values.
package modbusshared

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Field describes one decoded value within a polled register block.
// Register is a 0-based register offset within the block (not an absolute
// Modbus address); e.g. Register 2 in a block starting at address 100 is
// device register 102.
type Field struct {
	Name      string `json:"name"`
	Register  int    `json:"register"`
	Type      string `json:"type"`                // "uint16"|"int16"|"uint32"|"int32"|"uint64"|"int64"|"float32"|"float64"|"string"|"bit"
	Length    int    `json:"length,omitempty"`    // register count, type "string" (2 chars/register)
	BitOffset int    `json:"bitOffset,omitempty"` // 0-15, type "bit" (bit position within Register's 16-bit word)
	WordOrder string `json:"wordOrder,omitempty"` // "big" (default, high word first) | "little", multi-register types only
}

func registerCount(f Field) (int, error) {
	switch f.Type {
	case "uint16", "int16", "bit":
		return 1, nil
	case "uint32", "int32", "float32":
		return 2, nil
	case "uint64", "int64", "float64":
		return 4, nil
	case "string":
		if f.Length <= 0 {
			return 0, fmt.Errorf("field %q: length (register count) is required for type \"string\"", f.Name)
		}
		return f.Length, nil
	default:
		return 0, fmt.Errorf("field %q: unknown type %q", f.Name, f.Type)
	}
}

// registerWords extracts f's registers from raw (2 bytes/register, as
// returned by the Modbus client) as big-endian uint16 words in device
// order, then reorders multi-register words per WordOrder so word[0] is
// always the most-significant.
func registerWords(raw []byte, f Field, n int) ([]uint16, error) {
	start := f.Register * 2
	end := start + n*2
	if start < 0 || end > len(raw) {
		return nil, fmt.Errorf("field %q: register %d+%d exceeds the polled block (%d registers)", f.Name, f.Register, n, len(raw)/2)
	}
	words := make([]uint16, n)
	for i := 0; i < n; i++ {
		words[i] = binary.BigEndian.Uint16(raw[start+i*2 : start+i*2+2])
	}
	if f.WordOrder == "little" {
		for i, j := 0, len(words)-1; i < j; i, j = i+1, j-1 {
			words[i], words[j] = words[j], words[i]
		}
	}
	return words, nil
}

// DecodeRegisters decodes every field in fields out of raw (the byte block
// a single ReadHoldingRegisters/ReadInputRegisters call returned).
func DecodeRegisters(raw []byte, fields []Field) (map[string]any, error) {
	out := make(map[string]any, len(fields))
	for _, f := range fields {
		if f.Type == "bit" {
			words, err := registerWords(raw, f, 1)
			if err != nil {
				return nil, err
			}
			if f.BitOffset < 0 || f.BitOffset > 15 {
				return nil, fmt.Errorf("field %q: bitOffset must be 0-15, got %d", f.Name, f.BitOffset)
			}
			out[f.Name] = words[0]&(1<<uint(f.BitOffset)) != 0
			continue
		}

		n, err := registerCount(f)
		if err != nil {
			return nil, err
		}
		words, err := registerWords(raw, f, n)
		if err != nil {
			return nil, err
		}
		switch f.Type {
		case "uint16":
			out[f.Name] = float64(words[0])
		case "int16":
			out[f.Name] = float64(int16(words[0]))
		case "uint32":
			out[f.Name] = float64(uint32(words[0])<<16 | uint32(words[1]))
		case "int32":
			out[f.Name] = float64(int32(uint32(words[0])<<16 | uint32(words[1])))
		case "uint64":
			out[f.Name] = float64(words64(words))
		case "int64":
			out[f.Name] = float64(int64(words64(words)))
		case "float32":
			out[f.Name] = float64(math.Float32frombits(uint32(words[0])<<16 | uint32(words[1])))
		case "float64":
			out[f.Name] = math.Float64frombits(words64(words))
		case "string":
			b := make([]byte, 0, n*2)
			for _, w := range words {
				b = append(b, byte(w>>8), byte(w))
			}
			out[f.Name] = trimNulls(b)
		}
	}
	return out, nil
}

func words64(words []uint16) uint64 {
	var v uint64
	for _, w := range words {
		v = v<<16 | uint64(w)
	}
	return v
}

func trimNulls(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
}

// EncodeField converts a single decoded value back into device registers
// (big-endian words in device order, WordOrder-reversed if configured),
// for "modbus-sink" writes of multi-register types.
func EncodeField(f Field, value any) ([]uint16, error) {
	if f.Type == "string" {
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field %q: expected a string value", f.Name)
		}
		n, err := registerCount(f)
		if err != nil {
			return nil, err
		}
		b := make([]byte, n*2)
		copy(b, s)
		words := make([]uint16, n)
		for i := range words {
			words[i] = binary.BigEndian.Uint16(b[i*2 : i*2+2])
		}
		return words, nil
	}

	n, err := toFloat(value)
	if err != nil {
		return nil, fmt.Errorf("field %q: expected a numeric value: %w", f.Name, err)
	}
	var words []uint16
	switch f.Type {
	case "uint16", "int16":
		words = []uint16{uint16(int64(n))}
	case "uint32", "int32":
		v := uint32(int64(n))
		words = []uint16{uint16(v >> 16), uint16(v)}
	case "uint64", "int64":
		v := uint64(int64(n))
		words = []uint16{uint16(v >> 48), uint16(v >> 32), uint16(v >> 16), uint16(v)}
	case "float32":
		v := math.Float32bits(float32(n))
		words = []uint16{uint16(v >> 16), uint16(v)}
	case "float64":
		v := math.Float64bits(n)
		words = []uint16{uint16(v >> 48), uint16(v >> 32), uint16(v >> 16), uint16(v)}
	default:
		return nil, fmt.Errorf("field %q: unknown type %q", f.Name, f.Type)
	}
	if f.WordOrder == "little" {
		for i, j := 0, len(words)-1; i < j; i, j = i+1, j-1 {
			words[i], words[j] = words[j], words[i]
		}
	}
	return words, nil
}

func toFloat(v any) (float64, error) {
	switch t := v.(type) {
	case float64:
		return t, nil
	case int:
		return float64(t), nil
	case int64:
		return float64(t), nil
	case bool:
		if t {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", v)
	}
}
