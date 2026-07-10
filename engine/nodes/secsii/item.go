// Package secsii implements the SECS-II (SEMI E5) item encoding used as an
// HSMS (engine/nodes/hsms) data message's body: a self-describing,
// recursively nestable tree of typed items (list, binary, boolean, ASCII,
// and 8 numeric widths), each prefixed by a format byte and 1-3 length
// bytes.
package secsii

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Format is a SECS-II item's format code (SEMI E5 §7.2's format byte,
// shifted right by 2 — the low 2 bits of the wire byte separately encode
// how many length bytes follow, computed automatically by Encode).
type Format byte

const (
	FormatList    Format = 0x00
	FormatBinary  Format = 0x08
	FormatBoolean Format = 0x09
	FormatASCII   Format = 0x10
	FormatJIS8    Format = 0x11
	FormatI8      Format = 0x18
	FormatI1      Format = 0x19
	FormatI2      Format = 0x1A
	FormatI4      Format = 0x1C
	FormatF8      Format = 0x20
	FormatF4      Format = 0x24
	FormatU8      Format = 0x28
	FormatU1      Format = 0x29
	FormatU2      Format = 0x2A
	FormatU4      Format = 0x2C
)

func (f Format) String() string {
	switch f {
	case FormatList:
		return "List"
	case FormatBinary:
		return "Binary"
	case FormatBoolean:
		return "Boolean"
	case FormatASCII:
		return "ASCII"
	case FormatJIS8:
		return "JIS8"
	case FormatI8:
		return "I8"
	case FormatI1:
		return "I1"
	case FormatI2:
		return "I2"
	case FormatI4:
		return "I4"
	case FormatF8:
		return "F8"
	case FormatF4:
		return "F4"
	case FormatU8:
		return "U8"
	case FormatU1:
		return "U1"
	case FormatU2:
		return "U2"
	case FormatU4:
		return "U4"
	default:
		return fmt.Sprintf("Format(%#x)", byte(f))
	}
}

// Item is one SECS-II item. Exactly the field(s) matching Format are
// populated; the rest are zero. Build one with the L/A/B/Bool/I*/U*/F*
// constructors rather than setting fields directly.
type Item struct {
	Format Format

	List  []Item
	Bytes []byte
	Bools []bool
	Str   string
	I1    []int8
	I2    []int16
	I4    []int32
	I8    []int64
	U1    []uint8
	U2    []uint16
	U4    []uint32
	U8    []uint64
	F4    []float32
	F8    []float64
}

func L(items ...Item) Item  { return Item{Format: FormatList, List: items} }
func A(s string) Item       { return Item{Format: FormatASCII, Str: s} }
func JIS8(s string) Item    { return Item{Format: FormatJIS8, Str: s} }
func B(b ...byte) Item      { return Item{Format: FormatBinary, Bytes: b} }
func Bool(v ...bool) Item   { return Item{Format: FormatBoolean, Bools: v} }
func I1v(v ...int8) Item    { return Item{Format: FormatI1, I1: v} }
func I2v(v ...int16) Item   { return Item{Format: FormatI2, I2: v} }
func I4v(v ...int32) Item   { return Item{Format: FormatI4, I4: v} }
func I8v(v ...int64) Item   { return Item{Format: FormatI8, I8: v} }
func U1v(v ...uint8) Item   { return Item{Format: FormatU1, U1: v} }
func U2v(v ...uint16) Item  { return Item{Format: FormatU2, U2: v} }
func U4v(v ...uint32) Item  { return Item{Format: FormatU4, U4: v} }
func U8v(v ...uint64) Item  { return Item{Format: FormatU8, U8: v} }
func F4v(v ...float32) Item { return Item{Format: FormatF4, F4: v} }
func F8v(v ...float64) Item { return Item{Format: FormatF8, F8: v} }

// count returns an item's declared element count: number of sub-items for
// a List, number of bytes of encoded data for everything else.
func (it Item) count() int {
	switch it.Format {
	case FormatList:
		return len(it.List)
	case FormatBinary:
		return len(it.Bytes)
	case FormatBoolean:
		return len(it.Bools)
	case FormatASCII, FormatJIS8:
		return len(it.Str)
	case FormatI1, FormatU1:
		return len(it.I1) + len(it.U1)
	case FormatI2:
		return len(it.I2) * 2
	case FormatU2:
		return len(it.U2) * 2
	case FormatI4:
		return len(it.I4) * 4
	case FormatU4:
		return len(it.U4) * 4
	case FormatF4:
		return len(it.F4) * 4
	case FormatI8:
		return len(it.I8) * 8
	case FormatU8:
		return len(it.U8) * 8
	case FormatF8:
		return len(it.F8) * 8
	default:
		return 0
	}
}

func lengthBytesNeeded(n int) byte {
	switch {
	case n <= 0xFF:
		return 1
	case n <= 0xFFFF:
		return 2
	default:
		return 3
	}
}

// Encode serializes it as a SECS-II item: format byte, 1-3 length bytes,
// then the data (recursively, for a List).
func Encode(it Item) []byte {
	body := body(it)
	n := it.count()
	lb := lengthBytesNeeded(n)
	fb := byte(it.Format)<<2 | lb

	out := make([]byte, 0, 1+int(lb)+len(body))
	out = append(out, fb)
	switch lb {
	case 1:
		out = append(out, byte(n))
	case 2:
		var b [2]byte
		binary.BigEndian.PutUint16(b[:], uint16(n))
		out = append(out, b[:]...)
	case 3:
		out = append(out, byte(n>>16), byte(n>>8), byte(n))
	}
	return append(out, body...)
}

func body(it Item) []byte {
	switch it.Format {
	case FormatList:
		var b []byte
		for _, sub := range it.List {
			b = append(b, Encode(sub)...)
		}
		return b
	case FormatBinary:
		return it.Bytes
	case FormatBoolean:
		b := make([]byte, len(it.Bools))
		for i, v := range it.Bools {
			if v {
				b[i] = 1
			}
		}
		return b
	case FormatASCII, FormatJIS8:
		return []byte(it.Str)
	case FormatI1:
		b := make([]byte, len(it.I1))
		for i, v := range it.I1 {
			b[i] = byte(v)
		}
		return b
	case FormatU1:
		return it.U1
	case FormatI2:
		b := make([]byte, len(it.I2)*2)
		for i, v := range it.I2 {
			binary.BigEndian.PutUint16(b[i*2:], uint16(v))
		}
		return b
	case FormatU2:
		b := make([]byte, len(it.U2)*2)
		for i, v := range it.U2 {
			binary.BigEndian.PutUint16(b[i*2:], v)
		}
		return b
	case FormatI4:
		b := make([]byte, len(it.I4)*4)
		for i, v := range it.I4 {
			binary.BigEndian.PutUint32(b[i*4:], uint32(v))
		}
		return b
	case FormatU4:
		b := make([]byte, len(it.U4)*4)
		for i, v := range it.U4 {
			binary.BigEndian.PutUint32(b[i*4:], v)
		}
		return b
	case FormatF4:
		b := make([]byte, len(it.F4)*4)
		for i, v := range it.F4 {
			binary.BigEndian.PutUint32(b[i*4:], math.Float32bits(v))
		}
		return b
	case FormatI8:
		b := make([]byte, len(it.I8)*8)
		for i, v := range it.I8 {
			binary.BigEndian.PutUint64(b[i*8:], uint64(v))
		}
		return b
	case FormatU8:
		b := make([]byte, len(it.U8)*8)
		for i, v := range it.U8 {
			binary.BigEndian.PutUint64(b[i*8:], v)
		}
		return b
	case FormatF8:
		b := make([]byte, len(it.F8)*8)
		for i, v := range it.F8 {
			binary.BigEndian.PutUint64(b[i*8:], math.Float64bits(v))
		}
		return b
	default:
		return nil
	}
}

var errShort = errors.New("secsii: item data shorter than its declared length")

// Decode parses one SECS-II item (recursively, for a List) from the front
// of data, returning the item and the unconsumed remainder.
func Decode(data []byte) (Item, []byte, error) {
	if len(data) < 1 {
		return Item{}, nil, errors.New("secsii: empty item data")
	}
	fb := data[0]
	format := Format(fb >> 2)
	lb := fb & 0x03
	if lb == 0 {
		return Item{}, nil, errors.New("secsii: invalid length-byte count 0 in format byte")
	}
	if len(data) < 1+int(lb) {
		return Item{}, nil, errors.New("secsii: truncated length bytes")
	}
	var n int
	switch lb {
	case 1:
		n = int(data[1])
	case 2:
		n = int(binary.BigEndian.Uint16(data[1:3]))
	case 3:
		n = int(data[1])<<16 | int(data[2])<<8 | int(data[3])
	}
	rest := data[1+int(lb):]

	switch format {
	case FormatList:
		items := make([]Item, 0, n)
		remaining := rest
		for i := 0; i < n; i++ {
			item, r, err := Decode(remaining)
			if err != nil {
				return Item{}, nil, fmt.Errorf("secsii: list element %d: %w", i, err)
			}
			items = append(items, item)
			remaining = r
		}
		return Item{Format: FormatList, List: items}, remaining, nil
	case FormatBinary:
		if len(rest) < n {
			return Item{}, nil, errShort
		}
		return Item{Format: FormatBinary, Bytes: append([]byte{}, rest[:n]...)}, rest[n:], nil
	case FormatBoolean:
		if len(rest) < n {
			return Item{}, nil, errShort
		}
		bools := make([]bool, n)
		for i := range bools {
			bools[i] = rest[i] != 0
		}
		return Item{Format: FormatBoolean, Bools: bools}, rest[n:], nil
	case FormatASCII, FormatJIS8:
		if len(rest) < n {
			return Item{}, nil, errShort
		}
		return Item{Format: format, Str: string(rest[:n])}, rest[n:], nil
	case FormatI1:
		if len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]int8, n)
		for i := range vals {
			vals[i] = int8(rest[i])
		}
		return Item{Format: FormatI1, I1: vals}, rest[n:], nil
	case FormatU1:
		if len(rest) < n {
			return Item{}, nil, errShort
		}
		return Item{Format: FormatU1, U1: append([]byte{}, rest[:n]...)}, rest[n:], nil
	case FormatI2:
		if n%2 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]int16, n/2)
		for i := range vals {
			vals[i] = int16(binary.BigEndian.Uint16(rest[i*2:]))
		}
		return Item{Format: FormatI2, I2: vals}, rest[n:], nil
	case FormatU2:
		if n%2 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]uint16, n/2)
		for i := range vals {
			vals[i] = binary.BigEndian.Uint16(rest[i*2:])
		}
		return Item{Format: FormatU2, U2: vals}, rest[n:], nil
	case FormatI4:
		if n%4 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]int32, n/4)
		for i := range vals {
			vals[i] = int32(binary.BigEndian.Uint32(rest[i*4:]))
		}
		return Item{Format: FormatI4, I4: vals}, rest[n:], nil
	case FormatU4:
		if n%4 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]uint32, n/4)
		for i := range vals {
			vals[i] = binary.BigEndian.Uint32(rest[i*4:])
		}
		return Item{Format: FormatU4, U4: vals}, rest[n:], nil
	case FormatF4:
		if n%4 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]float32, n/4)
		for i := range vals {
			vals[i] = math.Float32frombits(binary.BigEndian.Uint32(rest[i*4:]))
		}
		return Item{Format: FormatF4, F4: vals}, rest[n:], nil
	case FormatI8:
		if n%8 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]int64, n/8)
		for i := range vals {
			vals[i] = int64(binary.BigEndian.Uint64(rest[i*8:]))
		}
		return Item{Format: FormatI8, I8: vals}, rest[n:], nil
	case FormatU8:
		if n%8 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]uint64, n/8)
		for i := range vals {
			vals[i] = binary.BigEndian.Uint64(rest[i*8:])
		}
		return Item{Format: FormatU8, U8: vals}, rest[n:], nil
	case FormatF8:
		if n%8 != 0 || len(rest) < n {
			return Item{}, nil, errShort
		}
		vals := make([]float64, n/8)
		for i := range vals {
			vals[i] = math.Float64frombits(binary.BigEndian.Uint64(rest[i*8:]))
		}
		return Item{Format: FormatF8, F8: vals}, rest[n:], nil
	default:
		return Item{}, nil, fmt.Errorf("secsii: unknown format code %#x", byte(format))
	}
}
