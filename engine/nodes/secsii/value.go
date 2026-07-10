package secsii

// Value converts it into a plain Go value suitable for a datagram payload:
// a []any for a List (recursively converted), a string for ASCII/JIS8, a
// []byte for Binary, and for Boolean and every numeric format a single
// scalar when the item holds exactly one value (the common case for a GEM
// status variable or equipment constant) or a slice otherwise.
func (it Item) Value() any {
	switch it.Format {
	case FormatList:
		out := make([]any, len(it.List))
		for i, sub := range it.List {
			out[i] = sub.Value()
		}
		return out
	case FormatBinary:
		return it.Bytes
	case FormatBoolean:
		return scalarOrSlice(it.Bools)
	case FormatASCII, FormatJIS8:
		return it.Str
	case FormatI1:
		return scalarOrSlice(it.I1)
	case FormatI2:
		return scalarOrSlice(it.I2)
	case FormatI4:
		return scalarOrSlice(it.I4)
	case FormatI8:
		return scalarOrSlice(it.I8)
	case FormatU1:
		return scalarOrSlice(it.U1)
	case FormatU2:
		return scalarOrSlice(it.U2)
	case FormatU4:
		return scalarOrSlice(it.U4)
	case FormatU8:
		return scalarOrSlice(it.U8)
	case FormatF4:
		return scalarOrSlice(it.F4)
	case FormatF8:
		return scalarOrSlice(it.F8)
	default:
		return nil
	}
}

func scalarOrSlice[T any](v []T) any {
	if len(v) == 1 {
		return v[0]
	}
	return v
}

// Int64 returns a numeric item's first element as an int64 and true, or
// (0, false) if it is not a numeric item or holds no elements. Used to
// read an equipment-chosen CEID/RPTID/SVID identifier generically without
// caring which of the 8 numeric widths the equipment encoded it as.
func (it Item) Int64() (int64, bool) {
	switch it.Format {
	case FormatI1:
		if len(it.I1) == 0 {
			return 0, false
		}
		return int64(it.I1[0]), true
	case FormatI2:
		if len(it.I2) == 0 {
			return 0, false
		}
		return int64(it.I2[0]), true
	case FormatI4:
		if len(it.I4) == 0 {
			return 0, false
		}
		return int64(it.I4[0]), true
	case FormatI8:
		if len(it.I8) == 0 {
			return 0, false
		}
		return it.I8[0], true
	case FormatU1:
		if len(it.U1) == 0 {
			return 0, false
		}
		return int64(it.U1[0]), true
	case FormatU2:
		if len(it.U2) == 0 {
			return 0, false
		}
		return int64(it.U2[0]), true
	case FormatU4:
		if len(it.U4) == 0 {
			return 0, false
		}
		return int64(it.U4[0]), true
	case FormatU8:
		if len(it.U8) == 0 {
			return 0, false
		}
		return int64(it.U8[0]), true
	default:
		return 0, false
	}
}

// Text returns an ASCII/JIS8 item's string value and true, or ("", false)
// otherwise. Named Text rather than String to avoid implying Item
// satisfies fmt.Stringer.
func (it Item) Text() (string, bool) {
	switch it.Format {
	case FormatASCII, FormatJIS8:
		return it.Str, true
	default:
		return "", false
	}
}
