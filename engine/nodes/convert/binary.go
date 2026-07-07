// binaryParse/binarySerialize implement PROC-120's "binary parse/serialize
// (configurable binary layout: offsets, types, endianness — for CON-290 raw
// frames)" for a documented subset: fixed-offset scalar fields and
// fixed-length strings. Bit-field extraction is not implemented yet (see
// TODO.md) — every field must be byte-aligned.
package convert

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"math"
)

func fieldByteOrder(f Field) binary.ByteOrder {
	if f.Endianness == "little" {
		return binary.LittleEndian
	}
	return binary.BigEndian
}

func fieldSize(f Field) (int, error) {
	switch f.Type {
	case "uint8", "int8":
		return 1, nil
	case "uint16", "int16":
		return 2, nil
	case "uint32", "int32", "float32":
		return 4, nil
	case "uint64", "int64", "float64":
		return 8, nil
	case "string":
		if f.Length <= 0 {
			return 0, fmt.Errorf("field %q: length is required for type \"string\"", f.Name)
		}
		return f.Length, nil
	default:
		return 0, fmt.Errorf("field %q: unknown type %q", f.Name, f.Type)
	}
}

func binaryParse(b64 string, fields []Field) (map[string]any, error) {
	data, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return nil, fmt.Errorf("binaryParse: invalid base64: %w", err)
	}

	out := make(map[string]any, len(fields))
	for _, f := range fields {
		size, err := fieldSize(f)
		if err != nil {
			return nil, err
		}
		if f.Offset < 0 || f.Offset+size > len(data) {
			return nil, fmt.Errorf("field %q: offset %d+%d exceeds payload length %d", f.Name, f.Offset, size, len(data))
		}
		chunk := data[f.Offset : f.Offset+size]
		order := fieldByteOrder(f)

		switch f.Type {
		case "uint8":
			out[f.Name] = float64(chunk[0])
		case "int8":
			out[f.Name] = float64(int8(chunk[0]))
		case "uint16":
			out[f.Name] = float64(order.Uint16(chunk))
		case "int16":
			out[f.Name] = float64(int16(order.Uint16(chunk)))
		case "uint32":
			out[f.Name] = float64(order.Uint32(chunk))
		case "int32":
			out[f.Name] = float64(int32(order.Uint32(chunk)))
		case "uint64":
			out[f.Name] = float64(order.Uint64(chunk))
		case "int64":
			out[f.Name] = float64(int64(order.Uint64(chunk)))
		case "float32":
			out[f.Name] = float64(math.Float32frombits(order.Uint32(chunk)))
		case "float64":
			out[f.Name] = math.Float64frombits(order.Uint64(chunk))
		case "string":
			out[f.Name] = string(chunk)
		}
	}
	return out, nil
}

func binarySerialize(values map[string]any, fields []Field) (string, error) {
	total := 0
	for _, f := range fields {
		size, err := fieldSize(f)
		if err != nil {
			return "", err
		}
		if f.Offset+size > total {
			total = f.Offset + size
		}
	}
	data := make([]byte, total)

	for _, f := range fields {
		size, err := fieldSize(f)
		if err != nil {
			return "", err
		}
		order := fieldByteOrder(f)
		v, present := values[f.Name]
		if !present {
			return "", fmt.Errorf("field %q: missing from payload", f.Name)
		}
		chunk := data[f.Offset : f.Offset+size]

		if f.Type == "string" {
			s, ok := v.(string)
			if !ok {
				return "", fmt.Errorf("field %q: expected a string value", f.Name)
			}
			copy(chunk, s)
			continue
		}

		n, ok := toFloat(v)
		if !ok {
			return "", fmt.Errorf("field %q: expected a numeric value", f.Name)
		}
		switch f.Type {
		case "uint8", "int8":
			chunk[0] = byte(int64(n))
		case "uint16", "int16":
			order.PutUint16(chunk, uint16(int64(n)))
		case "uint32", "int32":
			order.PutUint32(chunk, uint32(int64(n)))
		case "uint64", "int64":
			order.PutUint64(chunk, uint64(int64(n)))
		case "float32":
			order.PutUint32(chunk, math.Float32bits(float32(n)))
		case "float64":
			order.PutUint64(chunk, math.Float64bits(n))
		}
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func toFloat(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}
