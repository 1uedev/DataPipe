package convert

import "fmt"

// convertEncoding supports the utf8<->latin1 pair (ISO-8859-1's byte values
// map 1:1 onto the first 256 Unicode code points, per the same correct
// convention engine/nodes/filewatch uses for CSV/JSON file reads).
// utf8->latin1 is lossy for any code point above U+00FF; the source
// character is dropped rather than silently corrupted into garbage bytes,
// and the count of dropped characters is reported in the error if any
// occurred, per MAP-150's "cast failures... never silent coercion".
func convertEncoding(s, from, to string) (string, error) {
	if from == to {
		return s, nil
	}
	switch {
	case from == "utf8" && to == "latin1":
		return utf8ToLatin1(s)
	case from == "latin1" && to == "utf8":
		return latin1ToUTF8(s), nil
	default:
		return "", fmt.Errorf("unsupported conversion %q -> %q", from, to)
	}
}

func latin1ToUTF8(s string) string {
	runes := make([]rune, len(s))
	for i := 0; i < len(s); i++ {
		runes[i] = rune(s[i])
	}
	return string(runes)
}

func utf8ToLatin1(s string) (string, error) {
	out := make([]byte, 0, len(s))
	dropped := 0
	for _, r := range s {
		if r > 0xFF {
			dropped++
			continue
		}
		out = append(out, byte(r))
	}
	if dropped > 0 {
		return "", fmt.Errorf("utf8->latin1: %d character(s) have no Latin-1 representation", dropped)
	}
	return string(out), nil
}
