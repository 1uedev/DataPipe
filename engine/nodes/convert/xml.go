package convert

import (
	"encoding/xml"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// jsonToXML and xmlToJSON implement a deliberately simplified, documented
// subset of JSON<->XML conversion: elements only (no XML attributes), an
// array value under key K becomes K repeated as a sibling element per item
// (unambiguous and the exact inverse of xmlToJSON's sibling-grouping),
// object values become nested elements (keys sorted for determinism), and
// scalar values become an element's text content. cfg.ItemElement only
// applies when the whole payload is itself a bare array (no key to reuse).

func jsonToXML(value any, cfg XMLConfig) (string, error) {
	var b strings.Builder
	b.WriteString(xml.Header)
	if arr, ok := value.([]any); ok {
		fmt.Fprintf(&b, "<%s>", cfg.RootElement)
		for _, item := range arr {
			if err := writeXMLElement(&b, cfg.ItemElement, item, cfg); err != nil {
				return "", err
			}
		}
		fmt.Fprintf(&b, "</%s>", cfg.RootElement)
		return b.String(), nil
	}
	if err := writeXMLElement(&b, cfg.RootElement, value, cfg); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeXMLElement(b *strings.Builder, name string, value any, cfg XMLConfig) error {
	switch v := value.(type) {
	case map[string]any:
		fmt.Fprintf(b, "<%s>", xml.Name{Local: name}.Local)
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if err := writeXMLElement(b, k, v[k], cfg); err != nil {
				return err
			}
		}
		fmt.Fprintf(b, "</%s>", name)
	case []any:
		for _, item := range v {
			if err := writeXMLElement(b, name, item, cfg); err != nil {
				return err
			}
		}
	case nil:
		fmt.Fprintf(b, "<%s/>", name)
	default:
		fmt.Fprintf(b, "<%s>%s</%s>", name, xmlEscape(fmt.Sprint(v)), name)
	}
	return nil
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

// xmlToJSON parses raw as XML into a generic map[string]any/[]any/string
// tree, keyed by element name (siblings with the same name at one level
// become an array), following the same simplified convention as
// jsonToXML — the inverse is only exact for documents actually produced by
// jsonToXML or shaped like it.
func XMLToJSON(raw string) (any, error) {
	dec := xml.NewDecoder(strings.NewReader(raw))
	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("xml2json: %w", err)
		}
		if start, ok := tok.(xml.StartElement); ok {
			return parseXMLElement(dec, start)
		}
	}
}

func parseXMLElement(dec *xml.Decoder, start xml.StartElement) (any, error) {
	children := map[string][]any{}
	var text strings.Builder
	hasChildren := false

	for {
		tok, err := dec.Token()
		if err != nil {
			return nil, fmt.Errorf("xml2json: %w", err)
		}
		switch t := tok.(type) {
		case xml.StartElement:
			hasChildren = true
			child, err := parseXMLElement(dec, t)
			if err != nil {
				return nil, err
			}
			children[t.Name.Local] = append(children[t.Name.Local], child)
		case xml.CharData:
			text.Write(t)
		case xml.EndElement:
			if !hasChildren {
				return parseScalar(strings.TrimSpace(text.String())), nil
			}
			out := make(map[string]any, len(children))
			for name, values := range children {
				if len(values) == 1 {
					out[name] = values[0]
				} else {
					out[name] = values
				}
			}
			return out, nil
		}
	}
}

func parseScalar(s string) any {
	if s == "" {
		return nil
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f
	}
	if b, err := strconv.ParseBool(s); err == nil {
		return b
	}
	return s
}
