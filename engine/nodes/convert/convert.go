// Package convert implements the "convert" node (PROC-120): dedicated
// format-conversion modes — JSON<->XML, CSV<->records, base64, gzip
// compression, character-encoding conversion, and binary parse/serialize
// (a documented scalar-field subset: no bitfields yet, see binary.go).
package convert

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": {
			"type": "string",
			"enum": ["json2xml", "xml2json", "csv2json", "json2csv", "base64encode", "base64decode", "gzipCompress", "gzipDecompress", "encodingConvert", "binaryParse", "binarySerialize"]
		},
		"xml": {
			"type": "object",
			"properties": {
				"rootElement": { "type": "string", "default": "root" },
				"itemElement": { "type": "string", "default": "item", "description": "Element name used for array items." }
			}
		},
		"csv": {
			"type": "object",
			"properties": {
				"delimiter": { "type": "string", "default": "," },
				"hasHeader": { "type": "boolean", "default": true }
			}
		},
		"encoding": {
			"type": "object",
			"properties": {
				"from": { "type": "string", "enum": ["utf8", "latin1"] },
				"to": { "type": "string", "enum": ["utf8", "latin1"] }
			}
		},
		"fields": {
			"type": "array",
			"description": "binaryParse/binarySerialize field layout.",
			"items": {
				"type": "object",
				"properties": {
					"name": { "type": "string" },
					"offset": { "type": "integer", "minimum": 0 },
					"length": { "type": "integer", "minimum": 1 },
					"type": { "type": "string", "enum": ["uint8", "uint16", "uint32", "uint64", "int8", "int16", "int32", "int64", "float32", "float64", "string"] },
					"endianness": { "type": "string", "enum": ["big", "little"], "default": "big" }
				},
				"required": ["name", "offset", "type"]
			}
		}
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("convert", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Convert",
		Category:     flow.CategoryProcessor,
		Description:  "Format conversions: JSON<->XML, CSV<->records, base64, gzip, character encodings, binary parse/serialize (PROC-120).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// XMLConfig controls json2xml's element naming.
type XMLConfig struct {
	RootElement string `json:"rootElement,omitempty"`
	ItemElement string `json:"itemElement,omitempty"`
}

// CSVConfig controls csv2json/json2csv.
type CSVConfig struct {
	Delimiter string `json:"delimiter,omitempty"`
	HasHeader *bool  `json:"hasHeader,omitempty"`
}

// EncodingConfig controls encodingConvert.
type EncodingConfig struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// Field describes one binaryParse/binarySerialize field.
type Field struct {
	Name       string `json:"name"`
	Offset     int    `json:"offset"`
	Length     int    `json:"length,omitempty"`
	Type       string `json:"type"`
	Endianness string `json:"endianness,omitempty"`
}

// Config is the "convert" node's "config" object.
type Config struct {
	Mode     string          `json:"mode"`
	XML      XMLConfig       `json:"xml,omitempty"`
	CSV      CSVConfig       `json:"csv,omitempty"`
	Encoding *EncodingConfig `json:"encoding,omitempty"`
	Fields   []Field         `json:"fields,omitempty"`
}

var validModes = map[string]bool{
	"json2xml": true, "xml2json": true, "csv2json": true, "json2csv": true,
	"base64encode": true, "base64decode": true, "gzipCompress": true, "gzipDecompress": true,
	"encodingConvert": true, "binaryParse": true, "binarySerialize": true,
}

type node struct{ cfg Config }

// New is the flow.Factory for the "convert" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	if !validModes[cfg.Mode] {
		return nil, fmt.Errorf("convert: unknown mode %q", cfg.Mode)
	}
	if cfg.Mode == "encodingConvert" {
		if cfg.Encoding == nil || cfg.Encoding.From == "" || cfg.Encoding.To == "" {
			return nil, fmt.Errorf("convert: encoding.from and encoding.to are required for mode %q", cfg.Mode)
		}
	}
	if (cfg.Mode == "binaryParse" || cfg.Mode == "binarySerialize") && len(cfg.Fields) == 0 {
		return nil, fmt.Errorf("convert: fields is required for mode %q", cfg.Mode)
	}
	if cfg.XML.RootElement == "" {
		cfg.XML.RootElement = "root"
	}
	if cfg.XML.ItemElement == "" {
		cfg.XML.ItemElement = "item"
	}
	if cfg.CSV.Delimiter == "" {
		cfg.CSV.Delimiter = ","
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	out, err := n.convert(in.Payload.Value)
	if err != nil {
		return nil, fmt.Errorf("convert: %s: %w", n.cfg.Mode, err)
	}
	d := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: out})
	return []flow.PortDatagram{{Port: "out", Datagram: d}}, nil
}

func (n *node) convert(value any) (any, error) {
	switch n.cfg.Mode {
	case "json2xml":
		return jsonToXML(value, n.cfg.XML)
	case "xml2json":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("xml2json: payload must be a string")
		}
		return xmlToJSON(s)
	case "csv2json":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("csv2json: payload must be a string")
		}
		return csvToJSON(s, n.cfg.CSV)
	case "json2csv":
		return jsonToCSV(value, n.cfg.CSV)
	case "base64encode":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("base64encode: payload must be a string")
		}
		return base64.StdEncoding.EncodeToString([]byte(s)), nil
	case "base64decode":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("base64decode: payload must be a string")
		}
		b, err := base64.StdEncoding.DecodeString(s)
		if err != nil {
			return nil, fmt.Errorf("base64decode: %w", err)
		}
		return string(b), nil
	case "gzipCompress":
		s, ok := value.(string)
		if !ok {
			b, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("gzipCompress: %w", err)
			}
			s = string(b)
		}
		return gzipCompress(s)
	case "gzipDecompress":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("gzipDecompress: payload must be a base64 string")
		}
		return gzipDecompress(s)
	case "encodingConvert":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("encodingConvert: payload must be a string")
		}
		return convertEncoding(s, n.cfg.Encoding.From, n.cfg.Encoding.To)
	case "binaryParse":
		s, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("binaryParse: payload must be a base64 string")
		}
		return binaryParse(s, n.cfg.Fields)
	case "binarySerialize":
		m, ok := value.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("binarySerialize: payload must be an object")
		}
		return binarySerialize(m, n.cfg.Fields)
	default:
		return nil, fmt.Errorf("unhandled mode %q", n.cfg.Mode)
	}
}

func gzipCompress(s string) (string, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write([]byte(s)); err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	if err := w.Close(); err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	return base64.StdEncoding.EncodeToString(buf.Bytes()), nil
}

func gzipDecompress(b64 string) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return "", fmt.Errorf("gzip: invalid base64: %w", err)
	}
	r, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	defer func() { _ = r.Close() }()
	out, err := io.ReadAll(r)
	if err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	return string(out), nil
}
