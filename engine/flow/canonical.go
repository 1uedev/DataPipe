package flow

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
)

// Parse decodes a flow file from JSON. It does not sort or otherwise
// canonicalize the result; call MarshalCanonical to get the deterministic
// on-disk representation.
func Parse(data []byte) (*FlowFile, error) {
	var f FlowFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("flow: parse: %w", err)
	}
	return &f, nil
}

// MarshalCanonical renders f as the deterministic, diff-friendly format
// required by Flow-File-Format.md §1/§7.6: object keys sorted, nodes/wires/
// env arrays ordered by id (env by name), 2-space indent. Calling
// MarshalCanonical on the result of Parse(result) again reproduces the same
// bytes (the round-trip guarantee of §7.6).
func (f FlowFile) MarshalCanonical() ([]byte, error) {
	sorted := f
	sorted.Graph.Nodes = append([]Node(nil), f.Graph.Nodes...)
	sorted.Graph.Wires = append([]Wire(nil), f.Graph.Wires...)
	sorted.Env = append([]EnvVar(nil), f.Env...)

	sort.Slice(sorted.Graph.Nodes, func(i, j int) bool { return sorted.Graph.Nodes[i].ID < sorted.Graph.Nodes[j].ID })
	sort.Slice(sorted.Graph.Wires, func(i, j int) bool { return sorted.Graph.Wires[i].ID < sorted.Graph.Wires[j].ID })
	sort.Slice(sorted.Env, func(i, j int) bool { return sorted.Env[i].Name < sorted.Env[j].Name })

	raw, err := json.Marshal(sorted)
	if err != nil {
		return nil, fmt.Errorf("flow: marshal: %w", err)
	}
	return canonicalizeJSON(raw)
}

// canonicalizeJSON re-encodes raw JSON with alphabetically sorted object
// keys (Go's encoding/json already sorts map[string]any keys; round-tripping
// through a generic value gets that behavior at every nesting level) and a
// 2-space indent. json.Number preserves numeric formatting exactly, so this
// never perturbs values.
func canonicalizeJSON(raw []byte) ([]byte, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var generic any
	if err := dec.Decode(&generic); err != nil {
		return nil, fmt.Errorf("flow: canonicalize: %w", err)
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetIndent("", "  ")
	enc.SetEscapeHTML(false)
	if err := enc.Encode(generic); err != nil {
		return nil, fmt.Errorf("flow: canonicalize: %w", err)
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}
