package api

import (
	"encoding/json"
	"net/http"

	"github.com/1uedev/DataPipe/engine/flow"
)

// NodeType is the editor-facing view of a registered node type: enough to
// render a palette entry (UI-110) and generate its config form from
// ConfigSchema (UI-170; CLAUDE.md's "generated from JSON Schema" rule).
type NodeType struct {
	Type         string          `json:"type"`
	DisplayName  string          `json:"displayName"`
	Category     string          `json:"category"`
	Description  string          `json:"description"`
	Kind         string          `json:"kind"` // "source" | "processor"
	Inputs       []string        `json:"inputs"`
	Outputs      []string        `json:"outputs"`
	ConfigSchema json.RawMessage `json:"configSchema"`
}

func (h *Handlers) listNodeTypes(w http.ResponseWriter, r *http.Request) {
	if _, ok := currentUser(w, r); !ok {
		return
	}
	registered := flow.ListNodeTypes()
	out := make([]NodeType, len(registered))
	for i, n := range registered {
		kind := "processor"
		if n.Info.Kind == flow.KindSource {
			kind = "source"
		}
		out[i] = NodeType{
			Type:         n.Type,
			DisplayName:  n.Info.DisplayName,
			Category:     string(n.Info.Category),
			Description:  n.Info.Description,
			Kind:         kind,
			Inputs:       n.Info.Inputs,
			Outputs:      n.Info.Outputs,
			ConfigSchema: n.Info.ConfigSchema,
		}
	}
	writeJSON(w, http.StatusOK, out)
}
