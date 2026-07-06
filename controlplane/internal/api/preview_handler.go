// MAP-110 "fetch sample now": design-time single-node preview for Source
// node types, mirroring DBG-130's executeNode — looks the node up in the
// flow's current draft and runs it in-process via engine/flow.PreviewSource,
// no live deployment or runtime round-trip involved.
package api

import (
	"net/http"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/engine/flow"
)

func (h *Handlers) previewNode(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	f, ok := h.flowAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}

	ff, err := flow.Parse(f.Content)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid flow file: "+err.Error())
		return
	}
	nodeID := r.PathValue("nodeId")
	var node *flow.Node
	for i := range ff.Graph.Nodes {
		if ff.Graph.Nodes[i].ID == nodeID {
			node = &ff.Graph.Nodes[i]
			break
		}
	}
	if node == nil {
		writeError(w, http.StatusBadRequest, "unknown node id")
		return
	}

	records, err := flow.PreviewSource(r.Context(), node.Type, node.Config, flow.DefaultPreviewMaxRecords, flow.DefaultPreviewTimeout)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	out := make([]map[string]any, len(records))
	for i, rec := range records {
		out[i] = map[string]any{"port": rec.Port, "datagram": rec.Datagram}
	}
	writeJSON(w, http.StatusOK, map[string]any{"records": out, "error": nil})
}
