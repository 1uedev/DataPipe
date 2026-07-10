// MAP-100 SECS/GEM report builder: browses a "secsgem" connection's live
// SVID catalog (S1F11 Status Variable Namelist), the same active-only
// dial-from-the-control-plane pattern CON-140's testConnection uses.
package api

import (
	"net/http"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/conntest"
)

// SecsgemSVID is one entry of a secsgemBrowse response.
type SecsgemSVID struct {
	SVID  uint32 `json:"svid"`
	Name  string `json:"name"`
	Units string `json:"units"`
}

// SecsgemBrowseResult is secsgemBrowse's response shape.
type SecsgemBrowseResult struct {
	OK      bool          `json:"ok"`
	Message string        `json:"message,omitempty"`
	SVIDs   []SecsgemSVID `json:"svids,omitempty"`
}

func (h *Handlers) secsgemBrowse(w http.ResponseWriter, r *http.Request) {
	user, ok := currentUser(w, r)
	if !ok {
		return
	}
	c, ok := h.connectionAndAuthorize(w, r, user, auth.RoleEditor)
	if !ok {
		return
	}

	info, err := NewConnectionResolver(h.store, h.vault).ResolveConnection(r.Context(), c.ID)
	if err != nil {
		writeJSON(w, http.StatusOK, SecsgemBrowseResult{OK: false, Message: err.Error()})
		return
	}
	if info.Type != "secsgem" {
		writeJSON(w, http.StatusOK, SecsgemBrowseResult{OK: false, Message: "not a secsgem connection"})
		return
	}

	list, err := conntest.SECSGEMBrowseSVIDs(r.Context(), info.ConfigJSON)
	result := SecsgemBrowseResult{OK: err == nil}
	if err != nil {
		result.Message = err.Error()
	} else {
		result.SVIDs = make([]SecsgemSVID, len(list))
		for i, sv := range list {
			result.SVIDs[i] = SecsgemSVID{SVID: sv.SVID, Name: sv.Name, Units: sv.Units}
		}
	}
	h.audit(r, user.ID, "connection.secsgemBrowse", "connection", c.ID, c.ProjectID, nil, result)
	writeJSON(w, http.StatusOK, result)
}
