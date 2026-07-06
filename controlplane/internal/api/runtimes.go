package api

import "net/http"

func (h *Handlers) listRuntimes(w http.ResponseWriter, r *http.Request) {
	if _, ok := currentUser(w, r); !ok {
		return
	}
	writeJSON(w, http.StatusOK, h.runtimes.ListRuntimes())
}
