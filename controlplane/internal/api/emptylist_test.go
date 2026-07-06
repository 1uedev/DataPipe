package api

import (
	"net/http"
	"testing"
)

// TestEmptyListsAreEmptyArraysNotNull guards against a real bug found while
// manually verifying the editor UI: Go's json.Marshal renders a nil slice
// as the JSON literal `null`, and the frontend's own "not loaded yet" state
// also used `null` — so an empty (but successfully loaded) list looked
// identical to "still loading" forever. Every list-shaped endpoint must
// return `[]`, never `null`, when there are zero results.
func TestEmptyListsAreEmptyArraysNotNull(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodGet, "/projects", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("GET /projects status = %d", resp.status)
	}
	if string(resp.body) == "null" {
		t.Error("GET /projects with zero projects returned `null`, want `[]`")
	}

	projResp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	projResp.decode(t, &project)

	for _, path := range []string{
		"/projects/" + project.ID + "/flows",
		"/projects/" + project.ID + "/connections",
		"/projects/" + project.ID + "/credentials",
	} {
		resp := e.request(http.MethodGet, path, token, nil)
		if resp.status != http.StatusOK {
			t.Fatalf("GET %s status = %d", path, resp.status)
		}
		if string(resp.body) == "null" {
			t.Errorf("GET %s with zero results returned `null`, want `[]`", path)
		}
	}
}
