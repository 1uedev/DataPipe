package api

import (
	"net/http"
	"testing"
)

func TestUI110_ListNodeTypesExposesRegisteredManifests(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")

	resp := e.request(http.MethodGet, "/node-types", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("status = %d, body = %s", resp.status, resp.body)
	}

	var types []NodeType
	resp.decode(t, &types)

	byType := make(map[string]NodeType, len(types))
	for _, n := range types {
		byType[n.Type] = n
	}
	for _, want := range []string{"inject", "set", "debug-log"} {
		got, ok := byType[want]
		if !ok {
			t.Fatalf("node type %q missing from /node-types response: %+v", want, types)
		}
		if got.DisplayName == "" || got.Category == "" || len(got.ConfigSchema) == 0 {
			t.Errorf("node type %q missing manifest fields: %+v", want, got)
		}
	}

	if byType["inject"].Kind != "source" {
		t.Errorf("inject kind = %q, want source", byType["inject"].Kind)
	}
	if byType["set"].Kind != "processor" || len(byType["set"].Inputs) != 1 {
		t.Errorf("set = %+v, want kind=processor with one input", byType["set"])
	}
}
