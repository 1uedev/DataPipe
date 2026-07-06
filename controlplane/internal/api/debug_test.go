package api

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func createProjectAndFlow(t *testing.T, e *testEnv, token string) (Project, Flow) {
	t.Helper()
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "debug-test-project"})
	if resp.status != http.StatusCreated {
		t.Fatalf("create project status = %d, body = %s", resp.status, resp.body)
	}
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/flows", token, map[string]any{
		"name": "debug test flow", "content": sampleFlowContent(),
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create flow status = %d, body = %s", resp.status, resp.body)
	}
	var f Flow
	resp.decode(t, &f)
	return project, f
}

func TestDBG130_ExecuteNodeViaREST(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	// sampleFlowContent's "n2" is a "set" node adding status=processed.
	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/n2/execute", token, map[string]any{"payload": map[string]any{"value": 1}})
	if resp.status != http.StatusOK {
		t.Fatalf("execute status = %d, body = %s", resp.status, resp.body)
	}
	var result struct {
		Outputs []struct {
			Port     string `json:"port"`
			Datagram struct {
				Payload struct {
					Value map[string]any `json:"value"`
				} `json:"payload"`
			} `json:"datagram"`
		} `json:"outputs"`
		Error *string `json:"error"`
	}
	resp.decode(t, &result)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", *result.Error)
	}
	if len(result.Outputs) != 1 {
		t.Fatalf("expected 1 output, got %+v", result.Outputs)
	}
	if result.Outputs[0].Datagram.Payload.Value["status"] != "processed" {
		t.Errorf("output payload = %+v, want status=processed", result.Outputs[0].Datagram.Payload.Value)
	}
}

func TestDBG130_ExecuteNodeUnknownNodeId(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/does-not-exist/execute", token, map[string]any{"payload": 1})
	if resp.status != http.StatusBadRequest {
		t.Errorf("execute unknown node status = %d, want 400, body = %s", resp.status, resp.body)
	}
}

func TestDBG130_ExecuteNodeRequiresEditor(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project, f := createProjectAndFlow(t, e, owner)

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), auth.RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/n2/execute", viewer, map[string]any{"payload": 1})
	if resp.status != http.StatusForbidden {
		t.Errorf("viewer executing a node status = %d, want 403", resp.status)
	}
}

func TestDBG130_PinsCRUD(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	resp := e.request(http.MethodGet, "/flows/"+f.ID+"/debug/pins", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list pins status = %d, body = %s", resp.status, resp.body)
	}
	var pins []DebugPin
	resp.decode(t, &pins)
	if len(pins) != 0 {
		t.Fatalf("expected no pins yet, got %+v", pins)
	}

	resp = e.request(http.MethodPut, "/flows/"+f.ID+"/nodes/n2/pins/out", token, map[string]any{"value": map[string]any{"status": "processed"}})
	if resp.status != http.StatusOK {
		t.Fatalf("pin status = %d, body = %s", resp.status, resp.body)
	}
	var pin DebugPin
	resp.decode(t, &pin)
	if pin.NodeID != "n2" || pin.Port != "out" {
		t.Errorf("pin = %+v, want nodeId=n2 port=out", pin)
	}

	resp = e.request(http.MethodGet, "/flows/"+f.ID+"/debug/pins", token, nil)
	resp.decode(t, &pins)
	if len(pins) != 1 {
		t.Fatalf("expected 1 pin after upsert, got %+v", pins)
	}

	// Upsert again at the same (node, port) overwrites rather than duplicating.
	resp = e.request(http.MethodPut, "/flows/"+f.ID+"/nodes/n2/pins/out", token, map[string]any{"value": 42})
	if resp.status != http.StatusOK {
		t.Fatalf("re-pin status = %d, body = %s", resp.status, resp.body)
	}
	resp = e.request(http.MethodGet, "/flows/"+f.ID+"/debug/pins", token, nil)
	resp.decode(t, &pins)
	if len(pins) != 1 {
		t.Fatalf("expected still 1 pin after overwrite, got %+v", pins)
	}
	if v, _ := pins[0].Value.(float64); v != 42 {
		t.Errorf("pin value after overwrite = %v, want 42", pins[0].Value)
	}

	resp = e.request(http.MethodDelete, "/flows/"+f.ID+"/nodes/n2/pins/out", token, nil)
	if resp.status != http.StatusNoContent {
		t.Fatalf("delete pin status = %d, body = %s", resp.status, resp.body)
	}
	resp = e.request(http.MethodGet, "/flows/"+f.ID+"/debug/pins", token, nil)
	resp.decode(t, &pins)
	if len(pins) != 0 {
		t.Fatalf("expected no pins after delete, got %+v", pins)
	}
}

func TestDBG110_LoadFullDebugEventNotFound(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	resp := e.request(http.MethodGet, "/flows/"+f.ID+"/debug/events/does-not-exist", token, nil)
	if resp.status != http.StatusNotFound {
		t.Errorf("load-full for unknown event status = %d, want 404", resp.status)
	}
}

func TestDBG170_DebugWebSocketRequiresAuthAndOperatorRole(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project, f := createProjectAndFlow(t, e, owner)

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), auth.RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	wsURL := strings.Replace(e.server.URL, "http://", "ws://", 1) + "/ws/debug"

	cases := []struct {
		name       string
		url        string
		wantStatus int
	}{
		{"no token", wsURL + "?flowId=" + f.ID, http.StatusUnauthorized},
		{"bad token", wsURL + "?flowId=" + f.ID + "&token=nonsense", http.StatusUnauthorized},
		{"missing flowId", wsURL + "?token=" + owner, http.StatusBadRequest},
		{"unknown flow", wsURL + "?flowId=does-not-exist&token=" + owner, http.StatusNotFound},
		{"viewer forbidden", wsURL + "?flowId=" + f.ID + "&token=" + viewer, http.StatusForbidden},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_, resp, err := websocket.Dial(ctx, c.url, nil)
			if err == nil {
				t.Fatal("expected the dial to fail")
			}
			if resp == nil || resp.StatusCode != c.wantStatus {
				got := 0
				if resp != nil {
					got = resp.StatusCode
				}
				t.Errorf("status = %d, want %d (err: %v)", got, c.wantStatus, err)
			}
		})
	}
}

// TestDBG170_DebugWebSocketConnectsAndSubscribesForAuthorizedUser proves the
// HTTP-layer wiring — Accept, role check, Hub.Subscribe/cancel — works for
// an authorized caller. The hub's own event relay/replay/truncation
// mechanics are covered thoroughly against the real gRPC wire protocol in
// controlplane/internal/debughub, so this only needs to prove the WS
// handler reaches a live, open subscription rather than erroring out.
func TestDBG170_DebugWebSocketConnectsAndSubscribesForAuthorizedUser(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	_, f := createProjectAndFlow(t, e, owner)

	wsURL := strings.Replace(e.server.URL, "http://", "ws://", 1) + "/ws/debug?flowId=" + f.ID + "&token=" + owner

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = conn.CloseNow() }()
	if resp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake status = %d, want 101", resp.StatusCode)
	}

	// No runtime is connected in this test, so nothing will ever arrive;
	// the read should simply time out rather than error or hang forever,
	// proving the connection is genuinely open and idle, not closed.
	readCtx, readCancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer readCancel()
	var msg wsMessage
	err = wsjson.Read(readCtx, conn, &msg)
	if err == nil {
		t.Fatalf("unexpected message with no runtime connected: %+v", msg)
	}
	if !errorsIsDeadlineExceeded(err) {
		t.Errorf("expected a deadline-exceeded read timeout, got: %v", err)
	}
}

func errorsIsDeadlineExceeded(err error) bool {
	return strings.Contains(err.Error(), "context deadline exceeded") ||
		strings.Contains(err.Error(), "i/o timeout")
}
