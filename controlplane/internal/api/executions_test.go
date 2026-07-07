package api

import (
	"context"
	"net/http"
	"testing"
)

// seedExecution drives Store.RecordExecutionEvent directly (as
// controlplane/internal/registry's EventChannel handler would in
// production) to create one execution with the given status, plus one
// node-io row for nodeID.
func seedExecution(t *testing.T, e *testEnv, flowID, execID, status string) {
	t.Helper()
	ctx := context.Background()
	if err := e.store.RecordExecutionEvent(ctx, "rt-1", ExecutionEventInput{
		ExecutionID: execID, FlowID: flowID, Phase: "started",
		TriggerNodeID: "trig", TriggerKind: "webhook", TimeUnixMs: 1000,
		SeedDatagramJSON: `{"header":{"id":"seed","correlationId":"` + execID + `"},"payload":{"value":"hi"}}`,
	}); err != nil {
		t.Fatalf("seed started: %v", err)
	}
	if err := e.store.RecordExecutionEvent(ctx, "rt-1", ExecutionEventInput{
		ExecutionID: execID, FlowID: flowID, Phase: "node",
		NodeID: "mf", Port: "in", Attempt: 1, TimeUnixMs: 1100, DurationUs: 500,
		InputJSON: `{"payload":{"value":"hi"}}`, OutputsJSON: `[]`,
	}); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	if status != "" {
		if err := e.store.RecordExecutionEvent(ctx, "rt-1", ExecutionEventInput{
			ExecutionID: execID, FlowID: flowID, Phase: "finished", Status: status, TimeUnixMs: 1200,
		}); err != nil {
			t.Fatalf("seed finished: %v", err)
		}
	}
}

func TestDBG140_ListExecutionsReturnsSeededExecutionsNewestFirst(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	seedExecution(t, e, f.ID, "exec-1", "success")
	seedExecution(t, e, f.ID, "exec-2", "failed")

	resp := e.request(http.MethodGet, "/flows/"+f.ID+"/executions", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", resp.status, resp.body)
	}
	var execs []Execution
	resp.decode(t, &execs)
	if len(execs) != 2 {
		t.Fatalf("expected 2 executions, got %+v", execs)
	}
	if execs[0].ID != "exec-2" || execs[1].ID != "exec-1" {
		t.Fatalf("expected newest first (exec-2, exec-1), got (%s, %s)", execs[0].ID, execs[1].ID)
	}
}

func TestDBG140_ListExecutionsFiltersByStatus(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	seedExecution(t, e, f.ID, "exec-1", "success")
	seedExecution(t, e, f.ID, "exec-2", "failed")

	resp := e.request(http.MethodGet, "/flows/"+f.ID+"/executions?status=failed", token, nil)
	var execs []Execution
	resp.decode(t, &execs)
	if len(execs) != 1 || execs[0].ID != "exec-2" {
		t.Fatalf("expected only exec-2, got %+v", execs)
	}
}

func TestDBG140_GetExecutionIncludesNodeIOTrace(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	seedExecution(t, e, f.ID, "exec-1", "success")

	resp := e.request(http.MethodGet, "/executions/exec-1", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", resp.status, resp.body)
	}
	var got struct {
		Execution
		NodeIO []ExecutionNodeIO `json:"nodeIO"`
	}
	resp.decode(t, &got)
	if got.Status != "success" {
		t.Fatalf("status = %q, want success", got.Status)
	}
	if len(got.NodeIO) != 1 || got.NodeIO[0].NodeID != "mf" {
		t.Fatalf("nodeIO = %+v, want one entry for node \"mf\"", got.NodeIO)
	}
}

func TestDBG140_GetExecutionUnknownIDReturns404(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	resp := e.request(http.MethodGet, "/executions/does-not-exist", token, nil)
	if resp.status != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.status)
	}
}

func TestDBG140_ExecutionsRequireProjectMembership(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	_, f := createProjectAndFlow(t, e, owner)
	seedExecution(t, e, f.ID, "exec-1", "success")

	stranger := e.createUserAndLogin("stranger", "")
	resp := e.request(http.MethodGet, "/flows/"+f.ID+"/executions", stranger, nil)
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a non-member", resp.status)
	}
	resp = e.request(http.MethodGet, "/executions/exec-1", stranger, nil)
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a non-member", resp.status)
	}
}

func TestENG130_RerunFromStartCallsCommanderWithSeedDatagram(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	seedExecution(t, e, f.ID, "exec-1", "failed")

	resp := e.request(http.MethodPost, "/executions/exec-1/rerun", token, map[string]any{"from": "start"})
	if resp.status != http.StatusAccepted {
		t.Fatalf("rerun status = %d, body = %s", resp.status, resp.body)
	}
	if e.commander.ranNodeID != "trig" || e.commander.ranPort != "out" || e.commander.ranReRunOf != "exec-1" {
		t.Fatalf("commander called with nodeID=%q port=%q reRunOf=%q, want trig/out/exec-1", e.commander.ranNodeID, e.commander.ranPort, e.commander.ranReRunOf)
	}
}

func TestENG130_RerunFromNodeUsesRecordedNodeInput(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	seedExecution(t, e, f.ID, "exec-1", "failed")

	resp := e.request(http.MethodPost, "/executions/exec-1/rerun", token, map[string]any{"from": "node", "nodeId": "mf"})
	if resp.status != http.StatusAccepted {
		t.Fatalf("rerun status = %d, body = %s", resp.status, resp.body)
	}
	if e.commander.ranNodeID != "mf" || e.commander.ranPort != "in" || e.commander.ranDatagramJSON != `{"payload":{"value":"hi"}}` {
		t.Fatalf("commander called with nodeID=%q port=%q datagram=%q, want mf/in/the recorded input", e.commander.ranNodeID, e.commander.ranPort, e.commander.ranDatagramJSON)
	}
}

func TestENG130_RerunFromNodeUnknownNodeIsBadRequest(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	seedExecution(t, e, f.ID, "exec-1", "failed")

	resp := e.request(http.MethodPost, "/executions/exec-1/rerun", token, map[string]any{"from": "node", "nodeId": "does-not-exist"})
	if resp.status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.status)
	}
}

func TestENG130_RerunRequiresOperatorRole(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project, f := createProjectAndFlow(t, e, owner)
	seedExecution(t, e, f.ID, "exec-1", "failed")

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), "viewer"); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}
	resp := e.request(http.MethodPost, "/executions/exec-1/rerun", viewer, map[string]any{"from": "start"})
	if resp.status != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 for a viewer", resp.status)
	}
}

func TestENG130_CancelExecutionCallsCommander(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	seedExecution(t, e, f.ID, "exec-1", "")

	resp := e.request(http.MethodPost, "/executions/exec-1/cancel", token, nil)
	if resp.status != http.StatusAccepted {
		t.Fatalf("cancel status = %d, body = %s", resp.status, resp.body)
	}
	if e.commander.cancelledExecutionID != "exec-1" {
		t.Fatalf("commander cancelledExecutionID = %q, want exec-1", e.commander.cancelledExecutionID)
	}
}
