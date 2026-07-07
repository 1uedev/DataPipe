package api

import (
	"context"
	"net/http"
	"testing"
)

func seedDeadLetter(t *testing.T, e *testEnv, flowID, nodeID string) string {
	t.Helper()
	if err := e.store.RecordDeadLetter(context.Background(), "rt-1", DeadLetterEventInput{
		FlowID: flowID, NodeID: nodeID, Port: "in", Reason: "node_error",
		DatagramJSON: `{"header":{"id":"x","correlationId":"x"},"payload":{"value":"bad"}}`,
		TimeUnixMs:   1000,
	}); err != nil {
		t.Fatalf("RecordDeadLetter: %v", err)
	}
	dls, err := e.store.ListDeadLetters(context.Background(), flowID, 10, 0)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	return dls[0].ID
}

func TestERR130_ListDeadLettersReturnsSeededEntries(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	seedDeadLetter(t, e, f.ID, "n1")

	resp := e.request(http.MethodGet, "/flows/"+f.ID+"/dead-letters", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", resp.status, resp.body)
	}
	var dls []DeadLetter
	resp.decode(t, &dls)
	if len(dls) != 1 || dls[0].NodeID != "n1" || dls[0].Reason != "node_error" {
		t.Fatalf("dead letters = %+v, want one entry for node n1", dls)
	}
}

func TestERR130_ReinjectDeadLetterCallsCommanderAndMarksReinjected(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	id := seedDeadLetter(t, e, f.ID, "n1")

	resp := e.request(http.MethodPost, "/dead-letters/"+id+"/reinject", token, nil)
	if resp.status != http.StatusAccepted {
		t.Fatalf("reinject status = %d, body = %s", resp.status, resp.body)
	}
	if e.commander.reinjectedNodeID != "n1" || e.commander.reinjectedPort != "in" {
		t.Fatalf("commander reinjected nodeID=%q port=%q, want n1/in", e.commander.reinjectedNodeID, e.commander.reinjectedPort)
	}

	dl, err := e.store.GetDeadLetter(context.Background(), id)
	if err != nil {
		t.Fatalf("GetDeadLetter: %v", err)
	}
	if dl.ReinjectedAt == nil {
		t.Fatal("expected ReinjectedAt to be set after a successful reinject command")
	}
}

func TestERR130_DeleteDeadLetter(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)
	id := seedDeadLetter(t, e, f.ID, "n1")

	resp := e.request(http.MethodDelete, "/dead-letters/"+id, token, nil)
	if resp.status != http.StatusNoContent {
		t.Fatalf("delete status = %d, body = %s", resp.status, resp.body)
	}
	if _, err := e.store.GetDeadLetter(context.Background(), id); err == nil {
		t.Fatal("expected the dead letter to be gone after delete")
	}
}

func TestERR130_DeadLetterActionsRequireOperatorRole(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project, f := createProjectAndFlow(t, e, owner)
	id := seedDeadLetter(t, e, f.ID, "n1")

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), "viewer"); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}
	if resp := e.request(http.MethodPost, "/dead-letters/"+id+"/reinject", viewer, nil); resp.status != http.StatusForbidden {
		t.Fatalf("reinject status = %d, want 403 for a viewer", resp.status)
	}
	if resp := e.request(http.MethodDelete, "/dead-letters/"+id, viewer, nil); resp.status != http.StatusForbidden {
		t.Fatalf("delete status = %d, want 403 for a viewer", resp.status)
	}
}
