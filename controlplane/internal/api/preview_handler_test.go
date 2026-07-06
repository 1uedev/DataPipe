package api

import (
	"context"
	"net/http"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
)

func TestMAP110_PreviewNodeViaREST(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	// sampleFlowContent's "n1" is an inject node (a Source) that fires once
	// (repeatMs: 0) with payload {"value": 1}.
	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/n1/preview", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("preview status = %d, body = %s", resp.status, resp.body)
	}
	var result struct {
		Records []struct {
			Port     string `json:"port"`
			Datagram struct {
				Payload struct {
					Value map[string]any `json:"value"`
				} `json:"payload"`
			} `json:"datagram"`
		} `json:"records"`
		Error *string `json:"error"`
	}
	resp.decode(t, &result)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", *result.Error)
	}
	if len(result.Records) != 1 {
		t.Fatalf("expected 1 record, got %+v", result.Records)
	}
	if result.Records[0].Datagram.Payload.Value["value"] != float64(1) {
		t.Errorf("record payload = %+v, want value=1", result.Records[0].Datagram.Payload.Value)
	}
}

func TestMAP110_PreviewNodeUnknownNodeId(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/does-not-exist/preview", token, nil)
	if resp.status != http.StatusBadRequest {
		t.Errorf("preview unknown node status = %d, want 400, body = %s", resp.status, resp.body)
	}
}

func TestMAP110_PreviewNodeRejectsProcessorNodeType(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	_, f := createProjectAndFlow(t, e, token)

	// "n2" is a "set" node — a Processor, not a Source.
	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/n2/preview", token, nil)
	if resp.status != http.StatusBadRequest {
		t.Errorf("preview processor node status = %d, want 400, body = %s", resp.status, resp.body)
	}
}

func TestMAP110_PreviewNodeRequiresEditor(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project, f := createProjectAndFlow(t, e, owner)

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), auth.RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	resp := e.request(http.MethodPost, "/flows/"+f.ID+"/nodes/n1/preview", viewer, nil)
	if resp.status != http.StatusForbidden {
		t.Errorf("viewer previewing a node status = %d, want 403", resp.status)
	}
}
