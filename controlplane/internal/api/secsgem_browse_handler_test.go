package api

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/engine/nodes/gemsim"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

func TestMAP100_SecsgemBrowseAgainstSimulator(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	project := createProjectOnly(t, e, token)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Atoi: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	simDone := make(chan struct{})
	go func() {
		defer close(simDone)
		sim, err := gemsim.Listen(ctx, addr, gemsim.Config{
			MDLN: "SIM-1", SoftRev: "1.0",
			SVIDs: map[uint32]gemsim.SVID{4001: {Name: "BeltSpeed", Units: "mm/s", Value: secsii.F8v(120)}},
		})
		if err != nil {
			return
		}
		<-ctx.Done()
		_ = sim.Close()
	}()
	time.Sleep(50 * time.Millisecond)

	resp := e.request(http.MethodPost, "/projects/"+project.ID+"/connections", token, map[string]any{
		"name": "sim-equip", "type": "secsgem",
		"config": map[string]any{"mode": "active", "host": "127.0.0.1", "port": port},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create connection status = %d, body = %s", resp.status, resp.body)
	}
	var conn Connection
	resp.decode(t, &conn)

	resp = e.request(http.MethodPost, "/connections/"+conn.ID+"/secsgem-browse", token, nil)
	cancel()
	<-simDone
	if resp.status != http.StatusOK {
		t.Fatalf("secsgem-browse status = %d, body = %s", resp.status, resp.body)
	}
	var result SecsgemBrowseResult
	resp.decode(t, &result)
	if !result.OK {
		t.Fatalf("expected ok=true, got %+v", result)
	}
	if len(result.SVIDs) != 1 || result.SVIDs[0].SVID != 4001 || result.SVIDs[0].Name != "BeltSpeed" {
		t.Errorf("got %+v, want [{4001 BeltSpeed mm/s}]", result.SVIDs)
	}
}

func TestMAP100_SecsgemBrowseWrongConnectionTypeFails(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	project := createProjectOnly(t, e, token)

	resp := e.request(http.MethodPost, "/projects/"+project.ID+"/connections", token, map[string]any{
		"name": "some-http-conn", "type": "http", "config": map[string]any{},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create connection status = %d, body = %s", resp.status, resp.body)
	}
	var conn Connection
	resp.decode(t, &conn)

	resp = e.request(http.MethodPost, "/connections/"+conn.ID+"/secsgem-browse", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("secsgem-browse status = %d, body = %s", resp.status, resp.body)
	}
	var result SecsgemBrowseResult
	resp.decode(t, &result)
	if result.OK {
		t.Error("expected ok=false for a non-secsgem connection")
	}
}

func TestMAP100_SecsgemBrowseRequiresEditor(t *testing.T) {
	e := newTestEnv(t)
	owner := e.createUserAndLogin("owner", "")
	project := createProjectOnly(t, e, owner)

	resp := e.request(http.MethodPost, "/projects/"+project.ID+"/connections", owner, map[string]any{
		"name": "sim-equip", "type": "secsgem",
		"config": map[string]any{"mode": "active", "host": "127.0.0.1", "port": 1},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create connection status = %d, body = %s", resp.status, resp.body)
	}
	var conn Connection
	resp.decode(t, &conn)

	viewer := e.createUserAndLogin("viewer", "")
	if err := e.authStore.SetProjectRole(context.Background(), project.ID, mustUserID(t, e, viewer), auth.RoleViewer); err != nil {
		t.Fatalf("SetProjectRole: %v", err)
	}

	resp = e.request(http.MethodPost, "/connections/"+conn.ID+"/secsgem-browse", viewer, nil)
	if resp.status != http.StatusForbidden {
		t.Errorf("viewer browsing a connection status = %d, want 403", resp.status)
	}
}
