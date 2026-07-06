package api

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/audit"
	"github.com/1uedev/DataPipe/controlplane/internal/auth"
	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
	"github.com/1uedev/DataPipe/controlplane/internal/db"

	// Registers the "inject"/"set"/"debug-log" node types so
	// engine/flow.Validate accepts test flow content, exactly as
	// cmd/controlplane and cmd/runtime do in production.
	_ "github.com/1uedev/DataPipe/engine/nodes/debuglog"
	_ "github.com/1uedev/DataPipe/engine/nodes/inject"
	_ "github.com/1uedev/DataPipe/engine/nodes/set"
)

type fakeDeployer struct {
	fail       bool
	deployedTo []deployCall
}

type deployCall struct {
	FlowID  string
	Version int64
	Content string
}

var errFakeDeployUnavailable = errors.New("no runtime connected (test double)")

func (f *fakeDeployer) DeployFlow(ctx context.Context, flowID string, version int64, flowJSON string) error {
	if f.fail {
		return errFakeDeployUnavailable
	}
	f.deployedTo = append(f.deployedTo, deployCall{FlowID: flowID, Version: version, Content: flowJSON})
	return nil
}

type fakeRuntimeLister struct{ runtimes []RuntimeInfo }

func (f *fakeRuntimeLister) ListRuntimes() []RuntimeInfo { return f.runtimes }

type testEnv struct {
	t         *testing.T
	authStore *auth.Store
	auditLog  *audit.Log
	deployer  *fakeDeployer
	server    *httptest.Server
}

func newTestEnv(t *testing.T) *testEnv {
	t.Helper()
	d, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	authStore := auth.NewStore(d)
	auditLog := audit.NewLog(d)
	store := NewStore(d)

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generating test master key: %v", err)
	}
	vault, err := crypto.NewVault(key)
	if err != nil {
		t.Fatalf("crypto.NewVault: %v", err)
	}

	deployer := &fakeDeployer{}
	handlers := NewHandlers(store, authStore, vault, auditLog, deployer, &fakeRuntimeLister{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	server := httptest.NewServer(handlers.Routes())
	t.Cleanup(server.Close)

	return &testEnv{t: t, authStore: authStore, auditLog: auditLog, deployer: deployer, server: server}
}

// createUserAndLogin creates a local account and returns a bearer token for it.
func (e *testEnv) createUserAndLogin(username string, systemRole auth.SystemRole) string {
	e.t.Helper()
	const password = "correct-horse-battery-staple"
	u, err := e.authStore.CreateUser(context.Background(), username, password, systemRole)
	if err != nil {
		e.t.Fatalf("CreateUser: %v", err)
	}
	token, _, err := e.authStore.CreateSession(context.Background(), u.ID)
	if err != nil {
		e.t.Fatalf("CreateSession: %v", err)
	}
	return token
}

type apiResponse struct {
	status int
	body   []byte
}

func (r apiResponse) decode(t *testing.T, v any) {
	t.Helper()
	if err := json.Unmarshal(r.body, v); err != nil {
		t.Fatalf("decoding response body %s: %v", r.body, err)
	}
}

func (e *testEnv) request(method, path, token string, body any) apiResponse {
	e.t.Helper()
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			e.t.Fatalf("marshal request body: %v", err)
		}
		reader = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, e.server.URL+path, reader)
	if err != nil {
		e.t.Fatalf("NewRequest: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		e.t.Fatalf("reading response body: %v", err)
	}
	return apiResponse{status: resp.StatusCode, body: respBody}
}

// sampleFlowContent is a valid inject -> set -> debug-log flow (matching
// examples/inject-set-log.flow.json), used across lifecycle tests.
func sampleFlowContent() json.RawMessage {
	return json.RawMessage(`{
		"formatVersion": 1,
		"kind": "flow",
		"id": "flow_test",
		"name": "test flow",
		"mode": "streaming",
		"graph": {
			"nodes": [
				{"id": "n1", "type": "inject", "typeVersion": 1, "config": {"payload": {"value": 1}, "repeatMs": 0}},
				{"id": "n2", "type": "set", "typeVersion": 1, "config": {"sets": [{"path": "status", "value": "processed"}]}},
				{"id": "n3", "type": "debug-log", "typeVersion": 1, "config": {}}
			],
			"wires": [
				{"id": "w1", "from": {"node": "n1", "port": "out"}, "to": {"node": "n2", "port": "in"}},
				{"id": "w2", "from": {"node": "n2", "port": "out"}, "to": {"node": "n3", "port": "in"}}
			]
		}
	}`)
}
