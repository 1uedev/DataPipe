package api

import (
	"bytes"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// TestSEC120_CredentialValueNeverInCreateResponse proves the create
// response is CredentialMeta-shaped only: no "value" field anywhere in the
// raw body, even though the request just sent one.
func TestSEC120_CredentialValueNeverInCreateResponse(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	const secretMarker = "hunter2-super-secret-password"
	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/credentials", token, map[string]any{
		"name":  "db password",
		"value": map[string]string{"username": "admin", "password": secretMarker},
	})
	if resp.status != http.StatusCreated {
		t.Fatalf("create credential status = %d, body = %s", resp.status, resp.body)
	}

	if bytes.Contains(resp.body, []byte(secretMarker)) {
		t.Fatalf("credential value leaked into create response: %s", resp.body)
	}
	if bytes.Contains(bytes.ToLower(resp.body), []byte(`"value"`)) {
		t.Fatalf("create response contains a value field: %s", resp.body)
	}

	var meta CredentialMeta
	resp.decode(t, &meta)
	if meta.ID == "" || meta.Name != "db password" {
		t.Errorf("meta = %+v, want a populated CredentialMeta", meta)
	}
}

func TestSEC120_CredentialValueNeverInListResponse(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	const secretMarker = "another-secret-value-xyz"
	e.request(http.MethodPost, "/projects/"+project.ID+"/credentials", token, map[string]any{
		"name": "cred", "value": map[string]string{"token": secretMarker},
	})

	resp = e.request(http.MethodGet, "/projects/"+project.ID+"/credentials", token, nil)
	if resp.status != http.StatusOK {
		t.Fatalf("list credentials status = %d", resp.status)
	}
	if bytes.Contains(resp.body, []byte(secretMarker)) {
		t.Fatalf("credential value leaked into list response: %s", resp.body)
	}

	var metas []CredentialMeta
	resp.decode(t, &metas)
	if len(metas) != 1 || metas[0].Name != "cred" {
		t.Errorf("metas = %+v, want one entry named \"cred\"", metas)
	}
}

// TestSEC120_CredentialValueNeverInAuditLog proves the audit trail records
// that a credential was created, but never the secret value itself.
func TestSEC120_CredentialValueNeverInAuditLog(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	const secretMarker = "yet-another-secret-abc123"
	e.request(http.MethodPost, "/projects/"+project.ID+"/credentials", token, map[string]any{
		"name": "cred", "value": map[string]string{"apiKey": secretMarker},
	})

	entries, err := e.auditLog.List(t.Context(), project.ID)
	if err != nil {
		t.Fatalf("audit List: %v", err)
	}
	found := false
	for _, entry := range entries {
		if entry.Action != "credential.create" {
			continue
		}
		found = true
		if strings.Contains(fmt.Sprintf("%+v", entry), secretMarker) {
			t.Fatalf("audit entry for credential.create contains the secret value: %+v", entry)
		}
	}
	if !found {
		t.Fatal("expected a credential.create audit entry")
	}
}

// TestSEC120_NoAPIRouteEverExposesADecryptedCredential is a structural
// guard: the OpenAPI contract intentionally has no GET-by-id or export
// route for a credential's value, only create/list-metadata/delete. This
// test documents and enforces that by hitting every plausible "give me the
// value back" path and confirming none of them work.
func TestSEC120_NoAPIRouteEverExposesADecryptedCredential(t *testing.T) {
	e := newTestEnv(t)
	token := e.createUserAndLogin("alice", "")
	resp := e.request(http.MethodPost, "/projects", token, map[string]string{"name": "p"})
	var project Project
	resp.decode(t, &project)

	resp = e.request(http.MethodPost, "/projects/"+project.ID+"/credentials", token, map[string]any{
		"name": "cred", "value": map[string]string{"secret": "s"},
	})
	var meta CredentialMeta
	resp.decode(t, &meta)

	for _, path := range []string{
		"/credentials/" + meta.ID,
		"/credentials/" + meta.ID + "/value",
		"/projects/" + project.ID + "/credentials/" + meta.ID,
	} {
		resp = e.request(http.MethodGet, path, token, nil)
		if resp.status == http.StatusOK {
			t.Errorf("GET %s unexpectedly returned 200 (body: %s) — no route should ever return a decrypted credential", path, resp.body)
		}
	}
}
