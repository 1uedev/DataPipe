package api

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"

	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
	"github.com/1uedev/DataPipe/controlplane/internal/db"
)

func newTestResolverEnv(t *testing.T) (*Store, *crypto.Vault) {
	t.Helper()
	d, err := db.Open(":memory:")
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	t.Cleanup(func() { _ = d.Close() })
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("db.Migrate: %v", err)
	}

	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatalf("generating test master key: %v", err)
	}
	vault, err := crypto.NewVault(key)
	if err != nil {
		t.Fatalf("crypto.NewVault: %v", err)
	}
	return NewStore(d), vault
}

func TestCON110_ConnectionResolverDecryptsCredential(t *testing.T) {
	store, vault := newTestResolverEnv(t)
	ctx := context.Background()

	project, err := store.CreateProject(ctx, "p", "")
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	sealed, err := vault.Seal([]byte(`{"username":"u","password":"secret-pass"}`))
	if err != nil {
		t.Fatalf("Seal: %v", err)
	}
	cred, err := store.CreateCredential(ctx, project.ID, "mqtt-cred", sealed)
	if err != nil {
		t.Fatalf("CreateCredential: %v", err)
	}

	conn, err := store.CreateConnection(ctx, project.ID, "broker", "mqtt", json.RawMessage(`{"broker":"tcp://localhost:1883"}`), &cred.ID)
	if err != nil {
		t.Fatalf("CreateConnection: %v", err)
	}

	resolver := NewConnectionResolver(store, vault)
	info, err := resolver.ResolveConnection(ctx, conn.ID)
	if err != nil {
		t.Fatalf("ResolveConnection: %v", err)
	}
	if info.Type != "mqtt" {
		t.Errorf("Type = %q, want mqtt", info.Type)
	}
	if string(info.ConfigJSON) != `{"broker":"tcp://localhost:1883"}` {
		t.Errorf("ConfigJSON = %s", info.ConfigJSON)
	}
	if string(info.CredentialJSON) != `{"username":"u","password":"secret-pass"}` {
		t.Errorf("CredentialJSON = %s, want the decrypted value", info.CredentialJSON)
	}
}

func TestCON110_ConnectionResolverWithoutCredentialLeavesNilJSON(t *testing.T) {
	store, vault := newTestResolverEnv(t)
	ctx := context.Background()

	project, err := store.CreateProject(ctx, "p", "")
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}
	conn, err := store.CreateConnection(ctx, project.ID, "schedule", "schedule", json.RawMessage(`{}`), nil)
	if err != nil {
		t.Fatalf("CreateConnection: %v", err)
	}

	resolver := NewConnectionResolver(store, vault)
	info, err := resolver.ResolveConnection(ctx, conn.ID)
	if err != nil {
		t.Fatalf("ResolveConnection: %v", err)
	}
	if info.CredentialJSON != nil {
		t.Errorf("CredentialJSON = %q, want nil", info.CredentialJSON)
	}
}

func TestCON110_ConnectionResolverUnknownConnectionErrors(t *testing.T) {
	store, vault := newTestResolverEnv(t)
	resolver := NewConnectionResolver(store, vault)
	if _, err := resolver.ResolveConnection(context.Background(), "does-not-exist"); err == nil {
		t.Fatal("expected an error for an unknown connection id")
	}
}
