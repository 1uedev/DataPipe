// Connection resolution (Increment 6, CON-110/SEC-120): the runtime-facing
// consumer of Store + crypto.Vault that registry.Service.ResolveConnection
// calls into. This is the one legitimate place a credential is decrypted
// outside a REST handler — and only ever to hand the plaintext to the
// runtime that actually needs it to connect, never through any API
// response, export, or audit entry.
package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/1uedev/DataPipe/controlplane/internal/crypto"
)

// ConnectionInfo mirrors registry.ConnectionInfo structurally; Go's
// structural typing lets ConnectionResolver satisfy registry's
// ConnectionResolver interface without this package importing registry.
type ConnectionInfo struct {
	Type           string
	ConfigJSON     json.RawMessage
	CredentialJSON json.RawMessage // nil if the connection has no credential
}

// ConnectionResolver resolves a connection id into its live config and
// (if it references one) decrypted credential.
type ConnectionResolver struct {
	store *Store
	vault *crypto.Vault
}

func NewConnectionResolver(store *Store, vault *crypto.Vault) *ConnectionResolver {
	return &ConnectionResolver{store: store, vault: vault}
}

func (r *ConnectionResolver) ResolveConnection(ctx context.Context, connectionID string) (ConnectionInfo, error) {
	conn, err := r.store.GetConnection(ctx, connectionID)
	if errors.Is(err, sql.ErrNoRows) {
		return ConnectionInfo{}, fmt.Errorf("connresolve: connection %q not found", connectionID)
	}
	if err != nil {
		return ConnectionInfo{}, fmt.Errorf("connresolve: loading connection: %w", err)
	}

	info := ConnectionInfo{Type: conn.Type, ConfigJSON: conn.Config}
	if conn.CredentialID != nil {
		sealed, err := r.store.GetCredentialSealed(ctx, *conn.CredentialID)
		if err != nil {
			return ConnectionInfo{}, fmt.Errorf("connresolve: loading credential: %w", err)
		}
		plaintext, err := r.vault.Open(*sealed)
		if err != nil {
			return ConnectionInfo{}, fmt.Errorf("connresolve: decrypting credential: %w", err)
		}
		info.CredentialJSON = plaintext
	}
	return info, nil
}
