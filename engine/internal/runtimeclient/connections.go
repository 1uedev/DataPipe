// Connection resolution (Increment 6, CON-110/SEC-120): a connector node
// asks for its connection's live config/credential via engine/flow's
// context-injected resolver, which calls back here over the
// ResolveConnection RPC — the decrypted credential value travels only over
// this one request/response, never embedded in a deploy push.
package runtimeclient

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	runtimev1 "github.com/1uedev/DataPipe/proto/gen/go/datapipe/runtime/v1"

	"github.com/1uedev/DataPipe/engine/flow"
)

// ConnectionResolver implements flow.ConnectionResolver by calling the
// control plane's ResolveConnection RPC. Safe to attach to a
// flow.Deployment before the runtime has ever registered: ResolveConnection
// simply errors until attach has been called at least once.
type ConnectionResolver struct {
	mu           sync.Mutex
	client       runtimev1.RuntimeRegistryServiceClient
	runtimeID    string
	sessionToken string
}

// NewConnectionResolver creates a resolver with no attached session yet.
func NewConnectionResolver() *ConnectionResolver {
	return &ConnectionResolver{}
}

func (r *ConnectionResolver) attach(client runtimev1.RuntimeRegistryServiceClient, runtimeID, sessionToken string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.client = client
	r.runtimeID = runtimeID
	r.sessionToken = sessionToken
}

// ResolveConnection implements flow.ConnectionResolver.
func (r *ConnectionResolver) ResolveConnection(ctx context.Context, connectionID string) (flow.ConnectionInfo, error) {
	r.mu.Lock()
	client, runtimeID, token := r.client, r.runtimeID, r.sessionToken
	r.mu.Unlock()

	if client == nil {
		return flow.ConnectionInfo{}, fmt.Errorf("runtimeclient: not registered with the control plane yet")
	}

	resp, err := client.ResolveConnection(ctx, &runtimev1.ResolveConnectionRequest{
		RuntimeId: runtimeID, SessionToken: token, ConnectionId: connectionID,
	})
	if err != nil {
		return flow.ConnectionInfo{}, fmt.Errorf("runtimeclient: resolving connection %q: %w", connectionID, err)
	}

	var credential json.RawMessage
	if cj := resp.GetCredentialJson(); cj != "" {
		credential = json.RawMessage(cj)
	}
	return flow.ConnectionInfo{
		Type:           resp.GetType(),
		Config:         json.RawMessage(resp.GetConfigJson()),
		CredentialJSON: credential,
	}, nil
}
