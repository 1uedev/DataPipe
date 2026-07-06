// Package mqttshared is the connection-establishment code shared by
// "mqtt-in" (CON-200) and "mqtt-out" (SNK-110): resolving a node's
// connection into broker URL + optional credential, and connecting with
// the shared reconnect/backoff helper (CON-130 — "connectors ... never own
// retry loops") for the initial connect. Reconnection after a live drop is
// then handled by the MQTT client library's own AutoReconnect.
package mqttshared

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

// RandSuffix returns a short random hex string, used to give each node
// instance its own MQTT client id when sharing a connection's broker
// config.
func RandSuffix() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// BrokerConfig is a "mqtt" connection's non-secret config.
type BrokerConfig struct {
	BrokerURL      string `json:"brokerUrl"`
	ClientIDPrefix string `json:"clientIdPrefix,omitempty"`
}

// Credential is a "mqtt" connection's optional credential shape.
type Credential struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// ConnectTimeout bounds a single connect attempt before it's counted as a
// failure and retried with backoff.
const ConnectTimeout = 10 * time.Second

// Connect resolves the calling node's connection and connects to the
// broker, retrying with exponential backoff+jitter until ctx is cancelled.
// clientIDSuffix disambiguates multiple nodes sharing one broker connection
// config (each gets its own MQTT client connection in this increment; see
// TODO.md for connection pooling as a follow-up).
func Connect(ctx context.Context, clientIDSuffix string) (mqtt.Client, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("mqttshared: %w", err)
	}
	var broker BrokerConfig
	if err := json.Unmarshal(info.Config, &broker); err != nil {
		return nil, fmt.Errorf("mqttshared: parsing broker config: %w", err)
	}
	if broker.BrokerURL == "" {
		return nil, fmt.Errorf("mqttshared: connection config is missing brokerUrl")
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("mqttshared: parsing credential: %w", err)
		}
	}

	clientID := broker.ClientIDPrefix
	if clientID == "" {
		clientID = "datapipe"
	}
	clientID += "-" + clientIDSuffix

	opts := mqtt.NewClientOptions().
		AddBroker(broker.BrokerURL).
		SetClientID(clientID).
		SetAutoReconnect(true).
		SetConnectTimeout(ConnectTimeout)
	if cred.Username != "" {
		opts.SetUsername(cred.Username)
		opts.SetPassword(cred.Password)
	}

	client := mqtt.NewClient(opts)
	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		token := client.Connect()
		if token.WaitTimeout(ConnectTimeout) && token.Error() == nil {
			return client, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(bo.Next()):
		}
	}
}
