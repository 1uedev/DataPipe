// Package kafkashared is the connection-resolution code shared by
// "kafka-in" and "kafka-out" (CON-260): unlike the other shared connectors,
// there is no persistent "dial" step to retry here — kafka-go's Reader/
// Writer establish and retry their own broker connections internally per
// message/fetch (CON-130's reconnect concern is already handled inside the
// library), so Resolve just turns a connection id into brokers + a Dialer
// with TLS/SASL applied.
package kafkashared

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"

	"github.com/1uedev/DataPipe/engine/flow"
)

// Config is a "kafka" connection's non-secret config.
type Config struct {
	Brokers []string `json:"brokers"`
	TLS     bool     `json:"tls,omitempty"`
}

// Credential is a "kafka" connection's credential shape (SASL/PLAIN; empty
// Username means no SASL auth — a plaintext/unauthenticated broker).
type Credential struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// Resolved is a resolved kafka connection: brokers plus a Dialer carrying
// TLS/SASL, ready to build a Reader or Writer.
type Resolved struct {
	Brokers []string
	Dialer  *kafka.Dialer
}

// Resolve resolves the calling node's configured connection into brokers and
// a Dialer.
func Resolve(ctx context.Context) (Resolved, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return Resolved{}, fmt.Errorf("kafkashared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return Resolved{}, fmt.Errorf("kafkashared: parsing connection config: %w", err)
	}
	if len(cfg.Brokers) == 0 {
		return Resolved{}, fmt.Errorf("kafkashared: connection config requires at least one broker")
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return Resolved{}, fmt.Errorf("kafkashared: parsing credential: %w", err)
		}
	}

	dialer := &kafka.Dialer{DualStack: true}
	if cfg.TLS {
		dialer.TLS = &tls.Config{} //nolint:gosec // operator-configured broker; no client cert pinning at this increment
	}
	if cred.Username != "" {
		dialer.SASLMechanism = plain.Mechanism{Username: cred.Username, Password: cred.Password}
	}
	return Resolved{Brokers: cfg.Brokers, Dialer: dialer}, nil
}
