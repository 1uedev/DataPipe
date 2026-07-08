// Package opcuashared is the connection-establishment code shared by
// "opcua-source" and "opcua-sink" (CON-210): OPC-UA client setup (security
// policy/mode, anonymous or username/password auth) and status-code ->
// datagram.Quality mapping. Not verified against a real or simulated
// OPC-UA server in this environment (none available) — see TODO.md;
// gopcua/opcua is a widely-used, spec-conformant pure-Go implementation, so
// the wire protocol itself is not reimplemented or guessed at here.
package opcuashared

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gopcua/opcua"
	"github.com/gopcua/opcua/ua"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

// Config is an "opcua" connection's non-secret config.
type Config struct {
	Endpoint       string `json:"endpoint"`                 // e.g. "opc.tcp://host:4840"
	SecurityPolicy string `json:"securityPolicy,omitempty"` // default "None"; e.g. "Basic256Sha256"
	SecurityMode   string `json:"securityMode,omitempty"`   // "None" (default) | "Sign" | "SignAndEncrypt"
	Auth           string `json:"auth,omitempty"`           // "anonymous" (default) | "username"
	CertFile       string `json:"certFile,omitempty"`       // client certificate, required by non-None security policies
	KeyFile        string `json:"keyFile,omitempty"`
}

// Credential is an "opcua" connection's credential shape (used when
// Auth == "username").
type Credential struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// Connect resolves the calling node's connection and dials an OPC-UA
// client, establishing the secure channel and session.
func Connect(ctx context.Context) (*opcua.Client, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("opcuashared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return nil, fmt.Errorf("opcuashared: parsing connection config: %w", err)
	}
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("opcuashared: connection config requires endpoint")
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("opcuashared: parsing credential: %w", err)
		}
	}

	opts := []opcua.Option{
		opcua.SecurityPolicy(orDefault(cfg.SecurityPolicy, "None")),
		opcua.SecurityModeString(orDefault(cfg.SecurityMode, "None")),
	}
	if cfg.CertFile != "" {
		opts = append(opts, opcua.CertificateFile(cfg.CertFile))
	}
	if cfg.KeyFile != "" {
		opts = append(opts, opcua.PrivateKeyFile(cfg.KeyFile))
	}
	switch orDefault(cfg.Auth, "anonymous") {
	case "anonymous":
		opts = append(opts, opcua.AuthAnonymous())
	case "username":
		opts = append(opts, opcua.AuthUsername(cred.Username, cred.Password))
	default:
		return nil, fmt.Errorf("opcuashared: unknown auth %q", cfg.Auth)
	}

	client, err := opcua.NewClient(cfg.Endpoint, opts...)
	if err != nil {
		return nil, fmt.Errorf("opcuashared: creating client: %w", err)
	}
	if err := client.Connect(ctx); err != nil {
		return nil, fmt.Errorf("opcuashared: connecting to %s: %w", cfg.Endpoint, err)
	}
	return client, nil
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// QualityOf maps an OPC-UA status code's severity (Part 4 Table 168: bits
// 31-30 of the code) onto datagram.Quality, per CON-210's "status codes map
// to datagram quality".
func QualityOf(code ua.StatusCode) datagram.Quality {
	switch uint32(code) >> 30 {
	case 0:
		return datagram.QualityGood
	case 1:
		return datagram.QualityUncertain
	default: // 2 (reserved, treated conservatively as bad) or 3 (Bad)
		return datagram.QualityBad
	}
}
