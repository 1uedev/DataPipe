// Package conntest implements CON-140's "test connection" button: given a
// connection's type and (already-decrypted, by the caller) config and
// credential, attempts a real, bounded connectivity check. Runs entirely
// in the control plane process — it already carries the necessary
// PostgreSQL and MQTT client libraries, so no runtime round-trip is
// needed. Connection types without a concrete endpoint to probe (e.g.
// auth-only HTTP connections) report success with an explanatory message
// rather than failing.
package conntest

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/1uedev/DataPipe/engine/nodes/mqttshared"
	"github.com/1uedev/DataPipe/engine/nodes/sqlshared"
)

// Timeout bounds how long a single test may take — this is invoked
// synchronously from an editor button click.
const Timeout = 10 * time.Second

// Result is CON-140's outcome: a browsable pass/fail with a human-readable
// reason.
type Result struct {
	OK      bool   `json:"ok"`
	Message string `json:"message"`
}

// Test attempts a live connectivity check for connType, using config
// (non-secret) and credential (already decrypted — never logged or
// returned beyond this process boundary).
func Test(ctx context.Context, connType string, config, credential json.RawMessage) Result {
	ctx, cancel := context.WithTimeout(ctx, Timeout)
	defer cancel()

	switch connType {
	case "postgres":
		return testPostgres(ctx, config, credential)
	case "mqtt":
		return testMQTT(ctx, config, credential)
	default:
		return Result{OK: true, Message: "no live test available for this connection type; config was accepted as-is"}
	}
}

func testPostgres(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg sqlshared.Config
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.Host == "" || cfg.Database == "" {
		return Result{OK: false, Message: "host and database are required"}
	}
	port := cfg.Port
	if port == 0 {
		port = 5432
	}
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}
	var cred sqlshared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}

	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		url.QueryEscape(cred.Username), url.QueryEscape(cred.Password), cfg.Host, port, cfg.Database, sslMode)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	defer func() { _ = db.Close() }()

	if err := db.PingContext(ctx); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	return Result{OK: true, Message: "connected successfully"}
}

func testMQTT(ctx context.Context, config, credential json.RawMessage) Result {
	var cfg mqttshared.BrokerConfig
	if err := json.Unmarshal(config, &cfg); err != nil {
		return Result{OK: false, Message: "invalid config: " + err.Error()}
	}
	if cfg.BrokerURL == "" {
		return Result{OK: false, Message: "brokerUrl is required"}
	}
	var cred mqttshared.Credential
	if len(credential) > 0 {
		if err := json.Unmarshal(credential, &cred); err != nil {
			return Result{OK: false, Message: "invalid credential: " + err.Error()}
		}
	}

	opts := mqtt.NewClientOptions().
		AddBroker(cfg.BrokerURL).
		SetClientID("datapipe-conntest-" + mqttshared.RandSuffix()).
		SetConnectTimeout(Timeout)
	if cred.Username != "" {
		opts.SetUsername(cred.Username)
		opts.SetPassword(cred.Password)
	}
	client := mqtt.NewClient(opts)
	token := client.Connect()
	waited := token.WaitTimeout(Timeout)
	defer client.Disconnect(100)

	if !waited {
		return Result{OK: false, Message: "connect timed out"}
	}
	if err := token.Error(); err != nil {
		return Result{OK: false, Message: err.Error()}
	}
	_ = ctx // the paho client manages its own timeout via SetConnectTimeout above
	return Result{OK: true, Message: "connected successfully"}
}
