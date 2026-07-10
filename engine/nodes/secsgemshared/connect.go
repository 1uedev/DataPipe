// Package secsgemshared is the connection-establishment code shared by
// "secsgem-in" (CON-220) and "secsgem-out" (SNK-130): resolving a node's
// connection into HSMS dial/listen parameters and this host's own
// identity, then opening the session with the shared reconnect/backoff
// helper (CON-130) for the "active" (host dials equipment) direction —
// "passive" (equipment dials in) has no equivalent retry concept, since a
// Listen either accepts a connection or it doesn't.
//
// Known limitation (see TODO.md): each node using a "secsgem" connection
// opens its own independent HSMS session, exactly like mqttshared's "each
// node gets its own client connection" precedent. Real SECS/GEM equipment
// conventionally accepts only one active host session at a time, so
// wiring both a secsgem-in and a secsgem-out node to the same connection
// in one flow may fail with a Select rejection on whichever connects
// second — sharing one persistent session across nodes is tracked as
// follow-up work.
package secsgemshared

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
	"github.com/1uedev/DataPipe/engine/nodes/hsms"
)

// Config is a "secsgem" connection's non-secret config.
type Config struct {
	Mode       string        `json:"mode"` // "active" (dial the equipment) | "passive" (listen for the equipment)
	Host       string        `json:"host,omitempty"`
	Port       int           `json:"port"`
	ListenHost string        `json:"listenHost,omitempty"` // mode "passive" only; default "0.0.0.0"
	SessionID  int           `json:"sessionId,omitempty"`
	MDLN       string        `json:"mdln,omitempty"`
	SoftRev    string        `json:"softRev,omitempty"`
	Timers     *TimersConfig `json:"timers,omitempty"`
}

// TimersConfig overrides hsms.DefaultTimers() per-field; a zero/absent
// field keeps the default.
type TimersConfig struct {
	T3Ms       int `json:"t3Ms,omitempty"`
	T5Ms       int `json:"t5Ms,omitempty"`
	T6Ms       int `json:"t6Ms,omitempty"`
	T7Ms       int `json:"t7Ms,omitempty"`
	T8Ms       int `json:"t8Ms,omitempty"`
	LinktestMs int `json:"linktestMs,omitempty"`
}

// HSMSTimers builds hsms.Timers from Config, defaulting unset fields.
func (c Config) HSMSTimers() hsms.Timers {
	t := hsms.DefaultTimers()
	if c.Timers == nil {
		return t
	}
	if c.Timers.T3Ms > 0 {
		t.T3 = time.Duration(c.Timers.T3Ms) * time.Millisecond
	}
	if c.Timers.T5Ms > 0 {
		t.T5 = time.Duration(c.Timers.T5Ms) * time.Millisecond
	}
	if c.Timers.T6Ms > 0 {
		t.T6 = time.Duration(c.Timers.T6Ms) * time.Millisecond
	}
	if c.Timers.T7Ms > 0 {
		t.T7 = time.Duration(c.Timers.T7Ms) * time.Millisecond
	}
	if c.Timers.T8Ms > 0 {
		t.T8 = time.Duration(c.Timers.T8Ms) * time.Millisecond
	}
	if c.Timers.LinktestMs > 0 {
		t.Linktest = time.Duration(c.Timers.LinktestMs) * time.Millisecond
	}
	return t
}

// Addr returns the dial (mode "active") or listen (mode "passive") address.
func (c Config) Addr() string {
	if c.Mode == "passive" {
		host := c.ListenHost
		if host == "" {
			host = "0.0.0.0"
		}
		return fmt.Sprintf("%s:%d", host, c.Port)
	}
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// Identity returns this host's configured MDLN/SoftRev, defaulting to
// "DataPipe"/"1.0" when unset.
func (c Config) Identity() (mdln, softrev string) {
	mdln, softrev = c.MDLN, c.SoftRev
	if mdln == "" {
		mdln = "DataPipe"
	}
	if softrev == "" {
		softrev = "1.0"
	}
	return mdln, softrev
}

// ConnectTimeout bounds a single active-mode dial attempt before it's
// counted as a failure and retried with backoff.
const ConnectTimeout = 10 * time.Second

// Connect resolves the calling node's connection and opens an HSMS
// session: mode "active" dials with the shared backoff helper until ctx is
// cancelled; mode "passive" listens once (no retry concept for accepting
// a connection).
func Connect(ctx context.Context) (*hsms.Conn, Config, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return nil, Config{}, fmt.Errorf("secsgemshared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return nil, Config{}, fmt.Errorf("secsgemshared: parsing connection config: %w", err)
	}
	if cfg.Port == 0 {
		return nil, cfg, fmt.Errorf("secsgemshared: port is required")
	}
	timers := cfg.HSMSTimers()
	sessionID := uint16(cfg.SessionID)

	if cfg.Mode == "passive" {
		conn, err := hsms.Listen(ctx, cfg.Addr(), sessionID, timers)
		if err != nil {
			return nil, cfg, fmt.Errorf("secsgemshared: %w", err)
		}
		return conn, cfg, nil
	}

	if cfg.Host == "" {
		return nil, cfg, fmt.Errorf("secsgemshared: host is required for mode \"active\"")
	}
	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			return nil, cfg, ctx.Err()
		}
		dialCtx, cancel := context.WithTimeout(ctx, ConnectTimeout)
		conn, err := hsms.Dial(dialCtx, cfg.Addr(), sessionID, timers)
		cancel()
		if err == nil {
			return conn, cfg, nil
		}
		select {
		case <-ctx.Done():
			return nil, cfg, ctx.Err()
		case <-time.After(bo.Next()):
		}
	}
}
