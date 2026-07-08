// Package redisshared is the connection-establishment code shared by
// "redis-source" and "redis-sink" (CON-520/SNK-200).
package redisshared

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/internal/backoff"
)

// Config is a "redis" connection's non-secret config.
type Config struct {
	Host string `json:"host"`
	Port int    `json:"port,omitempty"`
	DB   int    `json:"db,omitempty"`
}

// Credential is a "redis" connection's credential shape (Username is
// optional — Redis ACL/6+ auth; classic single-password auth leaves it
// empty).
type Credential struct {
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
}

// PingTimeout bounds a single connectivity check before it's counted as a
// failure and retried with backoff.
const PingTimeout = 5 * time.Second

// Connect resolves the calling node's connection, opens a *redis.Client, and
// retries pinging it with exponential backoff+jitter (CON-130) until ctx is
// cancelled or the connection succeeds.
func Connect(ctx context.Context) (*redis.Client, error) {
	info, err := flow.ResolveConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("redisshared: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(info.Config, &cfg); err != nil {
		return nil, fmt.Errorf("redisshared: parsing connection config: %w", err)
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("redisshared: connection config requires host")
	}
	port := cfg.Port
	if port == 0 {
		port = 6379
	}
	var cred Credential
	if len(info.CredentialJSON) > 0 {
		if err := json.Unmarshal(info.CredentialJSON, &cred); err != nil {
			return nil, fmt.Errorf("redisshared: parsing credential: %w", err)
		}
	}

	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, port),
		Username: cred.Username,
		Password: cred.Password,
		DB:       cfg.DB,
	})

	bo := backoff.New(500*time.Millisecond, 30*time.Second, 2)
	for {
		if ctx.Err() != nil {
			_ = client.Close()
			return nil, ctx.Err()
		}
		pingCtx, cancel := context.WithTimeout(ctx, PingTimeout)
		err := client.Ping(pingCtx).Err()
		cancel()
		if err == nil {
			return client, nil
		}
		select {
		case <-ctx.Done():
			_ = client.Close()
			return nil, ctx.Err()
		case <-time.After(bo.Next()):
		}
	}
}
