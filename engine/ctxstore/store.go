// Package ctxstore implements the node/flow/global context store backing
// PROC-410 (ENG-120): "node/flow/global scopes; pluggable persistence;
// state survives redeploys when the node id is unchanged; state is
// inspectable and deletable through the editor and API."
package ctxstore

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
)

// Scope is the visibility level of a stored value.
type Scope string

const (
	ScopeNode   Scope = "node"
	ScopeFlow   Scope = "flow"
	ScopeGlobal Scope = "global"
)

// ErrNotFound is returned by Delete when the key does not exist.
var ErrNotFound = errors.New("ctxstore: key not found")

// Key addresses a single stored value. FlowID is required for ScopeFlow and
// ScopeNode; NodeID is required for ScopeNode only. State keeps surviving
// redeploys as long as FlowID/NodeID stay the same, since the Store is
// independent of any particular node instance's lifecycle.
type Key struct {
	Scope  Scope
	FlowID string
	NodeID string
	Name   string
}

func (k Key) validate() error {
	if k.Name == "" {
		return fmt.Errorf("ctxstore: key name is required")
	}
	switch k.Scope {
	case ScopeGlobal:
		return nil
	case ScopeFlow:
		if k.FlowID == "" {
			return fmt.Errorf("ctxstore: flow scope requires FlowID")
		}
		return nil
	case ScopeNode:
		if k.FlowID == "" || k.NodeID == "" {
			return fmt.Errorf("ctxstore: node scope requires FlowID and NodeID")
		}
		return nil
	default:
		return fmt.Errorf("ctxstore: unknown scope %q", k.Scope)
	}
}

// Store is the pluggable persistence interface (ENG-120: "pluggable
// persistence"); MemoryStore is the Increment 1 backend, a durable
// (Postgres-backed) implementation can be added later without changing
// callers.
type Store interface {
	Get(ctx context.Context, key Key) (value any, found bool, err error)
	Set(ctx context.Context, key Key, value any) error
	Delete(ctx context.Context, key Key) error
	// Keys lists the names stored under the given scope (and FlowID/NodeID
	// where applicable), for state inspection in the editor/API.
	Keys(ctx context.Context, scope Scope, flowID, nodeID string) ([]string, error)
}

// MemoryStore is an in-process, non-durable Store implementation.
type MemoryStore struct {
	mu     sync.RWMutex
	values map[Key]any
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{values: make(map[Key]any)}
}

func (s *MemoryStore) Get(_ context.Context, key Key) (any, bool, error) {
	if err := key.validate(); err != nil {
		return nil, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.values[key]
	return v, ok, nil
}

func (s *MemoryStore) Set(_ context.Context, key Key, value any) error {
	if err := key.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.values[key] = value
	return nil
}

func (s *MemoryStore) Delete(_ context.Context, key Key) error {
	if err := key.validate(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.values[key]; !ok {
		return ErrNotFound
	}
	delete(s.values, key)
	return nil
}

func (s *MemoryStore) Keys(_ context.Context, scope Scope, flowID, nodeID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var names []string
	for k := range s.values {
		if k.Scope != scope || k.FlowID != flowID || k.NodeID != nodeID {
			continue
		}
		names = append(names, k.Name)
	}
	sort.Strings(names)
	return names, nil
}
