package ctxstore

import (
	"context"
	"errors"
	"testing"
)

func TestENG120_ScopedGetSetDelete(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	nodeKey := Key{Scope: ScopeNode, FlowID: "flow-1", NodeID: "node-1", Name: "counter"}
	flowKey := Key{Scope: ScopeFlow, FlowID: "flow-1", Name: "total"}
	globalKey := Key{Scope: ScopeGlobal, Name: "config"}

	for _, k := range []Key{nodeKey, flowKey, globalKey} {
		if _, found, err := s.Get(ctx, k); err != nil || found {
			t.Fatalf("Get(%+v) before Set = found=%v err=%v, want not found", k, found, err)
		}
	}

	if err := s.Set(ctx, nodeKey, 1); err != nil {
		t.Fatalf("Set node: %v", err)
	}
	if err := s.Set(ctx, flowKey, 2); err != nil {
		t.Fatalf("Set flow: %v", err)
	}
	if err := s.Set(ctx, globalKey, 3); err != nil {
		t.Fatalf("Set global: %v", err)
	}

	v, found, err := s.Get(ctx, nodeKey)
	if err != nil || !found || v != 1 {
		t.Fatalf("Get(nodeKey) = %v, %v, %v; want 1, true, nil", v, found, err)
	}

	if err := s.Delete(ctx, nodeKey); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := s.Get(ctx, nodeKey); found {
		t.Error("node key should be gone after Delete")
	}
	// Other scopes must be unaffected by deleting the node key.
	if _, found, _ := s.Get(ctx, flowKey); !found {
		t.Error("flow key must survive deleting an unrelated node key")
	}
}

func TestENG120_ScopesAreIsolatedByFlowAndNode(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	keyA := Key{Scope: ScopeNode, FlowID: "flow-1", NodeID: "node-A", Name: "x"}
	keyB := Key{Scope: ScopeNode, FlowID: "flow-1", NodeID: "node-B", Name: "x"}
	keyOtherFlow := Key{Scope: ScopeNode, FlowID: "flow-2", NodeID: "node-A", Name: "x"}

	if err := s.Set(ctx, keyA, "a"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, keyB, "b"); err != nil {
		t.Fatal(err)
	}
	if err := s.Set(ctx, keyOtherFlow, "other"); err != nil {
		t.Fatal(err)
	}

	v, _, _ := s.Get(ctx, keyA)
	if v != "a" {
		t.Errorf("keyA = %v, want %q (same-name keys in different nodes must not collide)", v, "a")
	}
	v, _, _ = s.Get(ctx, keyB)
	if v != "b" {
		t.Errorf("keyB = %v, want %q", v, "b")
	}
	v, _, _ = s.Get(ctx, keyOtherFlow)
	if v != "other" {
		t.Errorf("keyOtherFlow = %v, want %q (same node id in a different flow must not collide)", v, "other")
	}
}

func TestENG120_DeleteUnknownKeyReturnsNotFound(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	err := s.Delete(ctx, Key{Scope: ScopeGlobal, Name: "nope"})
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Delete unknown key error = %v, want ErrNotFound", err)
	}
}

func TestENG120_KeysListingIsScopedAndSorted(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	for _, name := range []string{"zeta", "alpha", "mid"} {
		if err := s.Set(ctx, Key{Scope: ScopeNode, FlowID: "f", NodeID: "n", Name: name}, true); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Set(ctx, Key{Scope: ScopeNode, FlowID: "f", NodeID: "other-node", Name: "unrelated"}, true); err != nil {
		t.Fatal(err)
	}

	keys, err := s.Keys(ctx, ScopeNode, "f", "n")
	if err != nil {
		t.Fatalf("Keys: %v", err)
	}
	want := []string{"alpha", "mid", "zeta"}
	if len(keys) != len(want) {
		t.Fatalf("Keys() = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("Keys()[%d] = %q, want %q", i, keys[i], want[i])
		}
	}
}

func TestENG120_StateSurvivesAcrossNodeInstancesWithSameID(t *testing.T) {
	// Simulates a redeploy: the store outlives any particular node instance
	// as long as flow/node ids are unchanged (ENG-120).
	ctx := context.Background()
	s := NewMemoryStore()
	key := Key{Scope: ScopeNode, FlowID: "flow-1", NodeID: "node-1", Name: "counter"}

	if err := s.Set(ctx, key, 42); err != nil {
		t.Fatal(err)
	}

	// "Redeploy": a brand new node instance is created with the same id and
	// reads from the same store.
	v, found, err := s.Get(ctx, key)
	if err != nil || !found || v != 42 {
		t.Fatalf("state did not survive redeploy: v=%v found=%v err=%v", v, found, err)
	}
}

func TestENG120_InvalidKeyRejected(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	cases := []Key{
		{Scope: ScopeNode, FlowID: "f", Name: "x"}, // missing NodeID
		{Scope: ScopeFlow, Name: "x"},              // missing FlowID
		{Scope: ScopeGlobal, Name: ""},             // missing Name
		{Scope: "bogus", FlowID: "f", NodeID: "n", Name: "x"},
	}
	for _, k := range cases {
		if err := s.Set(ctx, k, 1); err == nil {
			t.Errorf("Set(%+v) should have failed validation", k)
		}
	}
}
