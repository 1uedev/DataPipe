package flow

import (
	"context"
	"testing"

	"github.com/1uedev/DataPipe/engine/ctxstore"
)

func TestPROC410_ContextStoreRoundTrip(t *testing.T) {
	if _, ok := ContextStore(context.Background()); ok {
		t.Fatal("expected no store attached to a bare context")
	}

	store := ctxstore.NewMemoryStore()
	ctx := WithContextStore(context.Background(), store)
	got, ok := ContextStore(ctx)
	if !ok || got != store {
		t.Fatalf("ContextStore() = %v, %v; want the same store instance", got, ok)
	}
}

func TestPROC410_DeploymentDefaultsToAMemoryStoreAndAcceptsOverride(t *testing.T) {
	d := NewDeployment(testLogger())
	defer d.Stop()
	if d.ctxStore == nil {
		t.Fatal("expected a default context store")
	}
	custom := ctxstore.NewMemoryStore()
	d.SetContextStore(custom)
	if d.ctxStore != custom {
		t.Fatal("SetContextStore did not take effect")
	}
	d.SetContextStore(nil)
	if d.ctxStore == nil {
		t.Fatal("SetContextStore(nil) should reset to a fresh MemoryStore, not leave it nil")
	}
}
