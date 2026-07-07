package nodeutil

import (
	"context"
	"os"
	"testing"

	"github.com/1uedev/DataPipe/engine/ctxstore"
	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestMAP130_ExprDataPopulatesPayloadHeaderEnv(t *testing.T) {
	if err := os.Setenv("NODEUTIL_TEST_VAR", "hello"); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Unsetenv("NODEUTIL_TEST_VAR") }()

	in := datagram.New(datagram.Source{NodeID: "n1"}, datagram.Payload{Value: map[string]any{"x": 1}})
	in.Header.Tags = map[string]string{"line": "3"}

	d := ExprData(context.Background(), in)
	if d.Payload.(map[string]any)["x"] != 1 {
		t.Errorf("Payload = %+v", d.Payload)
	}
	if d.Header.Tags["line"] != "3" {
		t.Errorf("Header.Tags = %+v", d.Header.Tags)
	}
	if d.Env["NODEUTIL_TEST_VAR"] != "hello" {
		t.Errorf("Env[NODEUTIL_TEST_VAR] = %q", d.Env["NODEUTIL_TEST_VAR"])
	}
}

func TestMAP130_ExprDataWithoutContextStoreLeavesFlowGlobalNil(t *testing.T) {
	in := datagram.New(datagram.Source{NodeID: "n1"}, datagram.Payload{Value: 1})
	d := ExprData(context.Background(), in)
	if d.FlowGet != nil || d.FlowSet != nil || d.GlobalGet != nil || d.GlobalSet != nil {
		t.Error("expected no flow/global bindings without an attached context store")
	}
}

func TestMAP130_ExprDataGlobalScopeRoundTripsThroughContextStore(t *testing.T) {
	store := ctxstore.NewMemoryStore()
	ctx := flow.WithContextStore(context.Background(), store)
	in := datagram.New(datagram.Source{NodeID: "n1"}, datagram.Payload{Value: 1})
	d := ExprData(ctx, in)

	if _, found := d.GlobalGet("counter"); found {
		t.Fatal("expected no value before Set")
	}
	if err := d.GlobalSet("counter", 42.0); err != nil {
		t.Fatalf("GlobalSet: %v", err)
	}
	v, found := d.GlobalGet("counter")
	if !found || v != 42.0 {
		t.Errorf("GlobalGet = %v, %v; want 42.0, true", v, found)
	}
}
