package trycatch

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"

	_ "github.com/1uedev/DataPipe/engine/nodes/calculator"
)

// trycatchTestPanicker always panics — proves ARC-150 panic recovery
// reaches the catch port via engine/flow.ExecuteNode.
type trycatchTestPanicker struct{}

func (trycatchTestPanicker) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	panic("boom")
}

func newTrycatchTestPanicker(json.RawMessage) (any, error) { return trycatchTestPanicker{}, nil }

// trycatchTestFailer always returns an error.
type trycatchTestFailer struct{}

func (trycatchTestFailer) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	return nil, errors.New("deliberate failure")
}

func newTrycatchTestFailer(json.RawMessage) (any, error) { return trycatchTestFailer{}, nil }

// trycatchTestSource is a Source, used only to prove try-catch rejects
// wrapping a non-Processor type.
type trycatchTestSource struct{}

func (trycatchTestSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	return nil
}

func newTrycatchTestSource(json.RawMessage) (any, error) { return trycatchTestSource{}, nil }

func init() {
	flow.Register("trycatch-test-panicker", flow.NodeTypeInfo{Kind: flow.KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, newTrycatchTestPanicker)
	flow.Register("trycatch-test-failer", flow.NodeTypeInfo{Kind: flow.KindProcessor, Inputs: []string{"in"}, Outputs: []string{"out"}}, newTrycatchTestFailer)
	flow.Register("trycatch-test-source", flow.NodeTypeInfo{Kind: flow.KindSource, Outputs: []string{"out"}}, newTrycatchTestSource)
}

func newTestNode(t *testing.T, tryType string, tryConfig any) *node {
	t.Helper()
	cfgRaw, err := json.Marshal(tryConfig)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(Config{TryType: tryType, TryConfig: cfgRaw})
	if err != nil {
		t.Fatal(err)
	}
	instance, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return instance.(*node)
}

func TestPROC370_NewRequiresTryType(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected an error: tryType is required")
	}
}

func TestPROC370_NewRejectsUnknownType(t *testing.T) {
	raw, _ := json.Marshal(Config{TryType: "does-not-exist", TryConfig: json.RawMessage(`{}`)})
	if _, err := New(raw); err == nil {
		t.Fatal("expected an error for an unknown wrapped node type")
	}
}

func TestPROC370_NewRejectsSourceNodeType(t *testing.T) {
	raw, _ := json.Marshal(Config{TryType: "trycatch-test-source", TryConfig: json.RawMessage(`{}`)})
	if _, err := New(raw); err == nil {
		t.Fatal("expected an error: only Processor node types can be wrapped")
	}
}

func TestPROC370_SuccessRoutesToOut(t *testing.T) {
	n := newTestNode(t, "calculator", map[string]any{
		"fields": []map[string]any{{"path": "doubled", "expression": "payload.x * 2"}},
	})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"x": 5.0}})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 1 || results[0].Port != "out" {
		t.Fatalf("results = %+v", results)
	}
	m := results[0].Datagram.Payload.Value.(map[string]any)
	// goja exports whole-number results as int64 rather than float64.
	if m["doubled"] != int64(10) && m["doubled"] != float64(10) {
		t.Errorf("doubled = %v (%T)", m["doubled"], m["doubled"])
	}
}

func TestPROC370_ErrorRoutesToCatchWithERR100Shape(t *testing.T) {
	n := newTestNode(t, "trycatch-test-failer", map[string]any{})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process itself must not error: %v", err)
	}
	if len(results) != 1 || results[0].Port != "catch" {
		t.Fatalf("results = %+v, want exactly 1 catch-port result", results)
	}
	payload := results[0].Datagram.Payload.Value.(map[string]any)
	errInfo, ok := payload["error"].(map[string]any)
	if !ok {
		t.Fatalf("catch payload = %+v, want an \"error\" object (ERR-100 shape)", payload)
	}
	if errInfo["message"] != "deliberate failure" {
		t.Errorf("error message = %v", errInfo["message"])
	}
	if payload["original"] == nil {
		t.Error("expected the original datagram to be carried alongside the error (ERR-100)")
	}
}

func TestPROC370_PanicRoutesToCatch(t *testing.T) {
	n := newTestNode(t, "trycatch-test-panicker", map[string]any{})
	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})
	results, err := n.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process itself must not error even on a wrapped panic: %v", err)
	}
	if len(results) != 1 || results[0].Port != "catch" {
		t.Fatalf("results = %+v, want exactly 1 catch-port result", results)
	}
	errInfo := results[0].Datagram.Payload.Value.(map[string]any)["error"].(map[string]any)
	if errInfo["message"] != "boom" {
		t.Errorf("error message = %v, want the panic value", errInfo["message"])
	}
}
