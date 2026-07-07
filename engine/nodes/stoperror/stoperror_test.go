package stoperror

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func testDgm(payload any) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: payload})
}

func TestERR140_ProcessAlwaysReturnsAStructuredNodeError(t *testing.T) {
	instance, err := New(json.RawMessage(`{"message":"deliberate stop","code":"MY_CODE"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := instance.(*node)

	results, err := n.Process(context.Background(), testDgm(map[string]any{}))
	if results != nil {
		t.Fatalf("results = %v, want nil", results)
	}
	var nodeErr *flow.NodeError
	if !errors.As(err, &nodeErr) {
		t.Fatalf("error type = %T, want *flow.NodeError", err)
	}
	if nodeErr.Message != "deliberate stop" || nodeErr.Code != "MY_CODE" {
		t.Fatalf("nodeErr = %+v, want message %q code %q", nodeErr, "deliberate stop", "MY_CODE")
	}
}

func TestERR140_MessageAndCodeSupportExpressionTemplates(t *testing.T) {
	instance, err := New(json.RawMessage(`{"message":"failed for order {{payload.orderId}}","code":"ORD_{{payload.orderId}}"}`))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	n := instance.(*node)

	_, err = n.Process(context.Background(), testDgm(map[string]any{"orderId": "42"}))
	var nodeErr *flow.NodeError
	if !errors.As(err, &nodeErr) {
		t.Fatalf("error type = %T, want *flow.NodeError", err)
	}
	if nodeErr.Message != "failed for order 42" {
		t.Fatalf("Message = %q, want %q", nodeErr.Message, "failed for order 42")
	}
	if nodeErr.Code != "ORD_42" {
		t.Fatalf("Code = %q, want %q", nodeErr.Code, "ORD_42")
	}
}

func TestERR140_MessageIsRequired(t *testing.T) {
	if _, err := New(json.RawMessage(`{}`)); err == nil {
		t.Fatal("expected New to reject a config with no message")
	}
}
