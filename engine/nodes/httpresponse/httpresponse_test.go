package httpresponse

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/webhook"
)

func inWithCorrelation(correlationID string, value any) datagram.Datagram {
	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: value})
	d.Header.CorrelationID = correlationID
	return d
}

func TestSNK170_StringPayloadRepliesAsTextPlain(t *testing.T) {
	n, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	ch, cancel := webhook.Default.Await("corr-a")
	defer cancel()

	if _, err := proc.Process(context.Background(), inWithCorrelation("corr-a", "hello")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	resp := <-ch
	if resp.Status != 200 {
		t.Errorf("Status = %d, want 200 default", resp.Status)
	}
	if string(resp.Body) != "hello" {
		t.Errorf("Body = %q, want hello", resp.Body)
	}
	if resp.Headers["Content-Type"] != "text/plain; charset=utf-8" {
		t.Errorf("Content-Type = %q", resp.Headers["Content-Type"])
	}
}

func TestSNK170_ObjectPayloadRepliesAsJSON(t *testing.T) {
	n, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	ch, cancel := webhook.Default.Await("corr-b")
	defer cancel()

	if _, err := proc.Process(context.Background(), inWithCorrelation("corr-b", map[string]any{"ok": true})); err != nil {
		t.Fatalf("Process: %v", err)
	}
	resp := <-ch
	if resp.Headers["Content-Type"] != "application/json" {
		t.Errorf("Content-Type = %q", resp.Headers["Content-Type"])
	}
	var decoded map[string]any
	if err := json.Unmarshal(resp.Body, &decoded); err != nil {
		t.Fatalf("Body did not decode as JSON: %v (%s)", err, resp.Body)
	}
	if decoded["ok"] != true {
		t.Errorf("decoded = %+v", decoded)
	}
}

func TestSNK170_ConfiguredStatusAndHeadersOverrideDefaults(t *testing.T) {
	raw, err := json.Marshal(Config{Status: 404, Headers: map[string]string{"X-Custom": "v", "Content-Type": "text/csv"}})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	ch, cancel := webhook.Default.Await("corr-c")
	defer cancel()

	if _, err := proc.Process(context.Background(), inWithCorrelation("corr-c", "body")); err != nil {
		t.Fatalf("Process: %v", err)
	}
	resp := <-ch
	if resp.Status != 404 {
		t.Errorf("Status = %d, want 404", resp.Status)
	}
	if resp.Headers["X-Custom"] != "v" {
		t.Errorf("X-Custom = %q", resp.Headers["X-Custom"])
	}
	if resp.Headers["Content-Type"] != "text/csv" {
		t.Errorf("Content-Type = %q, want the configured override text/csv", resp.Headers["Content-Type"])
	}
}

func TestSNK170_NoAwaiterIsHarmless(t *testing.T) {
	n, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)
	results, err := proc.Process(context.Background(), inWithCorrelation("no-such-correlation", "x"))
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("http-response is a sink, got %d outputs", len(results))
	}
}
