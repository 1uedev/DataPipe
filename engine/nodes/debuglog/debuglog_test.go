package debuglog

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestDEBUGLOG_LogsLabelAndPayloadWithoutError(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)

	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	raw, err := json.Marshal(Config{Label: "my-debug"})
	if err != nil {
		t.Fatalf("marshal config: %v", err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"temp": 42.5}})
	results, err := proc.Process(context.Background(), in)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("debug-log is a sink, got %d output datagrams, want 0", len(results))
	}

	out := buf.String()
	if !strings.Contains(out, "my-debug") {
		t.Errorf("log output missing label:\n%s", out)
	}
	if !strings.Contains(out, in.Header.CorrelationID) {
		t.Errorf("log output missing correlation id:\n%s", out)
	}
}

func TestDEBUGLOG_NoLabelStillLogs(t *testing.T) {
	prev := slog.Default()
	defer slog.SetDefault(prev)
	var buf bytes.Buffer
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))

	n, err := New(nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	in := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: 1})
	if _, err := proc.Process(context.Background(), in); err != nil {
		t.Fatalf("Process: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("expected some log output")
	}
}
