package secsgemhost

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/gemsim"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

func TestCON220_NewValidatesConfig(t *testing.T) {
	cases := []string{
		`{"reports":[{"rptid":1}]}`,
		`{"events":[{"ceid":1}]}`,
		`{"traces":[{"trid":1,"periodSec":0,"svids":[1]}]}`,
	}
	for _, raw := range cases {
		if _, err := New(json.RawMessage(raw)); err == nil {
			t.Errorf("New(%s) should have failed validation", raw)
		}
	}
	if _, err := New(json.RawMessage(`{}`)); err != nil {
		t.Errorf("New({}) should be valid (no setup at all is allowed), got %v", err)
	}
}

func TestCON220_FormatDSPER(t *testing.T) {
	cases := []struct {
		in   float64
		want string
	}{
		{1, "0001.000"},
		{0.5, "0000.500"},
		{60.25, "0060.250"},
	}
	for _, c := range cases {
		if got := formatDSPER(c.in); got != c.want {
			t.Errorf("formatDSPER(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

// stubResolver implements flow.ConnectionResolver with a fixed,
// already-decrypted config — a test double standing in for the runtime's
// real credential-vault-backed resolver (mirrors httprequest_test.go's
// stubResolver).
type stubResolver struct{ config json.RawMessage }

func (s stubResolver) ResolveConnection(context.Context, string) (flow.ConnectionInfo, error) {
	return flow.ConnectionInfo{Config: s.config}, nil
}

// TestCON220_SourceEmitsEventsAndTracesFromSimulator runs the actual
// "secsgem-in" node (via flow.WithConnection, exactly how the runtime
// wires a real deployed node's connection) against gemsim.Simulator and
// proves it emits real datagrams on the events/traces ports.
func TestCON220_SourceEmitsEventsAndTracesFromSimulator(t *testing.T) {
	addr := freeAddr(t)
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Atoi: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	simCfg := gemsim.Config{
		MDLN: "SIM-1", SoftRev: "1.0",
		SVIDs: map[uint32]gemsim.SVID{
			2001: {Name: "Temperature", Units: "C", Value: secsii.F8v(72.5)},
		},
		EventInterval: 50 * time.Millisecond,
		TraceInterval: 50 * time.Millisecond,
	}
	simCh := make(chan *gemsim.Simulator, 1)
	go func() {
		sim, err := gemsim.Listen(ctx, addr, simCfg)
		if err == nil {
			simCh <- sim
		}
	}()
	time.Sleep(50 * time.Millisecond)

	rawCfg, err := json.Marshal(Config{
		Reports: []reportCfg{{RPTID: 1001, VIDs: []uint32{2001}}},
		Events:  []eventCfg{{CEID: 3001, RPTIDs: []uint32{1001}}},
		Traces:  []traceCfg{{TRID: 5, PeriodSec: 0.05, SVIDs: []uint32{2001}}},
	})
	if err != nil {
		t.Fatalf("marshal node config: %v", err)
	}
	n, err := New(rawCfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	connCfg, err := json.Marshal(map[string]any{"mode": "active", "host": host, "port": port})
	if err != nil {
		t.Fatalf("marshal connection config: %v", err)
	}
	runCtx := flow.WithConnection(ctx, stubResolver{config: connCfg}, "conn-1")

	events := make(chan datagram.Datagram, 4)
	traces := make(chan datagram.Datagram, 4)
	emit := func(port string, d datagram.Datagram) error {
		switch port {
		case "events":
			events <- d
		case "traces":
			traces <- d
		}
		return nil
	}

	// Start the node's Run loop first — it's the active side that dials
	// the simulator's passive Listen, so waiting on simCh before this would
	// deadlock (nothing would ever connect to accept).
	runDone := make(chan error, 1)
	go func() { runDone <- src.Run(runCtx, emit) }()

	sim := <-simCh
	defer func() { _ = sim.Close() }()

	select {
	case ev := <-events:
		m, ok := ev.Payload.Value.(map[string]any)
		if !ok || m["ceid"] != uint32(3001) {
			t.Errorf("event datagram = %+v, want ceid=3001", ev.Payload.Value)
		}
	case err := <-runDone:
		t.Fatalf("Run exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event datagram")
	}

	select {
	case td := <-traces:
		m, ok := td.Payload.Value.(map[string]any)
		if !ok || m["trid"] != uint32(5) {
			t.Errorf("trace datagram = %+v, want trid=5", td.Payload.Value)
		}
	case err := <-runDone:
		t.Fatalf("Run exited early: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a trace datagram")
	}
}
