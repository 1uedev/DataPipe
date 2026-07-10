package secsgemaction

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
)

func TestSNK130_NewValidatesConfig(t *testing.T) {
	cases := []string{
		`{"action":"remoteCommand"}`,
		`{"action":"newEquipmentConstants","equipmentConstants":{"notanumber":1.0}}`,
		`{"action":"bogus"}`,
	}
	for _, raw := range cases {
		if _, err := New(json.RawMessage(raw)); err == nil {
			t.Errorf("New(%s) should have failed validation", raw)
		}
	}
	valid := []string{
		`{"action":"remoteCommand","rcmd":"START"}`,
		`{"action":"newEquipmentConstants","equipmentConstants":{"500":1.5}}`,
		`{"action":"raw","stream":2,"function":41}`,
	}
	for _, raw := range valid {
		if _, err := New(json.RawMessage(raw)); err != nil {
			t.Errorf("New(%s) should be valid, got %v", raw, err)
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

type stubResolver struct{ config json.RawMessage }

func (s stubResolver) ResolveConnection(context.Context, string) (flow.ConnectionInfo, error) {
	return flow.ConnectionInfo{Config: s.config}, nil
}

func testDatagram() datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"recipe": "R1"}})
}

func TestSNK130_RemoteCommand(t *testing.T) {
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

	simCh := make(chan *gemsim.Simulator, 1)
	go func() {
		sim, err := gemsim.Listen(ctx, addr, gemsim.Config{MDLN: "SIM", SoftRev: "1.0"})
		if err == nil {
			simCh <- sim
		}
	}()

	rawCfg, err := json.Marshal(Config{Action: "remoteCommand", RCMD: "START", ParamsFromPayload: true})
	if err != nil {
		t.Fatalf("marshal node config: %v", err)
	}
	n, err := New(rawCfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	connCfg, err := json.Marshal(map[string]any{"mode": "active", "host": host, "port": port})
	if err != nil {
		t.Fatalf("marshal connection config: %v", err)
	}
	runCtx := flow.WithConnection(ctx, stubResolver{config: connCfg}, "conn-1")

	results := make(chan []flow.PortDatagram, 1)
	errs := make(chan error, 1)
	go func() {
		out, err := proc.Process(runCtx, testDatagram())
		if err != nil {
			errs <- err
			return
		}
		results <- out
	}()

	sim := <-simCh
	defer func() { _ = sim.Close() }()

	select {
	case out := <-results:
		if len(out) != 1 || out[0].Port != "out" {
			t.Fatalf("got %+v, want one PortDatagram on \"out\"", out)
		}
		if ack := out[0].Datagram.Header.Tags["secsgem.hcack"]; ack != "0" {
			t.Errorf("secsgem.hcack tag = %q, want \"0\"", ack)
		}
	case err := <-errs:
		t.Fatalf("Process: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Process to complete")
	}
}

func TestSNK130_Raw(t *testing.T) {
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

	simCh := make(chan *gemsim.Simulator, 1)
	go func() {
		sim, err := gemsim.Listen(ctx, addr, gemsim.Config{MDLN: "SIM", SoftRev: "1.0"})
		if err == nil {
			simCh <- sim
		}
	}()

	rawCfg, err := json.Marshal(Config{Action: "raw", Stream: 1, Function: 1, WBit: true})
	if err != nil {
		t.Fatalf("marshal node config: %v", err)
	}
	n, err := New(rawCfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	connCfg, err := json.Marshal(map[string]any{"mode": "active", "host": host, "port": port})
	if err != nil {
		t.Fatalf("marshal connection config: %v", err)
	}
	runCtx := flow.WithConnection(ctx, stubResolver{config: connCfg}, "conn-1")

	results := make(chan []flow.PortDatagram, 1)
	errs := make(chan error, 1)
	go func() {
		out, err := proc.Process(runCtx, testDatagram())
		if err != nil {
			errs <- err
			return
		}
		results <- out
	}()

	sim := <-simCh
	defer func() { _ = sim.Close() }()

	select {
	case out := <-results:
		vals, ok := out[0].Datagram.Payload.Value.([]any)
		if !ok || len(vals) != 2 {
			t.Fatalf("S1F2 reply value = %#v, want a 2-element list (MDLN, SoftRev)", out[0].Datagram.Payload.Value)
		}
		if vals[0] != "SIM" || vals[1] != "1.0" {
			t.Errorf("got %v, want [SIM 1.0]", vals)
		}
	case err := <-errs:
		t.Fatalf("Process: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Process to complete")
	}
}
