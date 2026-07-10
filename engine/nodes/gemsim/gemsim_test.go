package gemsim

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/nodes/gem"
	"github.com/1uedev/DataPipe/engine/nodes/hsms"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

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

// TestCON220_EstablishCommEventReportTraceCollection is the Development
// Plan's Increment 11 "done when" line proven end to end against
// gemsim.Simulator: a real gem.Host dials in, establishes communications,
// performs the full report define/link/enable setup sequence, receives a
// spontaneously fired event report, establishes a trace, and receives
// spontaneous trace data — using nothing but this package's own simulator,
// never real fab equipment.
func TestCON220_EstablishCommEventReportTraceCollection(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cfg := Config{
		MDLN:    "SIM-1",
		SoftRev: "1.0",
		SVIDs: map[uint32]SVID{
			2001: {Name: "Temperature", Units: "C", Value: secsii.F8v(72.5)},
			2002: {Name: "Pressure", Units: "torr", Value: secsii.F8v(1.2)},
		},
		EventInterval: 50 * time.Millisecond,
		TraceInterval: 50 * time.Millisecond,
	}

	simCh := make(chan *Simulator, 1)
	simErrCh := make(chan error, 1)
	go func() {
		sim, err := Listen(ctx, addr, cfg)
		if err != nil {
			simErrCh <- err
			return
		}
		simCh <- sim
	}()
	time.Sleep(50 * time.Millisecond)

	conn, err := hsms.Dial(ctx, addr, 7, hsms.DefaultTimers())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	var sim *Simulator
	select {
	case sim = <-simCh:
		defer func() { _ = sim.Close() }()
	case err := <-simErrCh:
		t.Fatalf("Listen: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the simulator to accept")
	}

	host := gem.NewHost(conn, "DataPipeHost", "1.0")
	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go func() { _ = host.Run(runCtx) }()

	// 1. Establish communications.
	commAck, mdln, softrev, err := host.EstablishCommunications(ctx)
	if err != nil {
		t.Fatalf("EstablishCommunications: %v", err)
	}
	if !commAck.OK() {
		t.Fatalf("EstablishCommunications ack = %v, want OK", commAck)
	}
	if mdln != "SIM-1" || softrev != "1.0" {
		t.Errorf("got identity (%q, %q), want (SIM-1, 1.0)", mdln, softrev)
	}

	// 2. Event report setup: define -> link -> enable.
	drack, err := host.DefineReport(ctx, 1, []gem.ReportDef{{RPTID: 1001, VIDs: []uint32{2001, 2002}}})
	if err != nil || !drack.OK() {
		t.Fatalf("DefineReport: ack=%v err=%v", drack, err)
	}
	lrack, err := host.LinkEventReport(ctx, 2, []gem.EventLink{{CEID: 3001, RPTIDs: []uint32{1001}}})
	if err != nil || !lrack.OK() {
		t.Fatalf("LinkEventReport: ack=%v err=%v", lrack, err)
	}
	erack, err := host.EnableEvents(ctx, true, []uint32{3001})
	if err != nil || !erack.OK() {
		t.Fatalf("EnableEvents: ack=%v err=%v", erack, err)
	}

	// 3. The simulator should now spontaneously fire the linked event report.
	select {
	case ev := <-host.Events():
		if ev.CEID != 3001 || len(ev.Reports) != 1 || ev.Reports[0].RPTID != 1001 {
			t.Fatalf("got %+v, want CEID=3001 with one RPTID=1001 report", ev)
		}
		if len(ev.Reports[0].Values) != 2 {
			t.Fatalf("got %d values, want 2 (Temperature, Pressure)", len(ev.Reports[0].Values))
		}
		temp, ok := ev.Reports[0].Values[0].Value().(float64)
		if !ok || temp != 72.5 {
			t.Errorf("Temperature value = %v, want 72.5", ev.Reports[0].Values[0].Value())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for the spontaneous event report")
	}

	// 4. Trace collection: establish, then receive spontaneous trace data.
	tiaack, err := host.EstablishTrace(ctx, 5, "0001.000", 0, 1, []uint32{2001, 2002})
	if err != nil || !tiaack.OK() {
		t.Fatalf("EstablishTrace: ack=%v err=%v", tiaack, err)
	}
	select {
	case td := <-host.Traces():
		if td.TRID != 5 || len(td.Values) != 2 {
			t.Fatalf("got %+v, want TRID=5 with 2 values", td)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for spontaneous trace data")
	}
}

// TestCON220_SimulatorAnswersCapabilityBrowse proves the SVID namelist
// request (S1F11/F12) MAP-100's report builder relies on works against
// the simulator.
func TestCON220_SimulatorAnswersCapabilityBrowse(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := Config{
		MDLN: "SIM-1", SoftRev: "1.0",
		SVIDs: map[uint32]SVID{7001: {Name: "SpindleSpeed", Units: "rpm", Value: secsii.U4v(1500)}},
	}
	simCh := make(chan *Simulator, 1)
	go func() {
		sim, err := Listen(ctx, addr, cfg)
		if err == nil {
			simCh <- sim
		}
	}()
	time.Sleep(50 * time.Millisecond)

	conn, err := hsms.Dial(ctx, addr, 7, hsms.DefaultTimers())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sim := <-simCh
	defer func() { _ = conn.Close(); _ = sim.Close() }()

	host := gem.NewHost(conn, "H", "1.0")
	list, err := host.StatusVariableNamelist(ctx)
	if err != nil {
		t.Fatalf("StatusVariableNamelist: %v", err)
	}
	if len(list) != 1 || list[0].SVID != 7001 || list[0].Name != "SpindleSpeed" || list[0].Units != "rpm" {
		t.Errorf("got %+v, want [{7001 SpindleSpeed rpm}]", list)
	}
}
