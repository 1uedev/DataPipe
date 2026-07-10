package gem

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/nodes/hsms"
	"github.com/1uedev/DataPipe/engine/nodes/secsii"
)

func fastTimers() hsms.Timers {
	return hsms.Timers{T3: 2 * time.Second, T5: time.Second, T6: time.Second, T7: 2 * time.Second, T8: 5 * time.Second, Linktest: 0}
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

// pairedHost dials a Host (active) against a raw equipment-side *hsms.Conn
// (passive) the test scripts directly — a hand-rolled stand-in for real
// equipment, distinct from the fuller engine/nodes/gemsim simulator used
// by the node-level integration test.
func pairedHost(t *testing.T) (host *Host, equip *hsms.Conn) {
	t.Helper()
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	equipCh := make(chan *hsms.Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := hsms.Listen(ctx, addr, 0, fastTimers())
		if err != nil {
			errCh <- err
			return
		}
		equipCh <- c
	}()
	time.Sleep(50 * time.Millisecond)

	activeConn, err := hsms.Dial(ctx, addr, 7, fastTimers())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case equip = <-equipCh:
	case err := <-errCh:
		t.Fatalf("Listen: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for equipment side")
	}
	return NewHost(activeConn, "DataPipeHost", "1.0"), equip
}

// equipReply answers the next primary message received on equip with the
// given function/body, once. Runs on a background goroutine in every
// caller, so it must never call the *testing.T fatal family — non-fatal
// Errorf only.
func equipReply(t *testing.T, equip *hsms.Conn, function byte, body secsii.Item) {
	t.Helper()
	select {
	case msg := <-equip.Recv():
		if err := equip.Reply(msg, function, secsii.Encode(body)); err != nil {
			t.Errorf("equip.Reply: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("timed out waiting for the host's request")
	}
}

func TestCON220_HostAreYouThere(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	done := make(chan struct{})
	go func() {
		defer close(done)
		equipReply(t, equip, 2, secsii.L(secsii.A("EQUIP-1"), secsii.A("2.3")))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	mdln, softrev, err := host.AreYouThere(ctx)
	if err != nil {
		t.Fatalf("AreYouThere: %v", err)
	}
	if mdln != "EQUIP-1" || softrev != "2.3" {
		t.Errorf("got (%q, %q), want (EQUIP-1, 2.3)", mdln, softrev)
	}
	<-done
}

func TestCON220_HostEstablishCommunications(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	go equipReply(t, equip, 14, secsii.L(secsii.U1v(0), secsii.L(secsii.A("EQUIP-1"), secsii.A("2.3"))))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ack, mdln, softrev, err := host.EstablishCommunications(ctx)
	if err != nil {
		t.Fatalf("EstablishCommunications: %v", err)
	}
	if !ack.OK() {
		t.Errorf("ack = %v, want OK", ack)
	}
	if mdln != "EQUIP-1" || softrev != "2.3" {
		t.Errorf("got (%q, %q), want (EQUIP-1, 2.3)", mdln, softrev)
	}
}

func TestCON220_HostStatusVariableNamelist(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	go equipReply(t, equip, 12, secsii.L(
		secsii.L(secsii.U4v(1001), secsii.A("Temperature"), secsii.A("C")),
		secsii.L(secsii.U4v(1002), secsii.A("Pressure"), secsii.A("torr")),
	))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	list, err := host.StatusVariableNamelist(ctx)
	if err != nil {
		t.Fatalf("StatusVariableNamelist: %v", err)
	}
	if len(list) != 2 || list[0].SVID != 1001 || list[0].Name != "Temperature" || list[0].Units != "C" {
		t.Errorf("got %+v, want [{1001 Temperature C} {1002 Pressure torr}]", list)
	}
}

// TestCON220_HostDefineLinkEnable proves the full "event report setup"
// sequence (S2F33 Define Report -> S2F35 Link Event Report -> S2F37 Enable
// Event Report), the Development-Plan's "event report setup" done-when
// criterion.
func TestCON220_HostDefineLinkEnable(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go equipReply(t, equip, 34, secsii.L(secsii.U1v(0)))
	drack, err := host.DefineReport(ctx, 1, []ReportDef{{RPTID: 1001, VIDs: []uint32{2001, 2002}}})
	if err != nil || !drack.OK() {
		t.Fatalf("DefineReport: ack=%v err=%v", drack, err)
	}

	go equipReply(t, equip, 36, secsii.L(secsii.U1v(0)))
	lrack, err := host.LinkEventReport(ctx, 2, []EventLink{{CEID: 3001, RPTIDs: []uint32{1001}}})
	if err != nil || !lrack.OK() {
		t.Fatalf("LinkEventReport: ack=%v err=%v", lrack, err)
	}

	go equipReply(t, equip, 38, secsii.L(secsii.U1v(0)))
	erack, err := host.EnableEvents(ctx, true, []uint32{3001})
	if err != nil || !erack.OK() {
		t.Fatalf("EnableEvents: ack=%v err=%v", erack, err)
	}
}

// TestCON220_HostReceiveEventReport proves an unsolicited S6F11 the
// equipment sends after report setup is auto-acknowledged (S6F12) and
// delivered on Events() by Run.
func TestCON220_HostReceiveEventReport(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = host.Run(runCtx) }()

	body := secsii.L(
		secsii.U4v(9), secsii.U4v(3001),
		secsii.L(secsii.L(secsii.U4v(1001), secsii.L(secsii.F8v(72.5), secsii.F8v(1.2)))),
	)
	if err := equip.Send(6, 11, secsii.Encode(body)); err != nil {
		t.Fatalf("equip.Send: %v", err)
	}

	select {
	case ev := <-host.Events():
		if ev.CEID != 3001 || ev.DataID != 9 || len(ev.Reports) != 1 || ev.Reports[0].RPTID != 1001 {
			t.Errorf("got %+v, want CEID=3001 DataID=9 Reports=[{1001 ...}]", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the event report")
	}

	// The equipment's S6F11 was sent with the W-bit clear above (Send, not
	// SendAndWait-shaped), so there is no ack to observe here; a W-bit-set
	// variant is exercised by TestCON220_HostReceiveTraceData below.
}

// TestCON220_HostReceiveTraceData proves the "trace collection" done-when
// criterion: S2F23 Establish Trace succeeds, and subsequent S6F1 trace data
// (sent with the W-bit set, as real equipment does to ensure delivery) is
// auto-acknowledged (S6F2) and delivered on Traces().
func TestCON220_HostReceiveTraceData(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go equipReply(t, equip, 24, secsii.L(secsii.U1v(0)))
	ack, err := host.EstablishTrace(ctx, 5, "0001.000", 0, 1, []uint32{1001, 1002})
	if err != nil || !ack.OK() {
		t.Fatalf("EstablishTrace: ack=%v err=%v", ack, err)
	}

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = host.Run(runCtx) }()

	acked := make(chan struct{})
	go func() {
		defer close(acked)
		msg, ok := <-equip.Recv()
		if !ok {
			return
		}
		_ = msg // the trace data was sent as a primary with W-bit; equip doesn't need the ack content, just that Run answered it
	}()

	body := secsii.L(secsii.U4v(5), secsii.L(secsii.F8v(21.0), secsii.F8v(1013.0)))
	reply, err := equip.SendAndWait(ctx, 6, 1, secsii.Encode(body))
	if err != nil {
		t.Fatalf("equip.SendAndWait: %v", err)
	}
	if reply.Header.Function() != 2 {
		t.Errorf("ack function = %d, want 2 (S6F2)", reply.Header.Function())
	}

	select {
	case td := <-host.Traces():
		if td.TRID != 5 || len(td.Values) != 2 {
			t.Errorf("got %+v, want TRID=5 with 2 values", td)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for trace data")
	}
}

func TestCON220_HostReceiveAlarm(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	runCtx, runCancel := context.WithCancel(context.Background())
	defer runCancel()
	go func() { _ = host.Run(runCtx) }()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	body := secsii.L(secsii.U1v(0x81), secsii.U4v(500), secsii.A("Door open"))
	reply, err := equip.SendAndWait(ctx, 5, 1, secsii.Encode(body))
	if err != nil {
		t.Fatalf("equip.SendAndWait: %v", err)
	}
	if reply.Header.Function() != 2 {
		t.Errorf("ack function = %d, want 2 (S5F2)", reply.Header.Function())
	}

	select {
	case al := <-host.Alarms():
		if !al.Set || al.Code != 1 || al.ALID != 500 || al.Text != "Door open" {
			t.Errorf("got %+v, want Set=true Code=1 ALID=500 Text=\"Door open\"", al)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the alarm")
	}
}

func TestCON220_HostSendRemoteCommand(t *testing.T) {
	host, equip := pairedHost(t)
	defer func() { _ = host.conn.Close(); _ = equip.Close() }()

	go equipReply(t, equip, 42, secsii.L(secsii.U1v(0), secsii.L()))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ack, err := host.SendRemoteCommand(ctx, "START", map[string]string{"RECIPE": "R1"})
	if err != nil || !ack.OK() {
		t.Fatalf("SendRemoteCommand: ack=%v err=%v", ack, err)
	}
}
