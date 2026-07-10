package hsms

import (
	"context"
	"net"
	"testing"
	"time"
)

func fastTimers() Timers {
	return Timers{T3: 2 * time.Second, T5: time.Second, T6: time.Second, T7: 2 * time.Second, T8: 5 * time.Second, Linktest: 0}
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

// TestCON220_ActivePassiveSelectHandshake is the HSMS spike's core proof:
// a passive (equipment-side) listener and an active (host-side) dialer
// reach CONNECTED/SELECTED against each other over a real loopback TCP
// connection, with no external library — settling ADR-003 in favor of a
// native Go implementation.
func TestCON220_ActivePassiveSelectHandshake(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	passiveCh := make(chan *Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := Listen(ctx, addr, 0, fastTimers())
		if err != nil {
			errCh <- err
			return
		}
		passiveCh <- c
	}()
	time.Sleep(50 * time.Millisecond) // let the listener bind before dialing

	active, err := Dial(ctx, addr, 7, fastTimers())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = active.Close() }()

	select {
	case passive := <-passiveCh:
		defer func() { _ = passive.Close() }()
		if passive.State() != StateConnectedSelected {
			t.Errorf("passive.State() = %v, want StateConnectedSelected", passive.State())
		}
	case err := <-errCh:
		t.Fatalf("Listen: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for passive side to complete Select")
	}

	if active.State() != StateConnectedSelected {
		t.Errorf("active.State() = %v, want StateConnectedSelected", active.State())
	}
}

// TestCON220_DataMessageRequestReply proves a primary data message sent
// with SendAndWait (the shape a GEM "Are You There" S1F1 host request
// takes) is delivered to the peer's Recv() channel and its Reply() answer
// is matched back to the original caller by system bytes.
func TestCON220_DataMessageRequestReply(t *testing.T) {
	active, passive := pairedConns(t)
	defer func() { _ = active.Close(); _ = passive.Close() }()

	replyDone := make(chan struct{})
	go func() {
		defer close(replyDone)
		msg := <-passive.Recv()
		if msg.Header.Stream() != 1 || msg.Header.Function() != 1 {
			t.Errorf("received S%dF%d, want S1F1", msg.Header.Stream(), msg.Header.Function())
		}
		if err := passive.Reply(msg, 2, []byte{0xAA}); err != nil {
			t.Errorf("Reply: %v", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	reply, err := active.SendAndWait(ctx, 1, 1, nil)
	if err != nil {
		t.Fatalf("SendAndWait: %v", err)
	}
	if reply.Header.Function() != 2 {
		t.Errorf("reply function = %d, want 2 (S1F2)", reply.Header.Function())
	}
	if len(reply.Body) != 1 || reply.Body[0] != 0xAA {
		t.Errorf("reply body = %v, want [0xAA]", reply.Body)
	}
	<-replyDone
}

// TestCON220_UnsolicitedDataMessage proves an equipment-initiated message
// with no matching pending request (the shape a GEM S6F11 event report
// takes) lands on Recv() rather than being dropped or blocking the reader.
func TestCON220_UnsolicitedDataMessage(t *testing.T) {
	active, passive := pairedConns(t)
	defer func() { _ = active.Close(); _ = passive.Close() }()

	if err := passive.Send(6, 11, []byte{0x01}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case msg := <-active.Recv():
		if msg.Header.Stream() != 6 || msg.Header.Function() != 11 {
			t.Errorf("received S%dF%d, want S6F11", msg.Header.Stream(), msg.Header.Function())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the unsolicited message")
	}
}

// TestCON220_Separate proves a graceful Separate() on one side is observed
// as a terminal error on the peer's Err() channel, without either side
// hanging.
func TestCON220_Separate(t *testing.T) {
	active, passive := pairedConns(t)
	defer func() { _ = active.Close() }()

	if err := active.Separate(); err != nil {
		t.Fatalf("Separate: %v", err)
	}
	select {
	case <-passive.Err():
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for passive side to observe the Separate")
	}
}

// TestCON220_Linktest proves the automatic linktest loop round-trips
// without producing a spurious connection error over several intervals.
func TestCON220_Linktest(t *testing.T) {
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	timers := fastTimers()
	timers.Linktest = 50 * time.Millisecond

	passiveCh := make(chan *Conn, 1)
	go func() {
		c, err := Listen(ctx, addr, 0, timers)
		if err == nil {
			passiveCh <- c
		}
	}()
	time.Sleep(50 * time.Millisecond)

	active, err := Dial(ctx, addr, 7, timers)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	passive := <-passiveCh
	defer func() { _ = active.Close(); _ = passive.Close() }()

	time.Sleep(220 * time.Millisecond) // several linktest intervals

	select {
	case err := <-active.Err():
		t.Fatalf("unexpected active error during linktest: %v", err)
	case err := <-passive.Err():
		t.Fatalf("unexpected passive error during linktest: %v", err)
	default:
	}
	if active.State() != StateConnectedSelected || passive.State() != StateConnectedSelected {
		t.Error("connection dropped out of Selected state during linktest exchanges")
	}
}

// pairedConns dials a fresh active/passive pair over loopback TCP for
// tests that only care about post-Select behavior.
func pairedConns(t *testing.T) (active, passive *Conn) {
	t.Helper()
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	passiveCh := make(chan *Conn, 1)
	errCh := make(chan error, 1)
	go func() {
		c, err := Listen(ctx, addr, 0, fastTimers())
		if err != nil {
			errCh <- err
			return
		}
		passiveCh <- c
	}()
	time.Sleep(50 * time.Millisecond)

	active, err := Dial(ctx, addr, 7, fastTimers())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	select {
	case passive = <-passiveCh:
		return active, passive
	case err := <-errCh:
		t.Fatalf("Listen: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for passive side")
	}
	return nil, nil
}

func TestCON220_SendBeforeSelectRejected(t *testing.T) {
	addr := freeAddr(t)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer func() { _ = ln.Close() }()

	nc, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = nc.Close() }()

	c := newConn(nc, 1, fastTimers())
	if err := c.Send(1, 1, nil); err != ErrNotSelected {
		t.Errorf("Send before select = %v, want ErrNotSelected", err)
	}
}
