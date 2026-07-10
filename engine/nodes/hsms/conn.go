package hsms

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// Mode selects which end of the TCP connection this side plays (SEMI E37
// §5.2): the active entity dials out and initiates Select; the passive
// entity listens and waits for the peer's Select.req.
type Mode int

const (
	ModeActive Mode = iota
	ModePassive
)

// Timers holds HSMS's five standard timeout parameters (SEMI E37 §8) plus
// the (not one of the five, but universally implemented) linktest
// interval. All are per-connection, not global.
type Timers struct {
	T3       time.Duration // reply timeout for a data message sent with the W-bit
	T5       time.Duration // connect separation timeout (reconnect backoff floor; enforced by the caller, not Conn itself)
	T6       time.Duration // control transaction timeout (Select/Deselect/Linktest reply)
	T7       time.Duration // not-selected timeout: how long a passive Conn waits for the peer's Select.req
	T8       time.Duration // network inter-character timeout; approximated here as one deadline for the whole message (see readLoop)
	Linktest time.Duration // linktest send interval once selected; <=0 disables the automatic linktest loop
}

// DefaultTimers returns the commonly used default values for Timers.
func DefaultTimers() Timers {
	return Timers{
		T3:       45 * time.Second,
		T5:       10 * time.Second,
		T6:       5 * time.Second,
		T7:       10 * time.Second,
		T8:       5 * time.Second,
		Linktest: 30 * time.Second,
	}
}

// State is a Conn's place in the HSMS connection state model (SEMI E37
// §5.4): NOT CONNECTED, CONNECTED/NOT SELECTED, CONNECTED/SELECTED.
type State int

const (
	StateNotConnected State = iota
	StateConnectedNotSelected
	StateConnectedSelected
)

// ErrNotSelected is returned by Send/SendAndWait when the session hasn't
// completed (or has lost) the Select procedure.
var ErrNotSelected = errors.New("hsms: session not selected")

// Conn is one HSMS session over one TCP connection. Create with Dial
// (active) or Listen (passive); both block until the Select procedure
// completes, so a returned *Conn is always already CONNECTED/SELECTED.
type Conn struct {
	conn      net.Conn
	sessionID uint16
	timers    Timers

	mu    sync.Mutex
	state State

	nextSystem  uint32
	pending     map[uint32]chan Message
	selectReqCh chan Message

	dataCh    chan Message
	errCh     chan error
	closeOnce sync.Once
	closed    chan struct{}
}

func newConn(nc net.Conn, sessionID uint16, timers Timers) *Conn {
	return &Conn{
		conn:        nc,
		sessionID:   sessionID,
		timers:      timers,
		state:       StateConnectedNotSelected,
		pending:     make(map[uint32]chan Message),
		selectReqCh: make(chan Message, 1),
		dataCh:      make(chan Message, 16),
		errCh:       make(chan error, 1),
		closed:      make(chan struct{}),
	}
}

// Dial opens an HSMS connection as the active entity (SEMI E37 §5.2): TCP
// dials addr, then performs the Select procedure. sessionID is the value
// proposed to the peer.
func Dial(ctx context.Context, addr string, sessionID uint16, timers Timers) (*Conn, error) {
	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("hsms: dial %s: %w", addr, err)
	}
	c := newConn(nc, sessionID, timers)
	go c.readLoop()
	if err := c.selectActive(ctx); err != nil {
		_ = c.Close()
		return nil, err
	}
	go c.linktestLoop()
	return c, nil
}

// Listen opens an HSMS connection as the passive entity (SEMI E37 §5.2):
// listens on addr, accepts exactly one connection, then waits (bounded by
// T7) for the peer's Select.req.
func Listen(ctx context.Context, addr string, sessionID uint16, timers Timers) (*Conn, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("hsms: listen %s: %w", addr, err)
	}
	defer func() { _ = ln.Close() }()

	type acceptResult struct {
		nc  net.Conn
		err error
	}
	acceptCh := make(chan acceptResult, 1)
	go func() {
		nc, err := ln.Accept()
		acceptCh <- acceptResult{nc, err}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-acceptCh:
		if r.err != nil {
			return nil, fmt.Errorf("hsms: accept: %w", r.err)
		}
		c := newConn(r.nc, sessionID, timers)
		go c.readLoop()
		if err := c.waitSelectPassive(ctx); err != nil {
			_ = c.Close()
			return nil, err
		}
		go c.linktestLoop()
		return c, nil
	}
}

func (c *Conn) nextSystemBytes() uint32 {
	return atomic.AddUint32(&c.nextSystem, 1)
}

func (c *Conn) setState(s State) {
	c.mu.Lock()
	c.state = s
	c.mu.Unlock()
}

// State reports the connection's current place in the HSMS state model.
func (c *Conn) State() State {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// selectActive sends Select.req and waits (bounded by T6) for Select.rsp.
func (c *Conn) selectActive(ctx context.Context) error {
	system := c.nextSystemBytes()
	replyCh := make(chan Message, 1)
	c.mu.Lock()
	c.pending[system] = replyCh
	c.mu.Unlock()

	req := Message{Header: Header{SessionID: c.sessionID, SType: STypeSelectReq, System: system}}
	if _, err := c.conn.Write(req.Encode()); err != nil {
		return fmt.Errorf("hsms: sending Select.req: %w", err)
	}

	select {
	case reply := <-replyCh:
		if reply.Header.SType != STypeSelectRsp {
			return fmt.Errorf("hsms: expected Select.rsp, got %s", reply.Header.SType)
		}
		if status := SelectStatus(reply.Header.Byte3); status != SelectOK {
			return fmt.Errorf("hsms: Select.req rejected, status %d", status)
		}
		c.setState(StateConnectedSelected)
		return nil
	case <-time.After(c.timers.T6):
		c.mu.Lock()
		delete(c.pending, system)
		c.mu.Unlock()
		return fmt.Errorf("hsms: T6 timeout waiting for Select.rsp")
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, system)
		c.mu.Unlock()
		return ctx.Err()
	case <-c.closed:
		return net.ErrClosed
	}
}

// waitSelectPassive waits (bounded by T7) for the peer's Select.req, then
// answers Select.rsp(OK).
func (c *Conn) waitSelectPassive(ctx context.Context) error {
	select {
	case msg := <-c.selectReqCh:
		c.sessionID = msg.Header.SessionID
		if err := c.sendControlReply(STypeSelectRsp, msg.Header.SessionID, byte(SelectOK), msg.Header.System); err != nil {
			return err
		}
		c.setState(StateConnectedSelected)
		return nil
	case <-time.After(c.timers.T7):
		return fmt.Errorf("hsms: T7 timeout waiting for Select.req")
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return net.ErrClosed
	}
}

func (c *Conn) sendControlReply(sType SType, sessionID uint16, byte3 byte, system uint32) error {
	msg := Message{Header: Header{SessionID: sessionID, Byte3: byte3, SType: sType, System: system}}
	_, err := c.conn.Write(msg.Encode())
	return err
}

// readLoop is the connection's single reader goroutine: decodes HSMS
// control messages itself (Linktest.req gets an immediate Linktest.rsp;
// Select.req past the initial handshake gets "already active"; Separate.req
// ends the session) and routes everything else either to a pending
// request's reply channel (matched by system bytes) or, for an unsolicited
// data message, to the Recv() channel.
func (c *Conn) readLoop() {
	for {
		// No read deadline while idle, waiting for the next message to
		// start — that's a normal gap between linktests/data, not a stall.
		_ = c.conn.SetReadDeadline(time.Time{})
		var lenBuf [4]byte
		if _, err := io.ReadFull(c.conn, lenBuf[:]); err != nil {
			c.fail(err)
			return
		}
		length := binary.BigEndian.Uint32(lenBuf[:])
		if length < 10 {
			c.fail(fmt.Errorf("hsms: message length %d shorter than the 10-byte header", length))
			return
		}
		if length > MaxLength {
			c.fail(fmt.Errorf("hsms: message length %d exceeds max %d", length, MaxLength))
			return
		}
		// T8 (network inter-character timeout) bounds only the read of a
		// message already announced by its length prefix — a peer that
		// stalls mid-transmission is what T8 exists to catch, not ordinary
		// idle time between messages.
		if c.timers.T8 > 0 {
			_ = c.conn.SetReadDeadline(time.Now().Add(c.timers.T8))
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(c.conn, buf); err != nil {
			c.fail(err)
			return
		}
		hdr, err := decodeHeader(buf[:10])
		if err != nil {
			c.fail(err)
			return
		}
		msg := Message{Header: hdr, Body: buf[10:]}

		switch msg.Header.SType {
		case STypeSeparateReq:
			c.fail(errSeparated)
			return
		case STypeLinktestReq:
			_ = c.sendControlReply(STypeLinktestRsp, msg.Header.SessionID, 0, msg.Header.System)
			continue
		case STypeSelectReq:
			if c.State() == StateConnectedSelected {
				_ = c.sendControlReply(STypeSelectRsp, msg.Header.SessionID, byte(SelectAlreadyActive), msg.Header.System)
			} else {
				select {
				case c.selectReqCh <- msg:
				case <-c.closed:
					return
				}
			}
			continue
		}

		c.mu.Lock()
		replyCh, ok := c.pending[msg.Header.System]
		if ok {
			delete(c.pending, msg.Header.System)
		}
		c.mu.Unlock()
		if ok {
			replyCh <- msg
			continue
		}

		if msg.Header.SType == STypeDataMessage {
			select {
			case c.dataCh <- msg:
			case <-c.closed:
				return
			}
		}
		// Any other unmatched control reply (e.g. a Linktest.rsp that arrived
		// after our own wait already timed out) is dropped.
	}
}

var errSeparated = errors.New("hsms: peer sent Separate.req")

func (c *Conn) fail(err error) {
	c.setState(StateNotConnected)
	select {
	case c.errCh <- err:
	default:
	}
	_ = c.Close()
}

// linktestLoop sends a Linktest.req every Timers.Linktest once selected, to
// detect a silently dead peer faster than TCP itself would. A no-op if
// Timers.Linktest <= 0.
func (c *Conn) linktestLoop() {
	if c.timers.Linktest <= 0 {
		return
	}
	ticker := time.NewTicker(c.timers.Linktest)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-ticker.C:
			if c.State() != StateConnectedSelected {
				continue
			}
			if err := c.linktest(); err != nil {
				c.fail(err)
				return
			}
		}
	}
}

func (c *Conn) linktest() error {
	system := c.nextSystemBytes()
	replyCh := make(chan Message, 1)
	c.mu.Lock()
	c.pending[system] = replyCh
	c.mu.Unlock()

	req := Message{Header: Header{SessionID: 0xFFFF, SType: STypeLinktestReq, System: system}}
	if _, err := c.conn.Write(req.Encode()); err != nil {
		return fmt.Errorf("hsms: sending Linktest.req: %w", err)
	}
	select {
	case <-replyCh:
		return nil
	case <-time.After(c.timers.T6):
		c.mu.Lock()
		delete(c.pending, system)
		c.mu.Unlock()
		return fmt.Errorf("hsms: T6 timeout waiting for Linktest.rsp")
	case <-c.closed:
		return net.ErrClosed
	}
}

// SendAndWait sends a primary data message with the W-bit set and blocks
// (bounded by T3) for its reply, matched by system bytes.
func (c *Conn) SendAndWait(ctx context.Context, stream, function byte, body []byte) (Message, error) {
	if c.State() != StateConnectedSelected {
		return Message{}, ErrNotSelected
	}
	system := c.nextSystemBytes()
	replyCh := make(chan Message, 1)
	c.mu.Lock()
	c.pending[system] = replyCh
	c.mu.Unlock()

	msg := Message{Header: DataHeader(c.sessionID, stream, function, true, system), Body: body}
	if _, err := c.conn.Write(msg.Encode()); err != nil {
		c.mu.Lock()
		delete(c.pending, system)
		c.mu.Unlock()
		return Message{}, err
	}

	select {
	case reply := <-replyCh:
		return reply, nil
	case <-time.After(c.timers.T3):
		c.mu.Lock()
		delete(c.pending, system)
		c.mu.Unlock()
		return Message{}, fmt.Errorf("hsms: T3 timeout waiting for S%dF%d reply", stream, function+1)
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, system)
		c.mu.Unlock()
		return Message{}, ctx.Err()
	case <-c.closed:
		return Message{}, net.ErrClosed
	}
}

// Send sends a data message with no reply expected (W-bit clear).
func (c *Conn) Send(stream, function byte, body []byte) error {
	if c.State() != StateConnectedSelected {
		return ErrNotSelected
	}
	msg := Message{Header: DataHeader(c.sessionID, stream, function, false, c.nextSystemBytes()), Body: body}
	_, err := c.conn.Write(msg.Encode())
	return err
}

// Reply sends a reply data message echoing `to`'s system bytes and session
// id, used to answer an unsolicited primary message received via Recv().
func (c *Conn) Reply(to Message, function byte, body []byte) error {
	msg := Message{Header: DataHeader(to.Header.SessionID, to.Header.Stream(), function, false, to.Header.System), Body: body}
	_, err := c.conn.Write(msg.Encode())
	return err
}

// Recv returns the channel of unsolicited primary data messages (ones the
// peer initiated, e.g. an equipment-originated event report) — replies to
// this side's own SendAndWait calls never appear here.
func (c *Conn) Recv() <-chan Message { return c.dataCh }

// Err returns the channel that receives the connection's terminal error
// (a protocol violation, a dropped TCP connection, or the peer's
// Separate.req) exactly once, for callers that want to detect and react to
// an unexpected session end.
func (c *Conn) Err() <-chan error { return c.errCh }

// Separate gracefully ends the session (SEMI E37 §5.5: Separate.req, no
// reply expected) and closes the underlying TCP connection.
func (c *Conn) Separate() error {
	if c.State() == StateConnectedSelected {
		msg := Message{Header: Header{SessionID: c.sessionID, SType: STypeSeparateReq, System: c.nextSystemBytes()}}
		_, _ = c.conn.Write(msg.Encode())
	}
	return c.Close()
}

// Close closes the underlying TCP connection without attempting Separate;
// idempotent.
func (c *Conn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.conn.Close()
	})
	return nil
}
