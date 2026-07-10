// Package hsms implements HSMS (SEMI E37, "High-Speed SECS Message
// Services") transport: the TCP/IP framing and session-management layer
// SECS-II (engine/nodes/secsii) and GEM (engine/nodes/gem) are built on top
// of. This is the Increment 11 "HSMS spike" that settled ADR-003: a native
// Go implementation of the header/framing/select-procedure was
// straightforward and fully testable loopback-to-loopback, so no .NET
// secs4net sidecar is needed (see docs/Architecture.md ADR-003).
package hsms

import (
	"encoding/binary"
	"fmt"
	"io"
)

// SType identifies an HSMS Session Type (SEMI E37 §7): 0 marks an ordinary
// SECS-II data message; the rest are HSMS control messages exchanged
// between the two ends of one TCP connection, never passed on to a node.
type SType byte

const (
	STypeDataMessage SType = 0
	STypeSelectReq   SType = 1
	STypeSelectRsp   SType = 2
	STypeDeselectReq SType = 3
	STypeDeselectRsp SType = 4
	STypeLinktestReq SType = 5
	STypeLinktestRsp SType = 6
	STypeRejectReq   SType = 7
	STypeSeparateReq SType = 9
)

func (t SType) String() string {
	switch t {
	case STypeDataMessage:
		return "DataMessage"
	case STypeSelectReq:
		return "Select.req"
	case STypeSelectRsp:
		return "Select.rsp"
	case STypeDeselectReq:
		return "Deselect.req"
	case STypeDeselectRsp:
		return "Deselect.rsp"
	case STypeLinktestReq:
		return "Linktest.req"
	case STypeLinktestRsp:
		return "Linktest.rsp"
	case STypeRejectReq:
		return "Reject.req"
	case STypeSeparateReq:
		return "Separate.req"
	default:
		return fmt.Sprintf("SType(%d)", byte(t))
	}
}

// SelectStatus is the status byte of a Select.rsp (SEMI E37 §7.3).
type SelectStatus byte

const (
	SelectOK            SelectStatus = 0
	SelectAlreadyActive SelectStatus = 1
	SelectNotReady      SelectStatus = 2
	SelectExhausted     SelectStatus = 3
)

// Header is the 10-byte HSMS message header (SEMI E37 §7): a session id,
// then two message-kind-dependent bytes (stream+W-bit / function for a data
// message; a control-message-specific status/reason byte otherwise), a
// PType byte (always 0 — SECS-II is the only presentation type DataPipe
// speaks), the SType, and 4 system bytes (a transaction id the reply
// echoes back, used to match requests to replies).
type Header struct {
	SessionID uint16
	Byte2     byte
	Byte3     byte
	PType     byte
	SType     SType
	System    uint32
}

// DataHeader builds the header for an ordinary SxFy data message.
func DataHeader(sessionID uint16, stream, function byte, wBit bool, system uint32) Header {
	b2 := stream & 0x7F
	if wBit {
		b2 |= 0x80
	}
	return Header{SessionID: sessionID, Byte2: b2, Byte3: function, SType: STypeDataMessage, System: system}
}

// Stream returns a data message's stream number (Byte2's low 7 bits).
func (h Header) Stream() byte { return h.Byte2 & 0x7F }

// WBit reports whether a reply is expected (Byte2's high bit) — data
// messages only.
func (h Header) WBit() bool { return h.Byte2&0x80 != 0 }

// Function returns a data message's function number (Byte3).
func (h Header) Function() byte { return h.Byte3 }

func (h Header) encode() [10]byte {
	var b [10]byte
	binary.BigEndian.PutUint16(b[0:2], h.SessionID)
	b[2] = h.Byte2
	b[3] = h.Byte3
	b[4] = h.PType
	b[5] = byte(h.SType)
	binary.BigEndian.PutUint32(b[6:10], h.System)
	return b
}

func decodeHeader(b []byte) (Header, error) {
	if len(b) != 10 {
		return Header{}, fmt.Errorf("hsms: header must be 10 bytes, got %d", len(b))
	}
	return Header{
		SessionID: binary.BigEndian.Uint16(b[0:2]),
		Byte2:     b[2],
		Byte3:     b[3],
		PType:     b[4],
		SType:     SType(b[5]),
		System:    binary.BigEndian.Uint32(b[6:10]),
	}, nil
}

// Message is one complete HSMS message: header + body (SECS-II-encoded
// item bytes for a data message; empty for every control message).
type Message struct {
	Header Header
	Body   []byte
}

// MaxLength bounds a single message's header+body (BUS-110: "nothing
// buffers unboundedly") — no legitimate SECS-II message approaches this;
// it exists only to reject a corrupt/hostile length header before
// allocating a buffer for it.
const MaxLength = 16 * 1024 * 1024

// Encode serializes m as an on-wire HSMS block: a 4-byte big-endian length
// (of header+body) followed by the 10-byte header and the body.
func (m Message) Encode() []byte {
	h := m.Header.encode()
	out := make([]byte, 4+10+len(m.Body))
	binary.BigEndian.PutUint32(out[0:4], uint32(10+len(m.Body)))
	copy(out[4:14], h[:])
	copy(out[14:], m.Body)
	return out
}

// ReadMessage reads one HSMS block from r.
func ReadMessage(r io.Reader) (Message, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		return Message{}, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])
	if length < 10 {
		return Message{}, fmt.Errorf("hsms: message length %d shorter than the 10-byte header", length)
	}
	if length > MaxLength {
		return Message{}, fmt.Errorf("hsms: message length %d exceeds max %d", length, MaxLength)
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Message{}, err
	}
	hdr, err := decodeHeader(buf[:10])
	if err != nil {
		return Message{}, err
	}
	return Message{Header: hdr, Body: buf[10:]}, nil
}
