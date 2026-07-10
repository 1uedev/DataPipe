package hsms

import (
	"bytes"
	"testing"
)

// TestCON220_MessageEncodeDecodeRoundTrip proves an HSMS block round-trips
// byte-for-byte through Encode/ReadMessage, and that Stream/Function/WBit
// are recovered exactly for a data message.
func TestCON220_MessageEncodeDecodeRoundTrip(t *testing.T) {
	msg := Message{
		Header: DataHeader(42, 6, 11, true, 0xdeadbeef),
		Body:   []byte{0x01, 0x02, 0x03},
	}
	encoded := msg.Encode()

	decoded, err := ReadMessage(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if decoded.Header.SessionID != 42 {
		t.Errorf("SessionID = %d, want 42", decoded.Header.SessionID)
	}
	if decoded.Header.Stream() != 6 {
		t.Errorf("Stream() = %d, want 6", decoded.Header.Stream())
	}
	if decoded.Header.Function() != 11 {
		t.Errorf("Function() = %d, want 11", decoded.Header.Function())
	}
	if !decoded.Header.WBit() {
		t.Error("WBit() = false, want true")
	}
	if decoded.Header.System != 0xdeadbeef {
		t.Errorf("System = %#x, want 0xdeadbeef", decoded.Header.System)
	}
	if !bytes.Equal(decoded.Body, msg.Body) {
		t.Errorf("Body = %v, want %v", decoded.Body, msg.Body)
	}
}

func TestCON220_MessageWBitClear(t *testing.T) {
	msg := Message{Header: DataHeader(1, 6, 12, false, 1)}
	encoded := msg.Encode()
	decoded, err := ReadMessage(bytes.NewReader(encoded))
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if decoded.Header.WBit() {
		t.Error("WBit() = true, want false")
	}
	if decoded.Header.Stream() != 6 || decoded.Header.Function() != 12 {
		t.Errorf("Stream/Function = %d/%d, want 6/12", decoded.Header.Stream(), decoded.Header.Function())
	}
}

func TestCON220_ReadMessageRejectsShortLength(t *testing.T) {
	var lenBuf [4]byte
	lenBuf[3] = 5 // length 5 < 10-byte header minimum
	if _, err := ReadMessage(bytes.NewReader(lenBuf[:])); err == nil {
		t.Fatal("expected an error for a length shorter than the header, got nil")
	}
}

func TestCON220_ReadMessageRejectsOversizedLength(t *testing.T) {
	var lenBuf [4]byte
	lenBuf[0] = 0xFF
	lenBuf[1] = 0xFF
	lenBuf[2] = 0xFF
	lenBuf[3] = 0xFF
	if _, err := ReadMessage(bytes.NewReader(lenBuf[:])); err == nil {
		t.Fatal("expected an error for a length exceeding MaxLength, got nil")
	}
}

func TestCON220_STypeString(t *testing.T) {
	cases := map[SType]string{
		STypeDataMessage: "DataMessage",
		STypeSelectReq:   "Select.req",
		STypeSelectRsp:   "Select.rsp",
		STypeSeparateReq: "Separate.req",
	}
	for st, want := range cases {
		if got := st.String(); got != want {
			t.Errorf("SType(%d).String() = %q, want %q", st, got, want)
		}
	}
}
