package modbussource

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/modbusshared"
)

func TestCON230_NewValidatesConfig(t *testing.T) {
	if _, err := New([]byte(`{"mode":"tcp"}`)); err == nil {
		t.Error("expected error for missing slaveId/pollingGroups")
	}
	raw, err := json.Marshal(Config{
		Config:        modbusshared.Config{Mode: "tcp", TCP: modbusshared.TCPConfig{Host: "localhost", Port: 502}, SlaveID: 1},
		PollingGroups: []PollingGroup{{Name: "g1", Area: "bogus", Address: 0, Quantity: 1, IntervalMs: 100}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(raw); err == nil {
		t.Error("expected error for unknown area")
	}
}

func TestCON230_DecodeBitsUnpacksLSBFirst(t *testing.T) {
	got := decodeBits([]byte{0b0000_0101}, 4)
	want := []bool{true, false, true, false}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("bit %d = %v, want %v", i, got[i], want[i])
		}
	}
}

// --- minimal Modbus TCP slave stub for a genuine wire-protocol round trip ---
// (function codes 3 ReadHoldingRegisters and 6 WriteSingleRegister only —
// enough to exercise modbus-source's polling loop and the real goburrow/
// modbus client's request/response encoding, not just the decode helpers.)

func startModbusTCPSlave(t *testing.T, registers []uint16) (addr string, cleanup func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveModbusConn(conn, registers)
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func serveModbusConn(conn net.Conn, registers []uint16) {
	defer func() { _ = conn.Close() }()
	for {
		header := make([]byte, 7)
		if _, err := readFull(conn, header); err != nil {
			return
		}
		length := binary.BigEndian.Uint16(header[4:6])
		pdu := make([]byte, length-1) // length includes the unit id byte already read
		if _, err := readFull(conn, pdu); err != nil {
			return
		}
		resp := handleModbusPDU(pdu, registers)

		out := make([]byte, 7+len(resp))
		copy(out, header[:4])
		binary.BigEndian.PutUint16(out[4:6], uint16(1+len(resp)))
		out[6] = header[6]
		copy(out[7:], resp)
		if _, err := conn.Write(out); err != nil {
			return
		}
	}
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

func handleModbusPDU(pdu []byte, registers []uint16) []byte {
	switch pdu[0] {
	case 3: // ReadHoldingRegisters
		start := binary.BigEndian.Uint16(pdu[1:3])
		qty := binary.BigEndian.Uint16(pdu[3:5])
		out := make([]byte, 2+int(qty)*2)
		out[0] = 3
		out[1] = byte(qty * 2)
		for i := 0; i < int(qty); i++ {
			binary.BigEndian.PutUint16(out[2+i*2:], registers[int(start)+i])
		}
		return out
	case 6: // WriteSingleRegister
		addr := binary.BigEndian.Uint16(pdu[1:3])
		val := binary.BigEndian.Uint16(pdu[3:5])
		registers[addr] = val
		return append([]byte{6}, pdu[1:5]...)
	default:
		return []byte{pdu[0] | 0x80, 1} // illegal function
	}
}

func TestCON230_SourcePollsRealModbusTCPSlaveAndDecodesFields(t *testing.T) {
	registers := make([]uint16, 10)
	registers[0] = 1 // high word of a uint32 = 0x00010000 = 65536
	registers[1] = 0
	addr, cleanup := startModbusTCPSlave(t, registers)
	defer cleanup()

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	raw, err := json.Marshal(Config{
		Config: modbusshared.Config{
			Mode: "tcp", TCP: modbusshared.TCPConfig{Host: host, Port: port}, SlaveID: 1, TimeoutMs: 2000,
		},
		PollingGroups: []PollingGroup{{
			Name: "g1", Area: "holdingRegisters", Address: 0, Quantity: 2, IntervalMs: 30,
			Fields: []modbusshared.Field{{Name: "counter", Register: 0, Type: "uint32"}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := n.(flow.Source)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	received := make(chan datagram.Datagram, 1)
	go func() {
		_ = src.Run(ctx, func(_ string, d datagram.Datagram) error {
			select {
			case received <- d:
			default:
			}
			return nil
		})
	}()

	select {
	case d := <-received:
		m, ok := d.Payload.Value.(map[string]any)
		if !ok || m["counter"] != float64(65536) {
			t.Errorf("payload = %+v", d.Payload.Value)
		}
		if d.Header.Tags["modbus.group"] != "g1" {
			t.Errorf("tags = %+v", d.Header.Tags)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for a polled datagram")
	}
}
