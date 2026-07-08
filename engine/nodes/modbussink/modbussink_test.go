package modbussink

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
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
		t.Error("expected error for missing slaveId/area")
	}
	raw, err := json.Marshal(Config{
		Config: modbusshared.Config{Mode: "tcp", TCP: modbusshared.TCPConfig{Host: "localhost", Port: 502}, SlaveID: 1},
		Area:   "bogus", Address: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(raw); err == nil {
		t.Error("expected error for unknown area")
	}
}

// startModbusTCPSlave is a minimal function-code-6-only slave, reused from
// the same wire-protocol-verification approach as modbussource's test.
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
			go func() {
				defer func() { _ = conn.Close() }()
				for {
					header := make([]byte, 7)
					if _, err := io.ReadFull(conn, header); err != nil {
						return
					}
					length := binary.BigEndian.Uint16(header[4:6])
					pdu := make([]byte, length-1)
					if _, err := io.ReadFull(conn, pdu); err != nil {
						return
					}
					var resp []byte
					switch pdu[0] {
					case 6: // WriteSingleRegister
						addr := binary.BigEndian.Uint16(pdu[1:3])
						val := binary.BigEndian.Uint16(pdu[3:5])
						registers[addr] = val
						resp = append([]byte{6}, pdu[1:5]...)
					case 16: // WriteMultipleRegisters
						addr := binary.BigEndian.Uint16(pdu[1:3])
						qty := binary.BigEndian.Uint16(pdu[3:5])
						data := pdu[6:]
						for i := 0; i < int(qty); i++ {
							registers[int(addr)+i] = binary.BigEndian.Uint16(data[i*2:])
						}
						resp = append([]byte{16}, pdu[1:5]...)
					case 5: // WriteSingleCoil
						resp = append([]byte{5}, pdu[1:5]...)
					default:
						resp = []byte{pdu[0] | 0x80, 1}
					}
					out := make([]byte, 7+len(resp))
					copy(out, header[:4])
					binary.BigEndian.PutUint16(out[4:6], uint16(1+len(resp)))
					out[6] = header[6]
					copy(out[7:], resp)
					if _, err := conn.Write(out); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().String(), func() { _ = ln.Close() }
}

func TestCON230_SinkWritesMultiRegisterFieldToRealModbusTCPSlave(t *testing.T) {
	registers := make([]uint16, 10)
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
		Config:  modbusshared.Config{Mode: "tcp", TCP: modbusshared.TCPConfig{Host: host, Port: port}, SlaveID: 1, TimeoutMs: 2000},
		Area:    "register",
		Address: 0,
		Field:   &modbusshared.Field{Name: "v", Register: 0, Type: "uint32"},
	})
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(raw)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	proc := n.(flow.Processor)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	d := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: float64(65536)})
	if _, err := proc.Process(ctx, d); err != nil {
		t.Fatalf("Process: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	if registers[0] != 1 || registers[1] != 0 {
		t.Errorf("registers[0:2] = %v, %v, want 1, 0 (65536 as a big-endian uint32)", registers[0], registers[1])
	}
}
