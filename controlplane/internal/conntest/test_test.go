package conntest

import (
	"context"
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/nodes/gemsim"
)

func TestCON140_UnknownConnectionTypeReportsNoLiveTestAvailable(t *testing.T) {
	result := Test(context.Background(), "http", json.RawMessage(`{}`), nil)
	if !result.OK {
		t.Fatalf("expected OK=true for a type with no live test, got %+v", result)
	}
}

func TestCON140_PostgresMissingHostFailsFast(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"database": "datapipe"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "postgres", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: host is required")
	}
	if !strings.Contains(result.Message, "host") {
		t.Errorf("message = %q, want it to mention the missing host", result.Message)
	}
}

func TestCON140_PostgresInvalidConfigJSONFailsFast(t *testing.T) {
	result := Test(context.Background(), "postgres", json.RawMessage(`not json`), nil)
	if result.OK {
		t.Fatal("expected failure: invalid config JSON")
	}
}

func TestCON140_PostgresUnreachableHostFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"host": "127.0.0.1", "port": 1, "database": "datapipe"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	result := Test(context.Background(), "postgres", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
	if elapsed := time.Since(start); elapsed > Timeout+2*time.Second {
		t.Errorf("Test took %v, expected to fail well within the %v timeout", elapsed, Timeout)
	}
}

func TestCON140_MQTTMissingBrokerURLFailsFast(t *testing.T) {
	result := Test(context.Background(), "mqtt", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: brokerUrl is required")
	}
	if !strings.Contains(result.Message, "brokerUrl") {
		t.Errorf("message = %q, want it to mention the missing brokerUrl", result.Message)
	}
}

func TestCON140_MQTTInvalidConfigJSONFailsFast(t *testing.T) {
	result := Test(context.Background(), "mqtt", json.RawMessage(`not json`), nil)
	if result.OK {
		t.Fatal("expected failure: invalid config JSON")
	}
}

func TestCON140_MQTTInvalidCredentialJSONFailsFast(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"brokerUrl": "tcp://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "mqtt", cfg, json.RawMessage(`not json`))
	if result.OK {
		t.Fatal("expected failure: invalid credential JSON")
	}
}

func TestCON140_MySQLMissingHostFailsFast(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"database": "datapipe"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "mysql", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: host is required")
	}
}

func TestCON140_SQLiteMissingFileFailsFast(t *testing.T) {
	result := Test(context.Background(), "sqlite", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: file is required")
	}
}

func TestCON140_MongoUnreachableHostFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"host": "127.0.0.1", "port": 1, "database": "datapipe"})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	result := Test(context.Background(), "mongodb", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
	if elapsed := time.Since(start); elapsed > Timeout+2*time.Second {
		t.Errorf("Test took %v, expected to fail well within the %v timeout", elapsed, Timeout)
	}
}

func TestCON140_MongoMissingHostFailsFast(t *testing.T) {
	result := Test(context.Background(), "mongodb", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: host and database are required")
	}
}

func TestCON140_RedisUnreachableHostFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"host": "127.0.0.1", "port": 1})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "redis", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
}

func TestCON140_RedisMissingHostFailsFast(t *testing.T) {
	result := Test(context.Background(), "redis", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: host is required")
	}
}

func TestCON140_KafkaMissingBrokersFailsFast(t *testing.T) {
	result := Test(context.Background(), "kafka", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: at least one broker is required")
	}
}

func TestCON140_KafkaUnreachableBrokerFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"brokers": []string{"127.0.0.1:1"}})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "kafka", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
}

func TestCON140_S3MissingBucketFailsFast(t *testing.T) {
	result := Test(context.Background(), "s3", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: bucket is required")
	}
}

func TestCON140_ModbusMissingHostFailsFast(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"mode": "tcp"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "modbus", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: tcp.host and tcp.port are required")
	}
}

func TestCON140_ModbusRTUReportsNoLiveTest(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"mode": "rtu", "rtu": map[string]any{"port": "/dev/ttyUSB0"}})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "modbus", cfg, nil)
	if !result.OK {
		t.Fatalf("expected OK=true (no live test for serial), got %+v", result)
	}
}

func TestCON140_OPCUAMissingEndpointFailsFast(t *testing.T) {
	result := Test(context.Background(), "opcua", json.RawMessage(`{}`), nil)
	if result.OK {
		t.Fatal("expected failure: endpoint is required")
	}
}

func TestCON140_OPCUAUnreachableEndpointFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"endpoint": "opc.tcp://127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "opcua", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
}

func TestCON140_SECSGEMMissingPortFailsFast(t *testing.T) {
	result := Test(context.Background(), "secsgem", json.RawMessage(`{"mode":"active","host":"127.0.0.1"}`), nil)
	if result.OK {
		t.Fatal("expected failure: port is required")
	}
}

func TestCON140_SECSGEMPassiveReportsNoLiveTest(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"mode": "passive", "port": 5000})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(context.Background(), "secsgem", cfg, nil)
	if !result.OK {
		t.Fatalf("expected OK=true (no live test for passive/equipment-dials-in connections), got %+v", result)
	}
}

func TestCON140_SECSGEMUnreachableHostFailsWithinTimeout(t *testing.T) {
	cfg, err := json.Marshal(map[string]any{"mode": "active", "host": "127.0.0.1", "port": 1})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	result := Test(context.Background(), "secsgem", cfg, nil)
	if result.OK {
		t.Fatal("expected failure: nothing listens on that port")
	}
	if elapsed := time.Since(start); elapsed > Timeout+2*time.Second {
		t.Errorf("Test took %v, want within Timeout (%v) plus slack", elapsed, Timeout)
	}
}

// TestCON140_SECSGEMConnectsToSimulator proves the real HSMS
// Select + Are-You-There round trip against gemsim.Simulator — a stronger
// check than most connector types get here, since a faithful equipment
// stand-in is available in-process.
func TestCON140_SECSGEMConnectsToSimulator(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("finding a free port: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("Atoi: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	simDone := make(chan struct{})
	go func() {
		defer close(simDone)
		sim, err := gemsim.Listen(ctx, addr, gemsim.Config{MDLN: "SIM-1", SoftRev: "2.0"})
		if err != nil {
			return
		}
		<-ctx.Done()
		_ = sim.Close()
	}()
	time.Sleep(50 * time.Millisecond) // let the simulator bind before dialing

	cfg, err := json.Marshal(map[string]any{"mode": "active", "host": "127.0.0.1", "port": port})
	if err != nil {
		t.Fatal(err)
	}
	result := Test(ctx, "secsgem", cfg, nil)
	cancel()
	<-simDone
	if !result.OK {
		t.Fatalf("expected success against the simulator, got %+v", result)
	}
	if !strings.Contains(result.Message, "SIM-1") {
		t.Errorf("message = %q, want it to mention the equipment's model (SIM-1)", result.Message)
	}
}
