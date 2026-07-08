//go:build itest

// CON-200/SNK-110 integration test against a real MQTT broker (Docker
// eclipse-mosquitto), per Development-Plan.md's "every connector has
// integration tests against containerized targets." Run via `make itest`
// (needs Docker with network access to pull eclipse-mosquitto:2).
package e2e

import (
	"context"
	"encoding/json"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
	"github.com/1uedev/DataPipe/engine/nodes/mqttin"
	"github.com/1uedev/DataPipe/engine/nodes/mqttout"
	"github.com/1uedev/DataPipe/engine/nodes/mqttshared"
)

// startMosquitto runs a disposable eclipse-mosquitto broker with anonymous
// access allowed (fine for a throwaway test container), returning its
// tcp:// URL and a cleanup func.
func startMosquitto(t *testing.T) (brokerURL string, cleanup func()) {
	t.Helper()
	const containerName = "datapipe-itest-mosquitto"
	_ = exec.Command("docker", "rm", "-f", containerName).Run() // clean up any stale container from a previous run

	cfg := `
listener 1883
allow_anonymous true
`
	run := exec.Command("docker", "run", "-d", "--rm", "--name", containerName,
		"-p", "18830:1883",
		"eclipse-mosquitto:2",
		"sh", "-c", "echo '"+cfg+"' > /mosquitto/config/mosquitto.conf && mosquitto -c /mosquitto/config/mosquitto.conf")
	out, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("starting mosquitto container: %v\n%s", err, out)
	}

	cleanup = func() { _ = exec.Command("docker", "rm", "-f", containerName).Run() }

	// Give the broker a moment to start listening.
	time.Sleep(2 * time.Second)
	return "tcp://localhost:18830", cleanup
}

// fakeResolver is the shared flow.ConnectionResolver stub for every itest
// in this package: each test resolves to a fixed connection regardless of
// the id asked for, since these tests only ever deploy one connection at a
// time.
type fakeResolver struct {
	connType   string
	config     json.RawMessage
	credential json.RawMessage
}

func (f fakeResolver) ResolveConnection(context.Context, string) (flow.ConnectionInfo, error) {
	return flow.ConnectionInfo{Type: f.connType, Config: f.config, CredentialJSON: f.credential}, nil
}

func TestCON200_SNK110_MQTTPublishSubscribeRoundTripAgainstRealBroker(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	brokerURL, cleanup := startMosquitto(t)
	defer cleanup()

	connCfg, err := json.Marshal(mqttshared.BrokerConfig{BrokerURL: brokerURL})
	if err != nil {
		t.Fatal(err)
	}
	resolver := fakeResolver{config: connCfg}

	inRaw, err := json.Marshal(mqttin.Config{Topic: "itest/+/temp"})
	if err != nil {
		t.Fatal(err)
	}
	inNodeAny, err := mqttin.New(inRaw)
	if err != nil {
		t.Fatalf("mqttin.New: %v", err)
	}
	inNode := inNodeAny.(flow.Source)

	outRaw, err := json.Marshal(mqttout.Config{Topic: "itest/room1/temp"})
	if err != nil {
		t.Fatal(err)
	}
	outNodeAny, err := mqttout.New(outRaw)
	if err != nil {
		t.Fatalf("mqttout.New: %v", err)
	}
	outNode := outNodeAny.(flow.Processor)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inCtx := flow.WithConnection(ctx, resolver, "conn-1")
	outCtx := flow.WithConnection(ctx, resolver, "conn-1")

	var mu sync.Mutex
	var received []any
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = inNode.Run(inCtx, func(_ string, d datagram.Datagram) error {
			mu.Lock()
			received = append(received, d.Payload.Value)
			mu.Unlock()
			return nil
		})
	}()
	defer func() { cancel(); <-done }()

	time.Sleep(1 * time.Second) // let the subscribe complete

	published := datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: map[string]any{"celsius": 21.5}})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := outNode.Process(outCtx, published); err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	waitDeadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(waitDeadline) {
		mu.Lock()
		n := len(received)
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) == 0 {
		t.Fatal("expected mqtt-in to receive the message mqtt-out published")
	}
	m, ok := received[0].(map[string]any)
	if !ok || m["celsius"] != 21.5 {
		t.Errorf("received = %+v", received[0])
	}
}
