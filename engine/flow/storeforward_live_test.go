package flow

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// --- test-only source: emits whatever string arrives on a package-level,
// test-controlled channel — a stand-in for a streaming connector (e.g. an
// MQTT-in) driven programmatically, without EDGE-130's own tracking
// semantics (Trigger: false, unlike execTestTrigger). ---

var (
	sfTestSourceMu    sync.Mutex
	sfTestSourceChans = map[string]chan string{}
)

func sfTestSourceChannel(key string) chan string {
	sfTestSourceMu.Lock()
	defer sfTestSourceMu.Unlock()
	if ch, ok := sfTestSourceChans[key]; ok {
		return ch
	}
	ch := make(chan string, 10)
	sfTestSourceChans[key] = ch
	return ch
}

type sfTestSource struct{ ch chan string }

func (n *sfTestSource) Run(ctx context.Context, emit func(port string, d datagram.Datagram) error) error {
	for {
		select {
		case v := <-n.ch:
			d := datagram.New(datagram.Source{NodeID: "sf-source"}, datagram.Payload{Value: v})
			if err := emit("out", d); err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func newSFTestSource(raw json.RawMessage) (any, error) {
	var cfg struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	return &sfTestSource{ch: sfTestSourceChannel(cfg.Key)}, nil
}

func init() {
	Register("sf-test-source", NodeTypeInfo{Kind: KindSource, Outputs: []string{"out"}}, newSFTestSource)
}

// --- test-only sink: a stand-in for a node writing to a remote
// destination (e.g. mqtt-out/sql-sink) that can be down for a while —
// "reachable" toggles per key, shared package-level state so the test can
// simulate the destination coming back without touching the node itself.
// Delivered values land on a package-level channel for the test to observe. ---

var (
	sfTestSinkMu        sync.Mutex
	sfTestSinkReachable = map[string]*atomic.Bool{}
	sfTestSinkDelivered = map[string]chan string{}
)

func sfTestSinkState(key string) (*atomic.Bool, chan string) {
	sfTestSinkMu.Lock()
	defer sfTestSinkMu.Unlock()
	reachable, ok := sfTestSinkReachable[key]
	if !ok {
		reachable = &atomic.Bool{}
		reachable.Store(true)
		sfTestSinkReachable[key] = reachable
	}
	delivered, ok := sfTestSinkDelivered[key]
	if !ok {
		delivered = make(chan string, 10)
		sfTestSinkDelivered[key] = delivered
	}
	return reachable, delivered
}

func sfTestSetReachable(key string, reachable bool) {
	r, _ := sfTestSinkState(key)
	r.Store(reachable)
}

type sfTestFlakySink struct {
	reachable *atomic.Bool
	delivered chan string
}

func (n *sfTestFlakySink) Process(ctx context.Context, in datagram.Datagram) ([]PortDatagram, error) {
	if !n.reachable.Load() {
		return nil, fmt.Errorf("sf-test-flaky-sink: destination unreachable")
	}
	v, _ := in.Payload.Value.(string)
	select {
	case n.delivered <- v:
	case <-ctx.Done():
	}
	return nil, nil
}

func newSFTestFlakySink(raw json.RawMessage) (any, error) {
	var cfg struct {
		Key string `json:"key"`
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	reachable, delivered := sfTestSinkState(cfg.Key)
	return &sfTestFlakySink{reachable: reachable, delivered: delivered}, nil
}

func init() {
	Register("sf-test-flaky-sink", NodeTypeInfo{Kind: KindProcessor, Inputs: []string{"in"}}, newSFTestFlakySink)
}

// storeForwardFlowFile builds source -> flaky-sink, streaming mode, with
// the sink configured for EDGE-130's onError:"storeForward" policy.
func storeForwardFlowFile(t *testing.T, key string) *FlowFile {
	return &FlowFile{
		FormatVersion: 1, Kind: KindFlow, ID: "flow_sf_" + key, Name: "t", Mode: ModeStreaming,
		Graph: Graph{
			Nodes: []Node{
				{ID: "src", Type: "sf-test-source", TypeVersion: 1, Config: rawConfig(t, map[string]any{"key": key})},
				{
					ID: "sink", Type: "sf-test-flaky-sink", TypeVersion: 1,
					Config: rawConfig(t, map[string]any{"key": key}),
					ErrorPolicy: &ErrorPolicy{
						OnError:      "storeForward",
						StoreForward: &StoreForwardPolicy{MaxSizeMb: 0, MaxAgeSec: 0},
					},
				},
			},
			Wires: []Wire{
				{ID: "w1", From: Endpoint{Node: "src", Port: "out"}, To: Endpoint{Node: "sink", Port: "in"}},
			},
		},
	}
}

func TestEDGE130_StoreForwardQueuesWhileUnreachableAndDrainsOnRecovery(t *testing.T) {
	key := "recovery"
	sfTestSetReachable(key, false)
	_, delivered := sfTestSinkState(key)

	d := NewDeployment(testLogger())
	defer d.Stop()
	d.SetDataDir(t.TempDir())
	if err := d.Deploy(context.Background(), storeForwardFlowFile(t, key)); err != nil {
		t.Fatalf("Deploy: %v", err)
	}

	sfTestSourceChannel(key) <- "a"
	sfTestSourceChannel(key) <- "b"
	sfTestSourceChannel(key) <- "c"

	select {
	case v := <-delivered:
		t.Fatalf("destination unreachable but a value was still delivered: %q", v)
	case <-time.After(300 * time.Millisecond):
	}

	sfTestSetReachable(key, true)

	for _, want := range []string{"a", "b", "c"} {
		select {
		case v := <-delivered:
			if v != want {
				t.Fatalf("delivered %q, want %q (order must be preserved)", v, want)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %q to drain after recovery", want)
		}
	}
}

func TestEDGE130_StoreForwardQueueSurvivesDeploymentRestart(t *testing.T) {
	key := "restart"
	sfTestSetReachable(key, false)
	_, delivered := sfTestSinkState(key)
	dataDir := t.TempDir()

	d1 := NewDeployment(testLogger())
	d1.SetDataDir(dataDir)
	if err := d1.Deploy(context.Background(), storeForwardFlowFile(t, key)); err != nil {
		t.Fatalf("Deploy (first process): %v", err)
	}
	sfTestSourceChannel(key) <- "queued-before-restart"

	queueDir := filepath.Join(dataDir, "storeforward", "flow_sf_"+key, "sink")
	if !waitFor(t, func() bool {
		entries, err := os.ReadDir(queueDir)
		return err == nil && len(entries) > 0
	}, 2*time.Second) {
		t.Fatal("expected the entry to be durably queued to disk before simulating a restart")
	}
	d1.Stop() // simulate the runtime process stopping

	// A brand-new Deployment (standing in for the runtime process
	// restarting) over the SAME data directory must recover the queued
	// entry and still deliver it once the destination is reachable — this
	// is EDGE-130's actual point: surviving a real process restart, not
	// just an in-process hot redeploy.
	d2 := NewDeployment(testLogger())
	defer d2.Stop()
	d2.SetDataDir(dataDir)
	if err := d2.Deploy(context.Background(), storeForwardFlowFile(t, key)); err != nil {
		t.Fatalf("Deploy (second process): %v", err)
	}

	sfTestSetReachable(key, true)

	select {
	case v := <-delivered:
		if v != "queued-before-restart" {
			t.Fatalf("delivered %q, want the entry queued before the simulated restart", v)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the recovered queue to drain after restart")
	}
}
