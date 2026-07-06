package topics

import (
	"context"
	"testing"
	"time"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
)

func testDgm(v int) datagram.Datagram {
	return datagram.New(datagram.Source{NodeID: "test"}, datagram.Payload{Value: v})
}

func recvOrTimeout(t *testing.T, wire *bus.Wire) (datagram.Datagram, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	d, err := wire.Receive(ctx)
	return d, err == nil
}

func TestBUS120_PublishDeliversToMatchingSubscriber(t *testing.T) {
	b := NewBroker()
	wire, cancel := b.Subscribe("sensors/kitchen/temp", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer cancel()

	b.Publish(context.Background(), "sensors/kitchen/temp", nil, testDgm(1))

	d, ok := recvOrTimeout(t, wire)
	if !ok {
		t.Fatal("expected the exact-match subscriber to receive the datagram")
	}
	if v, _ := d.Payload.Value.(int); v != 1 {
		t.Errorf("payload = %v, want 1", d.Payload.Value)
	}
}

func TestBUS120_SingleLevelWildcard(t *testing.T) {
	b := NewBroker()
	wire, cancel := b.Subscribe("sensors/+/temp", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer cancel()

	b.Publish(context.Background(), "sensors/kitchen/temp", nil, testDgm(1))
	if _, ok := recvOrTimeout(t, wire); !ok {
		t.Fatal("expected + to match a single level")
	}

	b.Publish(context.Background(), "sensors/kitchen/humidity/hourly", nil, testDgm(2))
	if _, ok := recvOrTimeout(t, wire); ok {
		t.Fatal("+ must not match multiple levels or a different suffix")
	}
}

func TestBUS120_MultiLevelWildcard(t *testing.T) {
	b := NewBroker()
	wire, cancel := b.Subscribe("sensors/#", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer cancel()

	b.Publish(context.Background(), "sensors/kitchen/temp", nil, testDgm(1))
	if _, ok := recvOrTimeout(t, wire); !ok {
		t.Fatal("expected # to match any depth under sensors/")
	}

	b.Publish(context.Background(), "other/topic", nil, testDgm(2))
	if _, ok := recvOrTimeout(t, wire); ok {
		t.Fatal("# under sensors/ must not match an unrelated top-level topic")
	}
}

func TestBUS120_TagFilterRequiresSubsetMatch(t *testing.T) {
	b := NewBroker()
	wire, cancel := b.Subscribe("sensors/#", map[string]string{"site": "A"}, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer cancel()

	b.Publish(context.Background(), "sensors/temp", map[string]string{"site": "B"}, testDgm(1))
	if _, ok := recvOrTimeout(t, wire); ok {
		t.Fatal("a non-matching tag value must not be delivered")
	}

	b.Publish(context.Background(), "sensors/temp", map[string]string{"site": "A", "line": "3"}, testDgm(2))
	if _, ok := recvOrTimeout(t, wire); !ok {
		t.Fatal("extra tags on the publish side beyond the filter should still match")
	}
}

func TestBUS120_FanOutToMultipleSubscribersIndependentClones(t *testing.T) {
	b := NewBroker()
	w1, c1 := b.Subscribe("topic", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer c1()
	w2, c2 := b.Subscribe("topic", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	defer c2()

	b.Publish(context.Background(), "topic", nil, testDgm(42))

	d1, ok1 := recvOrTimeout(t, w1)
	d2, ok2 := recvOrTimeout(t, w2)
	if !ok1 || !ok2 {
		t.Fatal("expected both subscribers to receive the datagram")
	}
	if d1.Header.ID != d2.Header.ID {
		t.Errorf("fan-out clones should share the same logical event id: %s vs %s", d1.Header.ID, d2.Header.ID)
	}
}

func TestBUS120_UnsubscribeStopsDelivery(t *testing.T) {
	b := NewBroker()
	wire, cancel := b.Subscribe("topic", nil, bus.WireConfig{Capacity: 4, Overflow: bus.OverflowDropOldest})
	cancel()

	b.Publish(context.Background(), "topic", nil, testDgm(1))
	if _, ok := recvOrTimeout(t, wire); ok {
		t.Fatal("expected no delivery after unsubscribe")
	}
}

func TestBUS110_SubscriberOverflowPolicyBoundsTheQueue(t *testing.T) {
	b := NewBroker()
	wire, cancel := b.Subscribe("topic", nil, bus.WireConfig{Capacity: 2, Overflow: bus.OverflowDropOldest})
	defer cancel()

	for i := 0; i < 10; i++ {
		b.Publish(context.Background(), "topic", nil, testDgm(i))
	}
	if depth := wire.Depth(); depth > 2 {
		t.Errorf("subscriber queue depth = %d, want <= 2 (bounded, BUS-110)", depth)
	}
	if m := wire.Metrics(); m.Dropped == 0 {
		t.Error("expected some drops to be counted once the bounded queue overflowed")
	}
}
