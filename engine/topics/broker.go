// Package topics implements BUS-120's named internal bus topics: flows
// publish datagrams to named topics ("Bus Out", SNK-220) and subscribe
// ("Bus In", CON-600) with MQTT-style wildcard support (+ single-level, #
// multi-level) and tag-based filtering — decoupled flow-to-flow
// communication within a runtime. Bus links to other runtimes (ARC-230) are
// out of scope for this package.
package topics

import (
	"context"
	"strings"
	"sync"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/datagram"
)

// DefaultBroker is the process-wide topic broker every bus-in/bus-out node
// instance publishes to and subscribes through, so named topics work across
// flows within the same runtime regardless of which one deployed a given
// node.
var DefaultBroker = NewBroker()

type subscription struct {
	pattern string
	tags    map[string]string
	wire    *bus.Wire
}

// Broker is a named-topic pub/sub registry. Safe for concurrent use.
type Broker struct {
	mu   sync.Mutex
	subs map[*subscription]struct{}
}

func NewBroker() *Broker {
	return &Broker{subs: map[*subscription]struct{}{}}
}

// Subscribe registers interest in topics matching pattern (MQTT-style
// wildcards) whose tags are a superset of the given filter (empty filter
// matches any tags). wireCfg bounds the subscriber's own queue (BUS-110:
// every queue has a bound and overflow policy) — a slow subscriber can
// never block a publisher or other subscribers when a non-blocking policy
// is used. The returned cancel func must be called to unsubscribe.
func (b *Broker) Subscribe(pattern string, tags map[string]string, wireCfg bus.WireConfig) (*bus.Wire, func()) {
	wire := bus.NewWire(wireCfg)
	sub := &subscription{pattern: pattern, tags: tags, wire: wire}

	b.mu.Lock()
	b.subs[sub] = struct{}{}
	b.mu.Unlock()

	cancel := func() {
		b.mu.Lock()
		delete(b.subs, sub)
		b.mu.Unlock()
		wire.Close()
	}
	return wire, cancel
}

// Publish delivers d to every current subscriber whose pattern matches
// topic and whose tag filter is satisfied by tags. Each subscriber gets an
// independent clone (BUS-140 fan-out semantics).
func (b *Broker) Publish(ctx context.Context, topic string, tags map[string]string, d datagram.Datagram) {
	b.mu.Lock()
	matches := make([]*subscription, 0, len(b.subs))
	for sub := range b.subs {
		if matchTopic(sub.pattern, topic) && tagsMatch(sub.tags, tags) {
			matches = append(matches, sub)
		}
	}
	b.mu.Unlock()

	for _, sub := range matches {
		_, _ = sub.wire.Send(ctx, d.Clone(datagram.DefaultBinaryRefThreshold))
	}
}

// matchTopic implements MQTT-style wildcard matching: "+" matches exactly
// one "/"-separated level, "#" matches the rest of the topic (must be the
// last segment of pattern).
func matchTopic(pattern, topic string) bool {
	pParts := strings.Split(pattern, "/")
	tParts := strings.Split(topic, "/")
	for i, p := range pParts {
		if p == "#" {
			return true
		}
		if i >= len(tParts) {
			return false
		}
		if p != "+" && p != tParts[i] {
			return false
		}
	}
	return len(pParts) == len(tParts)
}

// tagsMatch reports whether every key/value in filter is present in tags
// (an empty filter matches anything).
func tagsMatch(filter, tags map[string]string) bool {
	for k, v := range filter {
		if tags[k] != v {
			return false
		}
	}
	return true
}
