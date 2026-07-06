package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
)

// DefaultPreviewMaxRecords and DefaultPreviewTimeout bound MAP-110's
// "fetch sample now": a preview must never run unbounded, since it's driven
// directly by an editor request.
const (
	DefaultPreviewMaxRecords = 10
	DefaultPreviewTimeout    = 10 * time.Second
)

// PreviewSource runs a Source node type in isolation, collecting up to
// maxRecords emitted datagrams (or until timeout/ctx cancellation), for
// MAP-110's "fetch sample now" preview — "show real data (max N records)
// ... before the flow is deployed." Like ExecuteNode, this is a pure
// in-process call: no Deployment, no wires. If the node references a
// connection, inject a resolver into ctx via WithConnection first. Panics
// inside the node are recovered (ARC-150).
func PreviewSource(ctx context.Context, nodeType string, config json.RawMessage, maxRecords int, timeout time.Duration) ([]PortDatagram, error) {
	info, factory, ok := Lookup(nodeType)
	if !ok {
		return nil, fmt.Errorf("flow: unknown node type %q", nodeType)
	}
	if info.Kind != KindSource {
		return nil, fmt.Errorf("flow: node type %q is not a Source; preview only applies to sources (MAP-110)", nodeType)
	}
	instance, err := factory(config)
	if err != nil {
		return nil, fmt.Errorf("flow: configuring node %q: %w", nodeType, err)
	}
	src, ok := instance.(Source)
	if !ok {
		return nil, fmt.Errorf("flow: node type %q factory did not return a Source", nodeType)
	}

	if maxRecords <= 0 {
		maxRecords = DefaultPreviewMaxRecords
	}
	if timeout <= 0 {
		timeout = DefaultPreviewTimeout
	}

	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var mu sync.Mutex
	var collected []PortDatagram
	emit := func(port string, d datagram.Datagram) error {
		mu.Lock()
		collected = append(collected, PortDatagram{Port: port, Datagram: d})
		full := len(collected) >= maxRecords
		mu.Unlock()
		if full {
			cancel() // enough samples: stop the source rather than waiting out the full timeout
		}
		return nil
	}

	done := make(chan struct{})
	var runErr error
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				runErr = fmt.Errorf("flow: node type %q panicked during preview: %v", nodeType, r)
			}
		}()
		runErr = src.Run(runCtx, emit)
	}()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(collected) == 0 && runErr != nil && !errors.Is(runErr, context.Canceled) && !errors.Is(runErr, context.DeadlineExceeded) {
		return nil, runErr
	}
	return collected, nil
}
