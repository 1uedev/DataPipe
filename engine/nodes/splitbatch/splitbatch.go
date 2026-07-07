// Package splitbatch implements the "split-batch" node (PROC-330): split an
// array payload into one datagram per element, or collect single
// datagrams into batches by count, time interval, or estimated byte size.
// Chunk-with-overlap (P2) is not implemented.
//
// A time- or size-triggered batch flush is checked reactively, on the next
// incoming datagram, not proactively on a background timer — the same
// engine/flow.Processor constraint documented for PROC-210/PROC-320: there
// is no lifecycle hook for a node to run its own background goroutine. A
// genuinely idle partial batch sits unflushed until either a new item
// arrives or the flow is redeployed.
package splitbatch

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/1uedev/DataPipe/engine/datagram"
	"github.com/1uedev/DataPipe/engine/flow"
)

const configSchema = `{
	"type": "object",
	"properties": {
		"mode": { "type": "string", "enum": ["split", "batch"] },
		"field": { "type": "string", "description": "mode \"split\": \".\"-separated path to the array to split; empty splits the whole payload." },
		"maxCount": { "type": "integer", "minimum": 1, "description": "mode \"batch\": emit once this many items have collected." },
		"maxIntervalMs": { "type": "integer", "minimum": 1, "description": "mode \"batch\": also emit once this long has passed since the batch's first item." },
		"maxBytes": { "type": "integer", "minimum": 1, "description": "mode \"batch\": also emit before a new item would push the batch's estimated JSON size over this limit." }
	},
	"required": ["mode"]
}`

func init() {
	flow.Register("split-batch", flow.NodeTypeInfo{
		Kind:         flow.KindProcessor,
		Inputs:       []string{"in"},
		Outputs:      []string{"out"},
		DisplayName:  "Split/Batch",
		Category:     flow.CategoryControl,
		Description:  "Split arrays into single datagrams, or collect singles into batches by count/time/size (PROC-330).",
		ConfigSchema: json.RawMessage(configSchema),
	}, New)
}

// Config is the "split-batch" node's "config" object.
type Config struct {
	Mode          string `json:"mode"`
	Field         string `json:"field,omitempty"`
	MaxCount      int    `json:"maxCount,omitempty"`
	MaxIntervalMs int    `json:"maxIntervalMs,omitempty"`
	MaxBytes      int    `json:"maxBytes,omitempty"`
}

type node struct {
	cfg Config

	// batch mode state
	batch      []any
	batchStart time.Time
	batchBytes int
}

// New is the flow.Factory for the "split-batch" node type.
func New(raw json.RawMessage) (any, error) {
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, err
	}
	switch cfg.Mode {
	case "split":
	case "batch":
		if cfg.MaxCount <= 0 && cfg.MaxIntervalMs <= 0 && cfg.MaxBytes <= 0 {
			return nil, fmt.Errorf("split-batch: mode \"batch\" requires at least one of maxCount/maxIntervalMs/maxBytes")
		}
	default:
		return nil, fmt.Errorf("split-batch: unknown mode %q", cfg.Mode)
	}
	return &node{cfg: cfg}, nil
}

func (n *node) Process(ctx context.Context, in datagram.Datagram) ([]flow.PortDatagram, error) {
	if n.cfg.Mode == "split" {
		return n.processSplit(in)
	}
	return n.processBatch(in)
}

func (n *node) processSplit(in datagram.Datagram) ([]flow.PortDatagram, error) {
	value := fieldValue(in.Payload.Value, n.cfg.Field)
	arr, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("split-batch: field %q is not an array (got %T)", n.cfg.Field, value)
	}
	results := make([]flow.PortDatagram, len(arr))
	for i, item := range arr {
		d := datagram.NewCaused(in, in.Header.Source, datagram.Payload{Value: item})
		results[i] = flow.PortDatagram{Port: "out", Datagram: d}
	}
	return results, nil
}

func (n *node) processBatch(in datagram.Datagram) ([]flow.PortDatagram, error) {
	itemBytes := estimateBytes(in.Payload.Value)
	now := time.Now()

	var results []flow.PortDatagram
	// A byte-size limit flushes the CURRENT batch before adding the new
	// item, since the new item is what would push it over.
	if n.cfg.MaxBytes > 0 && len(n.batch) > 0 && n.batchBytes+itemBytes > n.cfg.MaxBytes {
		results = append(results, n.flush(in))
	}

	if len(n.batch) == 0 {
		n.batchStart = now
	}
	n.batch = append(n.batch, in.Payload.Value)
	n.batchBytes += itemBytes

	full := n.cfg.MaxCount > 0 && len(n.batch) >= n.cfg.MaxCount
	expired := n.cfg.MaxIntervalMs > 0 && now.Sub(n.batchStart) >= time.Duration(n.cfg.MaxIntervalMs)*time.Millisecond
	if full || expired {
		results = append(results, n.flush(in))
	}
	return results, nil
}

// flush emits the current batch (using cause as the causing datagram for
// lineage) and resets state for the next batch.
func (n *node) flush(cause datagram.Datagram) flow.PortDatagram {
	d := datagram.NewCaused(cause, cause.Header.Source, datagram.Payload{Value: n.batch})
	n.batch = nil
	n.batchBytes = 0
	return flow.PortDatagram{Port: "out", Datagram: d}
}

func fieldValue(item any, path string) any {
	if path == "" {
		return item
	}
	cur := item
	for _, key := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[key]
	}
	return cur
}

func estimateBytes(v any) int {
	b, err := json.Marshal(v)
	if err != nil {
		return 0
	}
	return len(b)
}
