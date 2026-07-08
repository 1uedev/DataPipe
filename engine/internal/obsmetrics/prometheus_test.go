package obsmetrics

import (
	"strings"
	"testing"

	"github.com/1uedev/DataPipe/engine/bus"
	"github.com/1uedev/DataPipe/engine/flow"
)

func TestOBS100_FormatPrometheusIncludesNodeAndWireMetrics(t *testing.T) {
	dm := flow.DeploymentMetrics{
		FlowID: "flow-1",
		Nodes: map[string]flow.NodeStats{
			"node-a": {Running: true, StartCount: 1, Metrics: flow.MetricsSnapshot{
				Processed: 42, Errors: 3, Retries: 1,
				Processing: flow.HistogramSnapshot{
					BucketBounds: []float64{1, 5},
					BucketCounts: []uint64{10, 20},
					SumUs:        123456,
					Count:        20,
				},
			}},
		},
		Wires: []flow.WireStats{
			{FromNode: "node-a", FromPort: "out", ToNode: "node-b", ToPort: "in", Depth: 3, Capacity: 1024, Metrics: bus.Metrics{Delivered: 100, Dropped: 2}},
		},
	}

	out := FormatPrometheus(dm, RuntimeInfo{RuntimeID: "rt-1", CPUPercent: 12.5, MemoryBytes: 1048576})

	checks := []string{
		`datapipe_runtime_cpu_percent{runtime="rt-1"} 12.5`,
		`datapipe_runtime_memory_bytes{runtime="rt-1"} 1048576`,
		`datapipe_node_processed_total{flow="flow-1",node="node-a"} 42`,
		`datapipe_node_errors_total{flow="flow-1",node="node-a"} 3`,
		`datapipe_node_retries_total{flow="flow-1",node="node-a"} 1`,
		`datapipe_node_processing_duration_seconds_bucket{flow="flow-1",node="node-a",le="0.001"} 10`,
		`datapipe_node_processing_duration_seconds_bucket{flow="flow-1",node="node-a",le="+Inf"} 20`,
		`datapipe_node_processing_duration_seconds_count{flow="flow-1",node="node-a"} 20`,
		`datapipe_wire_queue_depth{flow="flow-1",from_node="node-a",from_port="out",to_node="node-b",to_port="in"} 3`,
		`datapipe_wire_queue_capacity{flow="flow-1",from_node="node-a",from_port="out",to_node="node-b",to_port="in"} 1024`,
		`datapipe_wire_delivered_total{flow="flow-1",from_node="node-a",from_port="out",to_node="node-b",to_port="in"} 100`,
		`datapipe_wire_dropped_total{flow="flow-1",from_node="node-a",from_port="out",to_node="node-b",to_port="in"} 2`,
	}
	for _, want := range checks {
		if !strings.Contains(out, want) {
			t.Errorf("output missing line %q\nfull output:\n%s", want, out)
		}
	}
}

func TestOBS100_EscapesLabelValues(t *testing.T) {
	got := escape(`has "quotes" and \backslash\`)
	want := `has \"quotes\" and \\backslash\\`
	if got != want {
		t.Errorf("escape() = %q, want %q", got, want)
	}
}
