// Package obsmetrics formats engine-collected metrics (OBS-100: "per node
// throughput in/out, error count, drop count, queue depth, processing time
// percentiles; per flow; per runtime CPU/memory") into the Prometheus text
// exposition format — the "standard scrapeable format" the requirement
// calls for.
package obsmetrics

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/1uedev/DataPipe/engine/flow"
)

// RuntimeInfo is the per-runtime gauges to include alongside deployment
// metrics (CPU/memory; "event loop/scheduler lag" is not tracked — Go has
// no single-threaded event loop to measure the way Node.js does, so it is
// deliberately omitted rather than reported as a meaningless placeholder).
type RuntimeInfo struct {
	RuntimeID   string
	CPUPercent  float64
	MemoryBytes uint64
}

// FormatPrometheus renders dm (one running deployment's node/wire metrics)
// plus rt (runtime-level gauges) as Prometheus text exposition format.
func FormatPrometheus(dm flow.DeploymentMetrics, rt RuntimeInfo) string {
	var b strings.Builder

	writeRuntimeGauges(&b, rt)
	writeNodeMetrics(&b, dm)
	writeWireMetrics(&b, dm)

	return b.String()
}

func writeRuntimeGauges(b *strings.Builder, rt RuntimeInfo) {
	rl := label{"runtime", rt.RuntimeID}
	fmt.Fprintln(b, "# HELP datapipe_runtime_cpu_percent Runtime process CPU usage percent, sampled since the last Heartbeat.")
	fmt.Fprintln(b, "# TYPE datapipe_runtime_cpu_percent gauge")
	fmt.Fprintf(b, "datapipe_runtime_cpu_percent{%s} %s\n", labels(rl), formatFloat(rt.CPUPercent))

	fmt.Fprintln(b, "# HELP datapipe_runtime_memory_bytes Runtime process memory usage in bytes.")
	fmt.Fprintln(b, "# TYPE datapipe_runtime_memory_bytes gauge")
	fmt.Fprintf(b, "datapipe_runtime_memory_bytes{%s} %d\n", labels(rl), rt.MemoryBytes)
}

func writeNodeMetrics(b *strings.Builder, dm flow.DeploymentMetrics) {
	ids := sortedKeys(dm.Nodes)

	fmt.Fprintln(b, "# HELP datapipe_node_processed_total Datagrams successfully processed by this node.")
	fmt.Fprintln(b, "# TYPE datapipe_node_processed_total counter")
	for _, id := range ids {
		fmt.Fprintf(b, "datapipe_node_processed_total{%s} %d\n", nodeLabels(dm.FlowID, id), dm.Nodes[id].Metrics.Processed)
	}

	fmt.Fprintln(b, "# HELP datapipe_node_errors_total Node invocations that ended in an error.")
	fmt.Fprintln(b, "# TYPE datapipe_node_errors_total counter")
	for _, id := range ids {
		fmt.Fprintf(b, "datapipe_node_errors_total{%s} %d\n", nodeLabels(dm.FlowID, id), dm.Nodes[id].Metrics.Errors)
	}

	fmt.Fprintln(b, "# HELP datapipe_node_retries_total Retry attempts made under an \"onError: retry\" policy.")
	fmt.Fprintln(b, "# TYPE datapipe_node_retries_total counter")
	for _, id := range ids {
		fmt.Fprintf(b, "datapipe_node_retries_total{%s} %d\n", nodeLabels(dm.FlowID, id), dm.Nodes[id].Metrics.Retries)
	}

	fmt.Fprintln(b, "# HELP datapipe_node_processing_duration_seconds Per-invocation processing time.")
	fmt.Fprintln(b, "# TYPE datapipe_node_processing_duration_seconds histogram")
	for _, id := range ids {
		h := dm.Nodes[id].Metrics.Processing
		nl := nodeLabels(dm.FlowID, id)
		for i, boundMs := range h.BucketBounds {
			fmt.Fprintf(b, "datapipe_node_processing_duration_seconds_bucket{%s,le=\"%s\"} %d\n", nl, formatFloat(boundMs/1000), h.BucketCounts[i])
		}
		fmt.Fprintf(b, "datapipe_node_processing_duration_seconds_bucket{%s,le=\"+Inf\"} %d\n", nl, h.Count)
		fmt.Fprintf(b, "datapipe_node_processing_duration_seconds_sum{%s} %s\n", nl, formatFloat(float64(h.SumUs)/1e6))
		fmt.Fprintf(b, "datapipe_node_processing_duration_seconds_count{%s} %d\n", nl, h.Count)
	}
}

func writeWireMetrics(b *strings.Builder, dm flow.DeploymentMetrics) {
	wires := append([]flow.WireStats(nil), dm.Wires...)
	sort.Slice(wires, func(i, j int) bool {
		if wires[i].FromNode != wires[j].FromNode {
			return wires[i].FromNode < wires[j].FromNode
		}
		return wires[i].FromPort < wires[j].FromPort
	})

	fmt.Fprintln(b, "# HELP datapipe_wire_queue_depth Current number of datagrams buffered on this wire.")
	fmt.Fprintln(b, "# TYPE datapipe_wire_queue_depth gauge")
	for _, w := range wires {
		fmt.Fprintf(b, "datapipe_wire_queue_depth{%s} %d\n", wireLabels(dm.FlowID, w), w.Depth)
	}

	fmt.Fprintln(b, "# HELP datapipe_wire_queue_capacity Configured bounded-queue size for this wire (BUS-110).")
	fmt.Fprintln(b, "# TYPE datapipe_wire_queue_capacity gauge")
	for _, w := range wires {
		fmt.Fprintf(b, "datapipe_wire_queue_capacity{%s} %d\n", wireLabels(dm.FlowID, w), w.Capacity)
	}

	fmt.Fprintln(b, "# HELP datapipe_wire_delivered_total Datagrams delivered across this wire.")
	fmt.Fprintln(b, "# TYPE datapipe_wire_delivered_total counter")
	for _, w := range wires {
		fmt.Fprintf(b, "datapipe_wire_delivered_total{%s} %d\n", wireLabels(dm.FlowID, w), w.Metrics.Delivered)
	}

	fmt.Fprintln(b, "# HELP datapipe_wire_dropped_total Datagrams dropped by this wire's overflow policy (BUS-110).")
	fmt.Fprintln(b, "# TYPE datapipe_wire_dropped_total counter")
	for _, w := range wires {
		fmt.Fprintf(b, "datapipe_wire_dropped_total{%s} %d\n", wireLabels(dm.FlowID, w), w.Metrics.Dropped)
	}
}

type label struct{ name, value string }

func labels(ls ...label) string {
	parts := make([]string, len(ls))
	for i, l := range ls {
		parts[i] = fmt.Sprintf("%s=%q", l.name, escape(l.value))
	}
	return strings.Join(parts, ",")
}

func nodeLabels(flowID, nodeID string) string {
	return labels(label{"flow", flowID}, label{"node", nodeID})
}

func wireLabels(flowID string, w flow.WireStats) string {
	return labels(
		label{"flow", flowID},
		label{"from_node", w.FromNode}, label{"from_port", w.FromPort},
		label{"to_node", w.ToNode}, label{"to_port", w.ToPort},
	)
}

// escape applies the Prometheus text-format label-value escaping rules
// (backslash, double quote, newline).
func escape(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	s = strings.ReplaceAll(s, "\n", `\n`)
	return s
}

func formatFloat(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func sortedKeys(m map[string]flow.NodeStats) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
