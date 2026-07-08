package flow

import "sync/atomic"

// procHistogramBucketsMs are the cumulative upper bounds (Prometheus
// histogram convention: each bucket counts observations <= its bound) for
// OBS-100's "processing time percentiles" — a fixed-bucket histogram is the
// standard Prometheus representation quantiles are computed from
// (histogram_quantile), not a client-side percentile calculation.
var procHistogramBucketsMs = []float64{1, 5, 10, 50, 100, 500, 1000, 5000}

// procHistogram is a minimal Prometheus-style cumulative histogram: one
// atomic counter per bucket bound, plus sum and count, safe for concurrent
// Observe from many node goroutines.
type procHistogram struct {
	buckets []atomic.Uint64
	sumUs   atomic.Uint64 // microseconds, to keep the accumulator an integer
	count   atomic.Uint64
}

func newProcHistogram() *procHistogram {
	return &procHistogram{buckets: make([]atomic.Uint64, len(procHistogramBucketsMs))}
}

// Observe records one processing-time sample in microseconds.
func (h *procHistogram) Observe(us uint64) {
	h.sumUs.Add(us)
	h.count.Add(1)
	ms := float64(us) / 1000
	for i, bound := range procHistogramBucketsMs {
		if ms <= bound {
			h.buckets[i].Add(1)
		}
	}
}

// HistogramSnapshot is a plain-value copy of procHistogram.
type HistogramSnapshot struct {
	BucketBounds []float64 // ms, ascending, matches BucketCounts index-for-index
	BucketCounts []uint64  // cumulative, per Prometheus histogram convention
	SumUs        uint64
	Count        uint64
}

func (h *procHistogram) Snapshot() HistogramSnapshot {
	counts := make([]uint64, len(procHistogramBucketsMs))
	for i := range h.buckets {
		counts[i] = h.buckets[i].Load()
	}
	return HistogramSnapshot{
		BucketBounds: append([]float64(nil), procHistogramBucketsMs...),
		BucketCounts: counts,
		SumUs:        h.sumUs.Load(),
		Count:        h.count.Load(),
	}
}
