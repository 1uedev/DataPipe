// Package procstats samples this process's own CPU and memory usage for
// fleet health reporting (Increment 9, EDGE-120 "inventory with health
// (online, CPU, memory, flow status, versions)"), sent on every Heartbeat.
package procstats

import (
	"runtime"
	"sync"
	"syscall"
	"time"
)

// Sampler tracks CPU time between calls to CPUPercent so it can report a
// percentage rather than a cumulative total. Safe for concurrent use,
// though in practice only the heartbeat loop calls it.
type Sampler struct {
	mu       sync.Mutex
	lastWall time.Time
	lastCPU  time.Duration
}

func NewSampler() *Sampler {
	return &Sampler{lastWall: time.Now()}
}

// CPUPercent returns this process's CPU usage (user+system time) as a
// percentage of one core since the previous call (0 on the first call,
// with nothing to compare against). Can exceed 100 if more than one core
// was busy in the interval, matching what "top"-style tools report.
func (s *Sampler) CPUPercent() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	cpu := timevalToDuration(ru.Utime) + timevalToDuration(ru.Stime)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	wallDelta := now.Sub(s.lastWall)
	cpuDelta := cpu - s.lastCPU
	s.lastWall, s.lastCPU = now, cpu

	if wallDelta <= 0 {
		return 0
	}
	return float64(cpuDelta) / float64(wallDelta) * 100
}

// MemoryBytes returns this process's current heap+runtime memory
// footprint (Go's own view via runtime.MemStats.Sys — the memory
// obtained from the OS — not full RSS, but portable with no extra
// dependency and a reasonable proxy for EDGE-110's footprint target).
func MemoryBytes() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Sys
}

// timevalToDuration converts a syscall.Timeval (whose Usec field is
// int32 on some platforms, int64 on others) to a time.Duration without
// depending on that platform-specific width.
func timevalToDuration(tv syscall.Timeval) time.Duration {
	return time.Duration(tv.Sec)*time.Second + time.Duration(tv.Usec)*time.Microsecond
}
