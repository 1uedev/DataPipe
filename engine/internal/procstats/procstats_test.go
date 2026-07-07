package procstats

import "testing"

func TestEDGE120_MemoryBytesReturnsNonZero(t *testing.T) {
	if MemoryBytes() == 0 {
		t.Fatal("expected a non-zero memory footprint")
	}
}

func TestEDGE120_CPUPercentDoesNotPanicAcrossCalls(t *testing.T) {
	s := NewSampler()
	first := s.CPUPercent()
	if first < 0 {
		t.Fatalf("first sample should never be negative, got %v", first)
	}
	// Burn a little CPU so the second sample has something to measure.
	sum := 0
	for i := 0; i < 10_000_000; i++ {
		sum += i
	}
	if sum == 0 {
		t.Fatal("unreachable")
	}
	second := s.CPUPercent()
	if second < 0 {
		t.Fatalf("cpu percent should never be negative, got %v", second)
	}
}
