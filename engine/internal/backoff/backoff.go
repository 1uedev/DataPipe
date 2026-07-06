// Package backoff implements the shared reconnect backoff used by the
// runtime's control-plane connection and, later, by connectors (CON-130) so
// no connector has to own its own retry loop.
package backoff

import (
	"math/rand"
	"time"
)

type Backoff struct {
	Min    time.Duration
	Max    time.Duration
	Factor float64

	attempt int
}

func New(min, max time.Duration, factor float64) *Backoff {
	return &Backoff{Min: min, Max: max, Factor: factor}
}

// Next returns the delay before the next attempt and advances the sequence.
func (b *Backoff) Next() time.Duration {
	d := float64(b.Min) * pow(b.Factor, b.attempt)
	if d > float64(b.Max) {
		d = float64(b.Max)
	} else {
		b.attempt++
	}
	jitter := 0.8 + 0.4*rand.Float64() // +/-20%
	return time.Duration(d * jitter)
}

// Reset clears the sequence, e.g. after a successful connection.
func (b *Backoff) Reset() {
	b.attempt = 0
}

func pow(base float64, exp int) float64 {
	result := 1.0
	for i := 0; i < exp; i++ {
		result *= base
	}
	return result
}
