package retry

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
)

const (
	int64Overflow = 512
	maxInt64      = float64(math.MaxInt64 - int64Overflow)
)

// Backoff is a time.Duration counter, starting at Min. After every call to
// the Duration method the current timing is multiplied by Factor, but it
// never exceeds Max.
//
// Backoff is not generally concurrent-safe, but the ForAttempt method can
// be used concurrently.
type Backoff struct {
	attempt uint64
	// Factor is the multiplying factor for each increment step
	Factor float64
	// Jitter eases contention by randomizing backoff steps
	Jitter bool
	// Min and Max are the minimum and maximum values of the counter
	Min, Max time.Duration
}

// DurationForAttempt returns the duration for a specific attempt. This is useful if
// you have a large number of independent Backoffs, but don't want use
// unnecessary memory storing the Backoff parameters per Backoff. The first
// attempt should be 0.
//
// ForAttempt is concurrent-safe.
func (b *Backoff) DurationForAttempt(attempt int) time.Duration {
	// Zero-values are nonsensical, so we use
	// them to apply defaults
	min := b.Min
	if min <= 0 {
		min = 100 * time.Millisecond
	}
	max := b.Max
	if max <= 0 {
		max = 10 * time.Second
	}
	if min >= max {
		// short-circuit
		return max
	}
	factor := b.Factor
	if factor <= 0 {
		factor = 2
	}
	// calculate this duration
	minf := float64(min)
	durf := minf * math.Pow(factor, float64(attempt))
	if b.Jitter {
		durf = rand.Float64()*(durf-minf) + minf
	}
	// ensure float64 wont overflow int64
	if durf > maxInt64 {
		return max
	}
	dur := time.Duration(durf)
	// keep within bounds
	if dur < min {
		return min
	}
	if dur > max {
		return max
	}
	return dur
}

// Reset restarts the current attempt counter at zero.
func (b *Backoff) Reset() {
	atomic.StoreUint64(&b.attempt, 0)
}
