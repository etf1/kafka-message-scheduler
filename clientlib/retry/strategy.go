package retry

import "time"

// Strategy represents a retry strategy.
// It can be a backoff strategy or something else.
type Strategy interface {
	DurationForAttempt(attempt int) time.Duration
	Reset()
}
