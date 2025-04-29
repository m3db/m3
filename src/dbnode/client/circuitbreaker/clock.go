package circuitbreaker

import "time"

// clock is a source of time for the circuit breaker.
type clock interface {
	// Now returns the current time.
	Now() time.Time
}

var _baseClock clock = baseClock{}

// baseClock implements Clock that uses time.Now method internally.
type baseClock struct{}

// Now returns the current time from time package.
func (baseClock) Now() time.Time {
	return time.Now()
}
