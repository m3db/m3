package cost

import "go.uber.org/atomic"

type tracker struct {
	total *atomic.Float64
}

// NewTracker returns a new Tracker which maintains a simple running total of all the
// costs it has seen so far.
func NewTracker() Tracker         { return tracker{total: atomic.NewFloat64(0)} }
func (t tracker) Add(c Cost) Cost { return Cost(t.total.Add(float64(c))) }
func (t tracker) Current() Cost   { return Cost(t.total.Load()) }

type noopTracker struct{}

// NewNoopTracker returns a tracker which always always returns a cost of 0.
func NewNoopTracker() Tracker         { return noopTracker{} }
func (t noopTracker) Add(c Cost) Cost { return 0 }
func (t noopTracker) Current() Cost   { return 0 }
