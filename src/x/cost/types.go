// Package cost provides facilities for estimating the cost of operations
// and enforcing limits on them.
package cost

import (
	"math"
)

// Cost represents the cost of an operation.
type Cost float64

const (
	// MaxCost is the maximum cost of an operation.
	MaxCost = Cost(math.MaxFloat64)

	// MinCost is the minimum cost of an operation.
	MinCost = Cost(0)
)

// Limit encapulates the configuration of a cost limit for an operation.
type Limit struct {
	Threshold Cost
	Enabled   bool
}

// LimitManager manages configuration of a cost limit for an operation.
type LimitManager interface {
	// Limit returns the current cost limit for an operation.
	Limit() Limit

	// Report reports metrics on the state of the manager.
	Report()

	// Close closes the manager.
	Close()
}

// Costable is an operation that can return its cost.
type Costable interface {
	Cost() (Cost, error)
}

// Tracker tracks the cost of operations seen so far.
type Tracker interface {
	// Add adds c to the tracker's current cost total and returns the new total.
	Add(c Cost) Cost

	// Current returns the tracker's current cost total.
	Current() Cost
}
