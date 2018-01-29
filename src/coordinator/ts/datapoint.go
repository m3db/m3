package ts

import (
	"math"
	"time"
)

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// ValueIsNaN returns true iff underlying value is NaN
func (d *Datapoint) ValueIsNaN() bool { return math.IsNaN(d.Value) }

// Datapoints is a list of datapoints.
type Datapoints []*Datapoint

// Len is the length of the array.
func (d Datapoints) Len() int { return len(d) }

// ValueAt returns the value at the nth element.
func (d Datapoints) ValueAt(n int) float64 { return d[n].Value }

// AllNaN returns true if all the values are NaN
func (d Datapoints) AllNaN() bool {
	for _, dp := range d {
		if !dp.ValueIsNaN() {
			return false
		}
	}
	return true
}
