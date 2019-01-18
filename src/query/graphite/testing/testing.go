package testing

import (
	"math/rand"
	"time"
)

// A Datapoint is a datapoint (timestamp, value, optional series) used in testing
type Datapoint struct {
	SeriesName string
	Timestamp  time.Time
	Value      float64
}

// Datapoints is a set of datapoints
type Datapoints []Datapoint

// Shuffle randomizes the set of datapoints
func (pts Datapoints) Shuffle() {
	for i := len(pts) - 1; i > 0; i-- {
		if j := rand.Intn(i + 1); i != j {
			pts[i], pts[j] = pts[j], pts[i]
		}
	}
}
