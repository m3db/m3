package testing

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/stretchr/testify/assert"
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

// Equalish asserts that two objects are equal. Looser than assert.Equal since
// it checks for equality of printing expected vs printing actual.
//
//    assert.Equal(t, 123, 123, "123 and 123 should be equal")
//
// Returns whether the assertion was successful (true) or not (false).
func Equalish(
	t assert.TestingT,
	expected, actual interface{},
	msgAndArgs ...interface{},
) bool {
	if assert.ObjectsAreEqual(expected, actual) {
		return true
	}

	// Last ditch effort
	if fmt.Sprintf("%#v", expected) == fmt.Sprintf("%#v", actual) {
		return true
	}

	return assert.Fail(t, fmt.Sprintf("Not equal: %#v (expected)\n"+
		"        != %#v (actual)", expected, actual), msgAndArgs...)
}
