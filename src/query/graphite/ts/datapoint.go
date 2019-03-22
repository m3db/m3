// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package ts

import (
	"math"
	"time"

	"github.com/m3db/m3/src/query/graphite/stats"
)

// A Datapoint is a single data value reported at a given time
type Datapoint struct {
	Timestamp time.Time
	Value     float64
}

// ValueIsNaN returns true iff underlying value is NaN
func (d Datapoint) ValueIsNaN() bool { return math.IsNaN(d.Value) }

// DatapointsByTimestamp is a sortable interface for datapoints
type DatapointsByTimestamp []Datapoint

// Len is the length of the datapoints
func (p DatapointsByTimestamp) Len() int { return len(p) }

// Less compares two datapoints by timestamp
func (p DatapointsByTimestamp) Less(i, j int) bool { return p[i].Timestamp.Before(p[j].Timestamp) }

// Swap swaps two datapoints
func (p DatapointsByTimestamp) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// ConsolidatedValue represents a time window of consolidated data
type ConsolidatedValue struct {
	// StartTime is the start time of the time window covered by this
	// consolidation
	StartTime time.Time

	// EndTime is the end time of the time window covered by this
	// consolidation
	EndTime time.Time

	// Values is the statistics for that consolidated time window
	Values stats.Statistics
}

// ConsolidatedValuesByStartTime is a sortable interface for consolidated values
type ConsolidatedValuesByStartTime []ConsolidatedValue

// Len is the length of the values
func (p ConsolidatedValuesByStartTime) Len() int { return len(p) }

// Less compares two values by start time
func (p ConsolidatedValuesByStartTime) Less(i, j int) bool {
	return p[i].StartTime.Before(p[j].StartTime)
}

// Swap swaps two values
func (p ConsolidatedValuesByStartTime) Swap(i, j int) { p[i], p[j] = p[j], p[i] }

// Datapoints is a list of datapoints that implement the stats.Values interface.
type Datapoints []Datapoint

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
