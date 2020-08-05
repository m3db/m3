// Copyright (c) 2020 Uber Technologies, Inc.
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

package tile

import "math"

// SeriesFrameSummary represents high level aggregations for summaries of
// values in a series frame.
type SeriesFrameSummary struct {
	set bool

	min   float64
	max   float64
	sum   float64
	last  float64
	count float64

	anyCounterResets bool
}

// Avg returns the average SeriesFrameSummary value.
func (s *SeriesFrameSummary) Avg() float64 {
	if s.count > 0 && s.set {
		return s.sum / s.count
	}
	return 0
}

// Min returns the minimum SeriesFrameSummary value.
func (s *SeriesFrameSummary) Min() float64 {
	return s.min
}

// Max returns the maximum SeriesFrameSummary value.
func (s *SeriesFrameSummary) Max() float64 {
	return s.max
}

// Sum returns the sum of all SeriesFrameSummary values.
func (s *SeriesFrameSummary) Sum() float64 {
	return s.sum
}

// Last returns the last SeriesFrameSummary value.
func (s *SeriesFrameSummary) Last() float64 {
	return s.last
}

// Count returns the number of SeriesFrameSummary values.
func (s *SeriesFrameSummary) Count() float64 {
	return s.count
}

// PossibleCounterResets returns true if there were any incoming values
// with a lower value than a previous point. SeriesFrameSummary tracks potential resets,
// since it is unknown at this point if the metric is a counter or gauge.
// Downstream consumers should decide if the values in this SeriesFrameSummary are valid;
// if there were resets and the metric in the SeriesFrameSummary is a counter, these
// values should be calculated from the raw data instead.
func (s *SeriesFrameSummary) PossibleCounterResets() bool {
	return s.anyCounterResets
}

// Valid returns true if this SeriesFrameSummary has valid values.
func (s *SeriesFrameSummary) Valid() bool {
	return s != nil && s.set
}

func newSeriesFrameSummary() *SeriesFrameSummary {
	s := &SeriesFrameSummary{}
	s.reset()
	return s
}

func (s *SeriesFrameSummary) reset() {
	s.set = false
	s.min = math.NaN()
	s.max = math.NaN()
	s.sum = math.NaN()
	s.last = math.NaN()
	s.count = 0
	s.anyCounterResets = false
}

func (s *SeriesFrameSummary) recordFirst(val float64) {
	s.set = true
	s.min = val
	s.max = val
	s.sum = val
	s.last = val
	s.count = 1
}

func (s *SeriesFrameSummary) record(val float64) {
	// NB: nan points do not contribute to result.
	if math.IsNaN(val) {
		s.count++
		return
	}

	if !s.set {
		s.recordFirst(val)
		return
	}

	if val < s.min {
		s.min = val
	}

	if val > s.max {
		s.max = val
	}

	s.sum = s.sum + val

	if val < s.last {
		s.anyCounterResets = true
	}

	s.last = val
	s.count = s.count + 1
}
