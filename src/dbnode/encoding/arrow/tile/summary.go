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

type summary struct {
	set bool

	avg   float64
	min   float64
	max   float64
	sum   float64
	last  float64
	count float64

	// NB: track if any recorded values correspond to potential resets; unknown
	// if this is a counter or a gauge; instead keep track of any resets and
	// allow downstream consumers to decide if this value is valid. If it is not,
	// the raw datapoints should be available.
	withResets bool
}

func newSummary() *summary {
	s := &summary{}
	s.reset()
	return s
}

func (s *summary) reset() {
	s.set = false
	s.avg = math.NaN()
	s.min = math.NaN()
	s.max = math.NaN()
	s.sum = math.NaN()
	s.last = math.NaN()
	s.count = math.NaN()
}

func (s *summary) recordFirst(val float64) {
	s.set = true
	s.avg = val
	s.min = val
	s.max = val
	s.sum = val
	s.last = val
	s.count = 1
}

func (s *summary) record(val float64) {
	// NB: nan points do not contribute to result.
	if math.IsNaN(val) {
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
		s.max = s.max
	}

	s.sum = s.sum + val
	s.last = val
	s.count = s.count + 1

	s.avg = s.sum / s.count
}
