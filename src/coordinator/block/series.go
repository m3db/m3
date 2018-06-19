// Copyright (c) 2018 Uber Technologies, Inc.
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

package block

import (
	"fmt"
	"time"
)

// Series is a single series within a block
type Series struct {
	values []float64
	bounds Bounds
}

// NewSeries creates a new series
func NewSeries(values []float64, bounds Bounds) Series {
	return Series{values: values, bounds: bounds}
}

// ValueAtStep returns the datapoint value at a step idx
func (s Series) ValueAtStep(idx int) float64 {
	return s.values[idx]
}

// ValueAtTime returns the datapoint value at a given time
func (s Series) ValueAtTime(t time.Time) (float64, error) {
	bounds := s.bounds
	if t.Before(bounds.Start) || t.After(bounds.End) {
		return 0, fmt.Errorf(errBounds, t, bounds)
	}

	step := int(t.Sub(bounds.Start) / bounds.StepSize)
	if step >= len(s.values) {
		return 0, fmt.Errorf(errBounds, t, bounds)
	}

	return s.ValueAtStep(step), nil
}
