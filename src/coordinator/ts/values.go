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

package ts

import (
	"context"
	"math"
)

// Values holds the values for a timeseries.  It provides a minimal interface
// for storing and retrieving values in the series, with Series providing a
// more convenient interface for applications to build on top of.  Values
// objects are not specific to a given time, allowing them to be
// pre-allocated, pooled, and re-used across multiple Series.  There are
// multiple implementations of Values so that we can optimize storage based on
// the density of the series.
type Values interface {
	// Len returns the number of values present
	Len() int

	// ValueAt returns the value at the nth element
	ValueAt(n int) float64

	// The number of millisseconds represented by each index
	MillisPerStep() int

	// Slice of data values in a range
	Slice(begin, end int) Values

	// AllNaN returns true if the values are all NaN
	AllNaN() bool
}

// MutableValues is the interface for values that can be updated
type MutableValues interface {
	Values

	// Resets the values
	Reset()

	// Sets the value at the given entry
	SetValueAt(n int, v float64)
}

// NewValues returns MutableValues supporting the given number of values at the
// requested granularity.  The values start off as NaN
func NewValues(ctx context.Context, millisPerStep, numSteps int) MutableValues {
	return newValues(ctx, millisPerStep, numSteps, math.NaN())
}

func newValues(ctx context.Context, millisPerStep, numSteps int, initialValue float64) MutableValues {
	values := make([]float64, numSteps)

	// Faster way to initialize an array instead of a loop
	Memset(values, initialValue)
	vals := &float64Values{
		ctx:           ctx,
		millisPerStep: millisPerStep,
		numSteps:      numSteps,
		allNaN:        math.IsNaN(initialValue),
		values:        values,
	}

	return vals
}

type float64Values struct {
	ctx           context.Context
	millisPerStep int
	numSteps      int
	values        []float64
	allNaN        bool
}

func (b *float64Values) Reset() {
	for i := range b.values {
		b.values[i] = math.NaN()
	}
	b.allNaN = true
}

func (b *float64Values) AllNaN() bool              { return b.allNaN }
func (b *float64Values) MillisPerStep() int        { return b.millisPerStep }
func (b *float64Values) Len() int                  { return b.numSteps }
func (b *float64Values) ValueAt(point int) float64 { return b.values[point] }
func (b *float64Values) SetValueAt(point int, v float64) {
	b.allNaN = b.allNaN && math.IsNaN(v)
	b.values[point] = v
}

func (b *float64Values) Slice(begin, end int) Values {
	return &float64Values{
		ctx:           b.ctx,
		millisPerStep: b.millisPerStep,
		values:        b.values[begin:end],
		numSteps:      end - begin,
		allNaN:        false,
	}
}
