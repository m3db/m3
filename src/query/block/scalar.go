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

	"github.com/m3db/m3/src/query/models"
)

// Scalar is a block containing a single value over a certain bound
// This represents constant values; it greatly simplifies downstream operations by
// allowing them to treat this as a regular block, while at the same time
// having an option to optimize by accessing the scalar value directly instead
type Scalar struct {
	meta Metadata
	s    ScalarFunc
}

// NewScalar creates a scalar block containing val over the bounds
func NewScalar(s ScalarFunc, bounds models.Bounds) Block {
	return &Scalar{
		s: s,
		meta: Metadata{
			Bounds: bounds,
			Tags:   models.EmptyTags(),
		},
	}
}

// Unconsolidated returns the unconsolidated version for the block
func (b *Scalar) Unconsolidated() (UnconsolidatedBlock, error) {
	return nil, fmt.Errorf("unconsolidated view not implemented for scalar block, meta: %s", b.meta)
}

// WithMetadata updates this blocks metadata, and the metadatas for each series.
func (b *Scalar) WithMetadata(
	meta Metadata,
	_ []SeriesMeta,
) (Block, error) {
	return &Scalar{
		meta: meta,
		s:    b.s,
	}, nil
}

// StepIter returns a StepIterator
func (b *Scalar) StepIter() (StepIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.Steps()
	return &scalarStepIter{
		meta:    b.meta,
		s:       b.s,
		numVals: steps,
		idx:     -1,
	}, nil
}

// ScalarFunc determines the function to apply to generate the value at each step
type ScalarFunc func(t time.Time) float64

// SeriesIter returns a SeriesIterator
func (b *Scalar) SeriesIter() (SeriesIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.Steps()
	vals := make([]float64, steps)
	t := bounds.Start
	for i := range vals {
		vals[i] = b.s(t)
		t = t.Add(bounds.StepSize)
	}

	return &scalarSeriesIter{
		meta: b.meta,
		vals: vals,
		idx:  -1,
	}, nil
}

// Close closes the scalar block
func (b *Scalar) Close() error { return nil }

// Value returns the value for the scalar block
func (b *Scalar) Value(t time.Time) float64 {
	return b.s(t)
}

type scalarStepIter struct {
	numVals, idx int
	stepTime     time.Time
	err          error
	meta         Metadata
	s            ScalarFunc
}

// build an empty SeriesMeta
func buildSeriesMeta() SeriesMeta {
	return SeriesMeta{
		Tags: models.EmptyTags(),
	}
}

func (it *scalarStepIter) Close()                   { /* No-op*/ }
func (it *scalarStepIter) Err() error               { return it.err }
func (it *scalarStepIter) StepCount() int           { return it.numVals }
func (it *scalarStepIter) SeriesMeta() []SeriesMeta { return []SeriesMeta{buildSeriesMeta()} }
func (it *scalarStepIter) Meta() Metadata           { return it.meta }

func (it *scalarStepIter) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	next := it.idx < it.numVals
	if next {
		it.stepTime, it.err = it.Meta().Bounds.TimeForIndex(it.idx)
		if it.err != nil {
			return false
		}
	}

	return it.idx < it.numVals
}

func (it *scalarStepIter) Current() Step {
	t := it.stepTime
	return &scalarStep{
		vals: []float64{it.s(t)},
		time: t,
	}
}

type scalarStep struct {
	vals []float64
	time time.Time
}

func (it *scalarStep) Time() time.Time   { return it.time }
func (it *scalarStep) Values() []float64 { return it.vals }

type scalarSeriesIter struct {
	meta Metadata
	vals []float64
	idx  int
}

func (it *scalarSeriesIter) Close()                   { /* No-op*/ }
func (it *scalarSeriesIter) Err() error               { return nil }
func (it *scalarSeriesIter) SeriesCount() int         { return 1 }
func (it *scalarSeriesIter) SeriesMeta() []SeriesMeta { return []SeriesMeta{buildSeriesMeta()} }
func (it *scalarSeriesIter) Meta() Metadata           { return it.meta }
func (it *scalarSeriesIter) Next() bool {
	it.idx++
	return it.idx == 0
}

func (it *scalarSeriesIter) Current() Series {
	if it.idx != 0 {
		// Indicates an error with the caller, having either not called Next() or attempted
		// to get Current after Next() is false
		panic("scalar iterator out of bounds")
	}

	return Series{
		Meta:   buildSeriesMeta(),
		values: it.vals,
	}
}
