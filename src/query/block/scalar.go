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
// This represents constant values; it greatly simplifies downstream operations
// by allowing them to treat this as a regular block, while at the same time
// having an option to optimize by accessing the scalar value directly instead.
type Scalar struct {
	val  float64
	meta Metadata
}

// NewScalar creates a scalar block whose value is given by the function over
// the metadata bounds.
func NewScalar(
	val float64,
	meta Metadata,
) Block {
	// NB: sanity check to ensure scalar values always have clean metadata.
	meta.ResultMetadata = NewResultMetadata()
	return &Scalar{
		val:  val,
		meta: meta,
	}
}

func (c *Scalar) Info() BlockInfo {
	return NewBlockInfo(BlockScalar)
}

// Unconsolidated returns the unconsolidated version for the block.
func (b *Scalar) Unconsolidated() (UnconsolidatedBlock, error) {
	return nil, fmt.Errorf(
		"unconsolidated view not implemented for scalar block, meta: %s", b.meta)
}

func (b *Scalar) Meta() Metadata {
	return b.meta
}

func (b *Scalar) StepIter() (StepIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.Steps()
	return &scalarStepIter{
		meta:    b.meta,
		vals:    []float64{b.val},
		numVals: steps,
		idx:     -1,
	}, nil
}

func (b *Scalar) SeriesIter() (SeriesIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.Steps()
	vals := make([]float64, steps)
	t := bounds.Start
	for i := range vals {
		vals[i] = b.val
		t = t.Add(bounds.StepSize)
	}

	return &scalarSeriesIter{
		meta: b.meta,
		vals: vals,
		idx:  -1,
	}, nil
}

func (b *Scalar) Close() error { return nil }

func (b *Scalar) Value() float64 {
	return b.val
}

type scalarStepIter struct {
	numVals, idx int
	stepTime     time.Time
	err          error
	meta         Metadata
	vals         []float64
}

// build an empty SeriesMetadata.
func buildSeriesMeta(meta Metadata) SeriesMeta {
	return SeriesMeta{
		Tags: models.NewTags(0, meta.Tags.Opts),
	}
}

func (it *scalarStepIter) Close()         { /* No-op*/ }
func (it *scalarStepIter) Err() error     { return it.err }
func (it *scalarStepIter) StepCount() int { return it.numVals }
func (it *scalarStepIter) SeriesMeta() []SeriesMeta {
	return []SeriesMeta{buildSeriesMeta(it.meta)}
}

func (it *scalarStepIter) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	next := it.idx < it.numVals
	if !next {
		return false
	}

	it.stepTime, it.err = it.meta.Bounds.TimeForIndex(it.idx)
	if it.err != nil {
		return false
	}

	return next
}

func (it *scalarStepIter) Current() Step {
	t := it.stepTime
	return &scalarStep{
		vals: it.vals,
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

func (it *scalarSeriesIter) Close()           { /* No-op*/ }
func (it *scalarSeriesIter) Err() error       { return nil }
func (it *scalarSeriesIter) SeriesCount() int { return 1 }
func (it *scalarSeriesIter) SeriesMeta() []SeriesMeta {
	return []SeriesMeta{buildSeriesMeta(it.meta)}
}

func (it *scalarSeriesIter) Next() bool {
	it.idx++
	return it.idx == 0
}

func (it *scalarSeriesIter) Current() Series {
	return Series{
		Meta:   buildSeriesMeta(it.meta),
		values: it.vals,
	}
}
