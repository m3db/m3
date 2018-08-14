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

// ScalarBlock is a block containing a single value
// over a certain bound
type ScalarBlock struct {
	val  float64
	meta Metadata
}

// assert that ScalarBlock implements Block
var _ Block = (*ScalarBlock)(nil)

// NewScalarBlockResult creates a Result consisting
// of scalar blocks over the bound
func NewScalarBlockResult(val float64, bounds Bounds) *Result {
	blocks := []Block{
		NewScalarBlock(val, bounds),
	}

	return &Result{
		Blocks: blocks,
	}
}

// NewScalarBlock creates a scalar block containing val over the bounds
func NewScalarBlock(val float64, bounds Bounds) Block {
	return &ScalarBlock{
		val: val,
		meta: Metadata{
			Bounds: bounds,
			Tags:   make(models.Tags),
		},
	}
}

// StepIter returns a StepIterator
func (b *ScalarBlock) StepIter() (StepIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.Steps()
	return &scalarStepIter{
		meta: b.meta,
		step: scalarStep{
			vals: []float64{b.val},
		},
		numVals: steps,
		idx:     -1,
	}, nil
}

// SeriesIter returns a SeriesIterator
func (b *ScalarBlock) SeriesIter() (SeriesIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.Steps()
	vals := make([]float64, steps)
	for i := range vals {
		vals[i] = b.val
	}
	return &scalarSeriesIter{
		meta: b.meta,
		vals: vals,
		idx:  -1,
	}, nil
}

// Close closes the scalar block
func (b *ScalarBlock) Close() error { return nil }

// Value returns the value for the scalar block
func (b *ScalarBlock) Value() float64 { return b.val }

type scalarStepIter struct {
	meta    Metadata
	step    scalarStep
	numVals int
	idx     int
}

// assert that scalarStepIter implements StepIter
var _ StepIter = (*scalarStepIter)(nil)

var seriesMeta = []SeriesMeta{
	SeriesMeta{
		Tags: models.Tags{},
	},
}

func (it *scalarStepIter) Close()                   { /* No-op*/ }
func (it *scalarStepIter) StepCount() int           { return it.numVals }
func (it *scalarStepIter) SeriesMeta() []SeriesMeta { return seriesMeta }
func (it *scalarStepIter) Meta() Metadata           { return it.meta }

func (it *scalarStepIter) Next() bool {
	it.idx++
	bounds := it.meta.Bounds
	it.step.time = bounds.Start.Add(time.Duration(it.idx) * bounds.StepSize)
	return it.idx < it.numVals
}

func (it *scalarStepIter) Current() (Step, error) {
	if it.idx >= it.numVals || it.idx < 0 {
		return nil, fmt.Errorf("invalid scalar index: %d, numVals: %d", it.idx, it.numVals)
	}
	return &it.step, nil
}

type scalarStep struct {
	vals []float64
	time time.Time
}

// assert that scalarStepIter implements StepIter
var _ Step = (*scalarStep)(nil)

func (it *scalarStep) Time() time.Time   { return it.time }
func (it *scalarStep) Values() []float64 { return it.vals }

type scalarSeriesIter struct {
	meta Metadata
	vals []float64
	idx  int
}

// assert that scalarStepIter implements StepIter
var _ SeriesIter = (*scalarSeriesIter)(nil)

func (it *scalarSeriesIter) Close()                   { /* No-op*/ }
func (it *scalarSeriesIter) SeriesCount() int         { return 1 }
func (it *scalarSeriesIter) SeriesMeta() []SeriesMeta { return seriesMeta }
func (it *scalarSeriesIter) Meta() Metadata           { return it.meta }
func (it *scalarSeriesIter) Next() bool {
	it.idx++
	return it.idx == 0
}

func (it *scalarSeriesIter) Current() (Series, error) {
	if it.idx != 0 {
		return Series{}, fmt.Errorf("invalid scalar index: %d, numVals: 1", it.idx)
	}
	return Series{
		Meta: SeriesMeta{
			Tags: make(models.Tags),
		},
		values: it.vals,
	}, nil
}
