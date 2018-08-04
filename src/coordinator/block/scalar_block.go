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

	"github.com/m3db/m3db/src/coordinator/models"
)

type scalarBlock struct {
	val  float64
	meta Metadata
}

// assert that scalarBlock implements Block
var _ Block = (*scalarBlock)(nil)

// NewScalarBlockResult creates a Result consisting of scalar blocks over the bound
func NewScalarBlockResult(val float64, bounds Bounds) *Result {
	blocks := []Block{
		newScalarBlock(val, bounds),
	}

	return &Result{
		Blocks: blocks,
	}
}

func newScalarBlock(val float64, bounds Bounds) Block {
	return &scalarBlock{
		val: val,
		meta: Metadata{
			Bounds: bounds,
			Tags:   make(models.Tags),
		},
	}
}

// StepIter returns a StepIterator
func (b *scalarBlock) StepIter() (StepIter, error) {
	bounds := b.meta.Bounds
	steps := bounds.End.Sub(bounds.Start) / bounds.StepSize
	return &scalarStepIter{
		meta: b.meta,
		step: scalarStep{
			vals: []float64{b.val},
		},
		numVals: int(steps),
		idx:     -1,
	}, nil
}

func (b *scalarBlock) SeriesIter() (SeriesIter, error) { return nil, nil }
func (b *scalarBlock) Close() error                    { return nil }

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
	next bool
	meta Metadata
	vals []float64
}

// assert that scalarStepIter implements StepIter
var _ SeriesIter = (*scalarSeriesIter)(nil)

func (it *scalarSeriesIter) Close()                   { /* No-op*/ }
func (it *scalarSeriesIter) SeriesCount() int         { return 1 }
func (it *scalarSeriesIter) SeriesMeta() []SeriesMeta { return seriesMeta }
func (it *scalarSeriesIter) Meta() Metadata           { return it.meta }
func (it *scalarSeriesIter) Next() bool {
	if it.next {
		it.next = false
		return true
	}
	return false
}

func (it *scalarSeriesIter) Current() (Series, error) {
	return Series{
		Meta: SeriesMeta{
			Tags: make(models.Tags),
		},
		values: it.vals,
	}, nil
}
