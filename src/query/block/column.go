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

	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
)

type column []float64

type columnBlock struct {
	blockType  BlockType
	columns    []column
	meta       Metadata
	seriesMeta []SeriesMeta
}

// Meta returns the metadata for the block.
func (c *columnBlock) Meta() Metadata {
	return c.meta
}

// SeriesMeta returns the metadata for each series in the block.
func (c *columnBlock) SeriesMeta() []SeriesMeta {
	return c.seriesMeta
}

// SeriesCount returns the number of series.
func (c *columnBlock) StepCount() int {
	return len(c.columns)
}

// Info returns information about the block.
func (c *columnBlock) Info() BlockInfo {
	return NewBlockInfo(c.blockType)
}

// Close frees up any resources.
func (c *columnBlock) Close() error {
	return nil
}

// StepIter returns a step-wise block iterator, giving consolidated values
// across all series comprising the box at a single time step.
func (c *columnBlock) StepIter() (StepIter, error) {
	steps := c.meta.Bounds.Steps()
	if len(c.columns) != steps {
		return nil, fmt.Errorf("mismatch in step columns and meta bounds, "+
			"columns: %d steps: %d bounds: %v", len(c.columns), steps, c.meta.Bounds)
	}

	return &colBlockIter{
		columns:    c.columns,
		seriesMeta: c.seriesMeta,
		meta:       c.meta,
		idx:        -1,
	}, nil
}

type colBlockIter struct {
	idx         int
	timeForStep time.Time
	err         error
	meta        Metadata
	seriesMeta  []SeriesMeta
	columns     []column
}

// SeriesMeta returns the metadata for each series in the block.
func (c *colBlockIter) SeriesMeta() []SeriesMeta {
	return c.seriesMeta
}

// StepCount returns the number of steps.
func (c *colBlockIter) StepCount() int {
	return len(c.columns)
}

// Current returns the current step for the block.
func (c *colBlockIter) Next() bool {
	if c.err != nil {
		return false
	}

	c.idx++
	next := c.idx < len(c.columns)
	if !next {
		return false
	}

	c.timeForStep, c.err = c.meta.Bounds.TimeForIndex(c.idx)
	if c.err != nil {
		return false
	}

	return next
}

// Err returns any error encountered during iteration.
func (c *colBlockIter) Err() error {
	return c.err
}

// ColStep is a single column containing data from multiple
// series at a given time step.
type ColStep struct {
	time   time.Time
	values []float64
}

// Time for the step.
func (c ColStep) Time() time.Time {
	return c.time
}

// Values for the column.
func (c ColStep) Values() []float64 {
	return c.values
}

// NewColStep creates a new column step.
func NewColStep(t time.Time, values []float64) Step {
	return ColStep{time: t, values: values}
}

func (c *colBlockIter) Current() Step {
	col := c.columns[c.idx]
	return ColStep{
		time:   c.timeForStep,
		values: col,
	}
}

// Close frees up resources held by the iterator.
func (c *colBlockIter) Close() { /*no-op*/ }

type colSeriesIter struct {
	startIdx   int
	endIdx     int
	meta       Metadata
	seriesMeta []SeriesMeta
	columns    []column
}

// SeriesMeta returns the metadata for each series in the block.
func (it *colSeriesIter) SeriesMeta() []SeriesMeta { return it.seriesMeta }

// SeriesCount returns the number of series.
func (it *colSeriesIter) SeriesCount() int { return len(it.seriesMeta) }

// Current returns the current series for the block.
func (it *colSeriesIter) Current() UnconsolidatedSeries {
	var (
		bounds = it.meta.Bounds
		steps  = bounds.Steps()
		start  = bounds.Start
		step   = bounds.StepSize

		dps = make(ts.Datapoints, 0, steps)
	)

	for i := 0; i < steps; i++ {
		dps = append(dps, ts.Datapoint{
			Timestamp: start,
			Value:     it.columns[i][it.startIdx],
		})

		start = start.Add(step)
	}

	return UnconsolidatedSeries{
		datapoints: dps,
		Meta:       it.seriesMeta[it.startIdx],
	}
}

// Next moves to the next item in the iterator. It will return false if there
// are no more items, or if encountering an error.
func (it *colSeriesIter) Next() bool {
	it.startIdx = it.startIdx + 1
	return it.startIdx < it.endIdx
}

// Err returns any error encountered during iteration.
func (it *colSeriesIter) Err() error { return nil }

// Close frees up resources held by the iterator.
func (it *colSeriesIter) Close() { /* no-op*/ }

// SeriesIter returns a series-wise block iterator, giving unconsolidated
// by series.
func (c *columnBlock) SeriesIter() (SeriesIter, error) {
	steps := c.meta.Bounds.Steps()
	if len(c.columns) != steps {
		return nil, fmt.Errorf("mismatch in series columns and meta bounds, "+
			"columns: %d steps: %d bounds: %v", len(c.columns), steps, c.meta.Bounds)
	}

	return &colSeriesIter{
		columns:    c.columns,
		meta:       c.meta,
		seriesMeta: c.seriesMeta,
		startIdx:   -1,
		endIdx:     len(c.seriesMeta),
	}, nil
}

// MultiSeriesIter is invalid for a columnar block.
// NB: All functions using MultiSeriesIter should already contain
// provisions to fall back to SeriesIter iteration.
func (c *columnBlock) MultiSeriesIter(
	concurrency int,
) ([]SeriesIterBatch, error) {
	batch, err := BuildEmptySeriesIteratorBatch(concurrency)
	if err != nil {
		return nil, err
	}

	var (
		iterCount  = len(c.seriesMeta)
		chunkSize  = iterCount / concurrency
		remainder  = iterCount % concurrency
		chunkSizes = make([]int, concurrency)
	)

	util.MemsetInt(chunkSizes, chunkSize)
	for i := 0; i < remainder; i++ {
		chunkSizes[i] = chunkSizes[i] + 1
	}

	startIdx := -1
	for i, size := range chunkSizes {
		if size == 0 {
			continue
		}

		endIdx := startIdx + size
		batch[i].Size = size
		batch[i].Iter = &colSeriesIter{
			columns:    c.columns,
			meta:       c.meta,
			seriesMeta: c.seriesMeta,
			startIdx:   startIdx,
			endIdx:     endIdx + 1,
		}

		startIdx = endIdx
	}

	return batch, nil
}
