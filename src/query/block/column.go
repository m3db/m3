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
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xcost "github.com/m3db/m3/src/x/cost"

	"github.com/uber-go/tally"
)

type column []float64

// ColumnBlockBuilder builds a block optimized for column iteration.
type ColumnBlockBuilder struct {
	block           *columnBlock
	enforcer        cost.ChainedEnforcer
	blockDatapoints tally.Counter
}

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

// StepIter returns a step-wise block iterator, giving consolidated values
// across all series comprising the box at a single time step.
func (c *columnBlock) StepIter() (StepIter, error) {
	if len(c.columns) != c.meta.Bounds.Steps() {
		return nil, fmt.
			Errorf("mismatch in block columns and meta bounds, columns: %d, bounds: %v",
				len(c.columns), c.meta.Bounds)
	}

	return &colBlockIter{
		columns:    c.columns,
		seriesMeta: c.seriesMeta,
		meta:       c.meta,
		idx:        -1,
	}, nil
}

type colSeriesIter struct {
	idx        int
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
			Value:     it.columns[i][it.idx],
		})

		start = start.Add(step)
	}

	return UnconsolidatedSeries{
		datapoints: dps,
		Meta:       it.seriesMeta[it.idx],
	}
}

// Next moves to the next item in the iterator. It will return false if there
// are no more items, or if encountering an error.
func (it *colSeriesIter) Next() bool {
	it.idx = it.idx + 1
	return it.idx < len(it.seriesMeta)
}

// Err returns any error encountered during iteration.
func (it *colSeriesIter) Err() error { return nil }

// Close frees up resources held by the iterator.
func (it *colSeriesIter) Close() { /* no-op*/ }

// SeriesIter returns a series-wise block iterator, giving unconsolidated
// by series.
func (c *columnBlock) SeriesIter() (SeriesIter, error) {
	if len(c.columns) != c.meta.Bounds.Steps() {
		return nil, fmt.
			Errorf("mismatch in block columns and meta bounds, columns: "+
				"%d, bounds: %v", len(c.columns), c.meta.Bounds)
	}

	return &colSeriesIter{
		columns:    c.columns,
		seriesMeta: c.seriesMeta,
		idx:        -1,
	}, nil
}

// MultiSeriesIter is invalid for a columnar block.
// NB: All functions using MultiSeriesIter should already contain
// provisions to fall back to SeriesIter iteration.
func (c *columnBlock) MultiSeriesIter(_ int) ([]SeriesIterBatch, error) {
	return nil, errors.New("processed columnar block does not " +
		"support multi series iteration")
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
	for _, col := range c.columns {
		col = col[:0]
		col = nil
	}

	c.columns = c.columns[:0]
	c.columns = nil
	return nil
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

func (c *colBlockIter) Current() Step {
	col := c.columns[c.idx]
	return ColStep{
		time:   c.timeForStep,
		values: col,
	}
}

// Close frees up resources held by the iterator.
func (c *colBlockIter) Close() { /*no-op*/ }

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

// NewColumnBlockBuilder creates a new column block builder.
func NewColumnBlockBuilder(
	queryCtx *models.QueryContext,
	meta Metadata,
	seriesMeta []SeriesMeta) Builder {
	return ColumnBlockBuilder{
		enforcer: queryCtx.Enforcer.Child(cost.BlockLevel),
		blockDatapoints: queryCtx.Scope.Tagged(
			map[string]string{"type": "generated"}).Counter("datapoints"),
		block: &columnBlock{
			meta:       meta,
			seriesMeta: seriesMeta,
			blockType:  BlockDecompressed,
		},
	}
}

// AppendValue adds a value to a column at index.
func (cb ColumnBlockBuilder) AppendValue(idx int, value float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	r := cb.enforcer.Add(1)
	if r.Error != nil {
		return r.Error
	}

	cb.blockDatapoints.Inc(1)

	columns[idx] = append(columns[idx], value)
	return nil
}

// AppendValues adds a slice of values to a column at index.
func (cb ColumnBlockBuilder) AppendValues(idx int, values []float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	r := cb.enforcer.Add(xcost.Cost(len(values)))
	if r.Error != nil {
		return r.Error
	}

	cb.blockDatapoints.Inc(int64(len(values)))
	columns[idx] = append(columns[idx], values...)
	return nil
}

// AddCols adds the given number of columns to the block.
func (cb ColumnBlockBuilder) AddCols(num int) error {
	if num < 1 {
		return fmt.Errorf("must add more than 0 columns, adding: %d", num)
	}

	newCols := make([]column, num)
	cb.block.columns = append(cb.block.columns, newCols...)
	return nil
}

// PopulateColumns sets all columns to the given row size.
func (cb ColumnBlockBuilder) PopulateColumns(size int) {
	cols := make([]float64, size*len(cb.block.columns))
	for i := range cb.block.columns {
		cb.block.columns[i] = cols[size*i : size*(i+1)]
	}
}

// SetRow sets a given block row to the given values and metadata.
func (cb ColumnBlockBuilder) SetRow(
	idx int,
	values []float64,
	meta SeriesMeta,
) error {
	cols := cb.block.columns
	if len(values) == 0 {
		// Sanity check. Should never happen.
		return errors.New("cannot insert empty values")
	}

	if len(values) != len(cols) {
		return fmt.Errorf("inserting column size %d does not match column size: %d",
			len(values), len(cols))
	}

	rows := len(cols[0])
	if idx < 0 || idx >= rows {
		return fmt.Errorf("cannot insert into row %d, have %d rows", idx, rows)
	}

	for i, v := range values {
		cb.block.columns[i][idx] = v
	}

	r := cb.enforcer.Add(xcost.Cost(len(values)))
	if r.Error != nil {
		return r.Error
	}

	cb.block.seriesMeta[idx] = meta
	return nil
}

// Build builds the block.
func (cb ColumnBlockBuilder) Build() Block {
	return NewAccountedBlock(cb.block, cb.enforcer)
}

// BuildAsType builds the block, forcing it to the given BlockType.
func (cb ColumnBlockBuilder) BuildAsType(blockType BlockType) Block {
	cb.block.blockType = blockType
	return NewAccountedBlock(cb.block, cb.enforcer)
}
