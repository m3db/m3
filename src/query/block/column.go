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

	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

type column struct {
	Values []float64
}

// ColumnBlockBuilder builds a block optimized for column iteration.
type ColumnBlockBuilder struct {
	block           *columnBlock
	blockDatapoints tally.Counter
}

type columnBlock struct {
	blockType  BlockType
	columns    []column
	meta       Metadata
	seriesMeta []SeriesMeta
}

func (c *columnBlock) Meta() Metadata {
	return c.meta
}

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

type seriesBlockIter struct {
	idx         int
	end         int
	curr        ts.Datapoints
	seriesMetas []SeriesMeta
	columns     []column
}

func newSeriesBlockIter(
	cols []column,
	start, end int,
	meta Metadata,
	metas []SeriesMeta,
) (SeriesIter, error) {
	var (
		dps      = make(ts.Datapoints, 0, len(cols))
		currTime = meta.Bounds.Start
		step     = meta.Bounds.StepSize
	)

	for i := 0; i < len(cols); i++ {
		dps = append(dps, ts.Datapoint{Timestamp: currTime})
		currTime = currTime.Add(step)
	}

	return &seriesBlockIter{
		idx:         start,
		end:         end,
		curr:        dps,
		seriesMetas: metas,
		columns:     cols,
	}, nil
}

func (c *seriesBlockIter) SeriesMeta() []SeriesMeta {
	return c.seriesMetas
}

func (c *seriesBlockIter) SeriesCount() int {
	return len(c.seriesMetas)
}

func (c *seriesBlockIter) Next() bool {
	c.idx++
	if c.idx > c.end {
		return false
	}

	for colIdx, val := range c.columns {
		c.curr[colIdx].Value = val.Values[c.idx]
	}

	return true
}

func (c *seriesBlockIter) Current() UnconsolidatedSeries {
	return UnconsolidatedSeries{
		datapoints: c.curr,
		Meta:       c.seriesMetas[c.idx],
	}
}

func (c *seriesBlockIter) Err() error { return nil }
func (c *seriesBlockIter) Close()     { /*no-op*/ }

func (c *columnBlock) SeriesIter() (SeriesIter, error) {
	return newSeriesBlockIter(c.columns, -1, len(c.seriesMeta)-1, c.meta, c.seriesMeta)
}

func (c *columnBlock) MultiSeriesIter(count int) ([]SeriesIterBatch, error) {
	if len(c.seriesMeta) < count {
		count = len(c.seriesMeta)
	}

	batches := make([]SeriesIterBatch, 0, count)
	batchSize := len(c.seriesMeta) / count
	start := -1
	for i := 0; i < count; i++ {
		end := start + batchSize
		if i == count-1 {
			// NB: collect all remaining elements into the last batch.
			end = len(c.seriesMeta) - 1
		}

		iter, err := newSeriesBlockIter(c.columns, start, end, c.meta, c.seriesMeta)
		if err != nil {
			return nil, err
		}

		batches = append(batches, SeriesIterBatch{
			Size: end - start,
			Iter: iter,
		})

		start = end
	}

	return batches, nil
}

func (c *columnBlock) SeriesMeta() []SeriesMeta {
	return c.seriesMeta
}

func (c *columnBlock) StepCount() int {
	return len(c.columns)
}

func (c *columnBlock) Info() BlockInfo {
	return NewBlockInfo(c.blockType)
}

// Close frees up any resources
// TODO: actually free up the resources
func (c *columnBlock) Close() error {
	return nil
}

type colBlockIter struct {
	idx         int
	timeForStep xtime.UnixNano
	err         error
	meta        Metadata
	seriesMeta  []SeriesMeta
	columns     []column
}

func (c *colBlockIter) SeriesMeta() []SeriesMeta {
	return c.seriesMeta
}

func (c *colBlockIter) StepCount() int {
	return len(c.columns)
}

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

func (c *colBlockIter) Err() error {
	return c.err
}

func (c *colBlockIter) Current() Step {
	col := c.columns[c.idx]
	return ColStep{
		time:   c.timeForStep,
		values: col.Values,
	}
}

func (c *colBlockIter) Close() { /*no-op*/ }

// ColStep is a single column containing data from multiple series at a given time step
type ColStep struct {
	time   xtime.UnixNano
	values []float64
}

// Time for the step
func (c ColStep) Time() xtime.UnixNano {
	return c.time
}

// Values for the column
func (c ColStep) Values() []float64 {
	return c.values
}

// NewColStep creates a new column step
func NewColStep(t xtime.UnixNano, values []float64) Step {
	return ColStep{time: t, values: values}
}

// NewColumnBlockBuilder creates a new column block builder
func NewColumnBlockBuilder(
	queryCtx *models.QueryContext,
	meta Metadata,
	seriesMeta []SeriesMeta) Builder {
	return ColumnBlockBuilder{
		blockDatapoints: queryCtx.Scope.Tagged(
			map[string]string{"type": "generated"}).Counter("datapoints"),
		block: &columnBlock{
			meta:       meta,
			seriesMeta: seriesMeta,
			blockType:  BlockDecompressed,
		},
	}
}

// AppendValue adds a value to a column at index
func (cb ColumnBlockBuilder) AppendValue(idx int, value float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	cb.blockDatapoints.Inc(1)

	columns[idx].Values = append(columns[idx].Values, value)
	return nil
}

// AppendValues adds a slice of values to a column at index
func (cb ColumnBlockBuilder) AppendValues(idx int, values []float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	cb.blockDatapoints.Inc(int64(len(values)))
	columns[idx].Values = append(columns[idx].Values, values...)
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
		cb.block.columns[i] = column{Values: cols[size*i : size*(i+1)]}
	}

	// NB: initialize a clean series meta list with given cap and len,
	// as row operations are done by arbitrary index.
	cb.block.seriesMeta = make([]SeriesMeta, size)
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

	rows := len(cols[0].Values)
	if idx < 0 || idx >= rows {
		return fmt.Errorf("cannot insert into row %d, have %d rows", idx, rows)
	}

	for i, v := range values {
		cb.block.columns[i].Values[idx] = v
	}

	cb.block.seriesMeta[idx] = meta
	return nil
}

// Build builds the block.
func (cb ColumnBlockBuilder) Build() Block {
	return cb.block
}

// BuildAsType builds the block, forcing it to the given BlockType.
func (cb ColumnBlockBuilder) BuildAsType(blockType BlockType) Block {
	cb.block.blockType = blockType
	return cb.block
}
