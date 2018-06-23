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

// ColumnBlockBuilder builds a block optimized for column iteration
type ColumnBlockBuilder struct {
	block *columnBlock
}

type columnBlock struct {
	columns    []column
	meta       Metadata
	seriesMeta []SeriesMeta
}

// Meta returns the metadata for the block
func (c *columnBlock) Meta() Metadata {
	return c.meta
}

// StepIter returns a StepIterator
func (c *columnBlock) StepIter() StepIter {
	return &colBlockIter{
		columns: c.columns,
		meta:    c.meta,
		idx:     -1,
	}
}

// TODO: allow series iteration
// SeriesIter returns a SeriesIterator
func (c *columnBlock) SeriesIter() SeriesIter {
	return newColumnBlockSeriesIter(c.columns, c.meta, c.seriesMeta)
}

// TODO: allow series iteration
// SeriesMeta returns the metadata for each series in the block
func (c *columnBlock) SeriesMeta() []SeriesMeta {
	return c.seriesMeta
}

// Close frees up any resources
// TODO: actually free up the resources
func (c *columnBlock) Close() error {
	return nil
}

type colBlockIter struct {
	columns []column
	meta    Metadata
	idx     int
}

// Next returns true if iterator has more values remaining
func (c *colBlockIter) Next() bool {
	c.idx++
	return c.idx < len(c.columns)
}

// Current returns the current step
func (c *colBlockIter) Current() Step {
	col := c.columns[c.idx]
	t, err := c.meta.Bounds.TimeForIndex(c.idx)
	// TODO: Test panic case
	if err != nil {
		panic(err)
	}

	return ColStep{
		time:   t,
		values: col.Values,
	}
}

// Steps returns the total steps
func (c *colBlockIter) Steps() int {
	return len(c.columns)
}

// Close frees up resources
func (c *colBlockIter) Close() {
}

// ColStep is a single column containing data from multiple series at a given time step
type ColStep struct {
	time   time.Time
	values []float64
}

// Time for the step
func (c ColStep) Time() time.Time {
	return c.time
}

// Values for the column
func (c ColStep) Values() []float64 {
	return c.values
}

// NewColStep creates a new column step
func NewColStep(t time.Time, values []float64) Step {
	return ColStep{time: t, values: values}
}

// NewColumnBlockBuilder creates a new column block builder
func NewColumnBlockBuilder(meta Metadata, seriesMeta []SeriesMeta) Builder {
	return ColumnBlockBuilder{
		block: &columnBlock{
			meta:       meta,
			seriesMeta: seriesMeta,
		},
	}
}

// AppendValue adds a value to a column at index
func (cb ColumnBlockBuilder) AppendValue(idx int, value float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	columns[idx].Values = append(columns[idx].Values, value)
	return nil
}

// AddCols adds new columns
func (cb ColumnBlockBuilder) AddCols(num int) error {
	newCols := make([]column, num)
	cb.block.columns = append(cb.block.columns, newCols...)
	return nil
}

// Build extracts the block
// TODO: Return an immutable copy
func (cb ColumnBlockBuilder) Build() Block {
	return cb.block
}

type column struct {
	Values []float64
}

type columnBlockSeriesIter struct {
	columns    []column
	idx        int
	blockMeta  Metadata
	seriesMeta []SeriesMeta
}

func newColumnBlockSeriesIter(columns []column, blockMeta Metadata, seriesMeta []SeriesMeta) SeriesIter {
	return &columnBlockSeriesIter{columns: columns, blockMeta: blockMeta, seriesMeta: seriesMeta, idx: -1}
}

func (m *columnBlockSeriesIter) Next() bool {
	m.idx++
	return m.idx < len(m.columns[0].Values)
}

func (m *columnBlockSeriesIter) Current() Series {
	values := make([]float64, len(m.columns))
	for i := 0; i < len(m.columns); i++ {
		values[i] = m.columns[i].Values[m.idx]
	}

	return NewSeries(values, m.seriesMeta[m.idx])
}

func (m *columnBlockSeriesIter) Close() {
}
