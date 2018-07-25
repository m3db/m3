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

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
)

// MultiSeriesBlock represents a vertically oriented block
type MultiSeriesBlock struct {
	Blocks   ConsolidatedBlocks
	Metadata block.Metadata
}

// MultiSeriesBlocks is a slice of MultiSeriesBlock
type MultiSeriesBlocks []MultiSeriesBlock

// Close closes each SeriesIterator
func (m MultiSeriesBlocks) Close() {}

// StepIter creates a new step iterator for a given MultiSeriesBlock
func (m MultiSeriesBlock) StepIter() (block.StepIter, error) {
	fmt.Println("step iter being created")
	return &multiBlockStepIter{
		seriesIters: newConsolidatedBlockIters(m.Blocks),
		index:       -1,
		m:           m,
	}, nil
}

// SeriesIter creates a new series iterator for a given MultiSeriesBlock
func (m MultiSeriesBlock) SeriesIter() (block.SeriesIter, error) {
	return &multiBlockSeriesIter{
		seriesIters: newConsolidatedBlockIters(m.Blocks),
		index:       -1,
		m:           m,
	}, nil
}

// Meta returns the Meta data for the block
func (m MultiSeriesBlock) Meta() block.Metadata {
	return m.Metadata
}

// SeriesMeta returns the metadata for the underlaying series
func (m MultiSeriesBlock) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.Blocks))
	for i, s := range m.Blocks {
		metas[i].Name = s.Metadata.Tags[models.MetricName]
		metas[i].Tags = s.Metadata.Tags
	}

	return metas
}

// Close frees up resources
func (m MultiSeriesBlock) Close() error {
	return nil
}

func newConsolidatedBlockIters(blocks ConsolidatedBlocks) []block.ValueIterator {
	seriesBlockIters := make([]block.ValueIterator, len(blocks))
	if len(blocks) == 0 {
		return seriesBlockIters
	}

	nsBlocksLen := len(blocks[0].NSBlocks)
	for i, seriesBlock := range blocks {
		nsBlockIters := make([]block.ValueIterator, nsBlocksLen)
		for j, nsBlock := range seriesBlock.NSBlocks {
			nsBlockIter := newNSBlockIter(nsBlock)
			nsBlockIters[j] = nsBlockIter
		}

		seriesBlockIters[i] = &consolidatedBlockIter{
			nsBlockIters: nsBlockIters,
		}
	}

	return seriesBlockIters
}

func newNSBlockIter(nsBlock NSBlock) *nsBlockIter {
	return &nsBlockIter{
		m3dbIters: nsBlock.SeriesIterators.Iters(),
		bounds:    nsBlock.Bounds,
		idx:       -1,
	}
}

type multiBlockStepIter struct {
	seriesIters []block.ValueIterator
	index       int
	m           MultiSeriesBlock
}

// Meta returns the metadata for the step iter
func (m *multiBlockStepIter) Meta() block.Metadata {
	return m.m.Meta()
}

// SeriesMeta returns metadata for the individual timeseries
func (m *multiBlockStepIter) SeriesMeta() []block.SeriesMeta {
	return m.m.SeriesMeta()
}

// StepCount returns the total steps/columns
func (m *multiBlockStepIter) StepCount() int {
	if len(m.m.Blocks) == 0 {
		return 0
	}
	//NB(braskin): inclusive of the end
	return m.m.Blocks[0].Metadata.Bounds.Steps()
}

// Next moves to the next item
func (m *multiBlockStepIter) Next() bool {
	fmt.Println("step next being called")
	if len(m.seriesIters) == 0 {
		return false
	}

	for _, s := range m.seriesIters {
		if !s.Next() {
			return false
		}
	}

	m.index++
	return true
}

// Current returns the slice of vals and timestamps for that step
func (m *multiBlockStepIter) Current() (block.Step, error) {
	bounds := m.m.Meta().Bounds
	t, _ := bounds.TimeForIndex(m.index)

	values := make([]float64, len(m.seriesIters))
	for i, s := range m.seriesIters {
		values[i] = s.Current()
	}

	fmt.Println("step values: ", values)

	return block.NewColStep(t, values), nil
}

// TODO: Actually free up resources
func (m *multiBlockStepIter) Close() {}

type multiBlockSeriesIter struct {
	seriesIters []block.ValueIterator
	index       int
	m           MultiSeriesBlock
}

// Meta returns the metadata for the block
func (m *multiBlockSeriesIter) Meta() block.Metadata {
	return m.m.Meta()
}

// Current returns the slice of vals and timestamps for that series
func (m *multiBlockSeriesIter) Current() (block.Series, error) {
	meta := m.m.Blocks[m.index].Metadata
	seriesMeta := block.SeriesMeta{
		Tags: meta.Tags,
		Name: meta.Tags[models.MetricName],
	}

	// todo(braskin): get size from bounds
	values := make([]float64, 0)
	seriesIter := m.seriesIters[m.index]
	for seriesIter.Next() {
		values = append(values, seriesIter.Current())
	}

	fmt.Println("series values: ", values)

	return block.NewSeries(values, seriesMeta), nil
}

// Next moves to the next item
func (m *multiBlockSeriesIter) Next() bool {
	m.index++
	return m.index < len(m.seriesIters)
}

// SeriesMeta returns the metadata for each series in the block
func (m *multiBlockSeriesIter) SeriesMeta() []block.SeriesMeta {
	return m.m.SeriesMeta()
}

// SeriesCount returns the number of series
func (m *multiBlockSeriesIter) SeriesCount() int {
	return len(m.m.Blocks)
}

// TODO: Actually free up resources
func (m *multiBlockSeriesIter) Close() {}
