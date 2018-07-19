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

type multiBlockStepIter struct {
	seriesIters []block.ValueStepIterator
	index       int
	meta        block.Metadata
	blocks      ConsolidatedBlocks
}

type multiBlockSeriesIter struct {
	seriesIters []block.ValueSeriesIterator
	index       int
	meta        block.Metadata
	blocks      ConsolidatedBlocks
}

// StepIter creates a new step iterator for a given MultiSeriesBlock
func (m MultiSeriesBlock) StepIter() (block.StepIter, error) {
	return &multiBlockStepIter{
		seriesIters: newConsolidatedBlockStepIters(m.Blocks),
		index:       -1,
		meta:        m.Metadata,
		blocks:      m.Blocks,
	}, nil
}

// SeriesIter creates a new series iterator for a given MultiSeriesBlock
func (m MultiSeriesBlock) SeriesIter() (block.SeriesIter, error) {
	return &multiBlockSeriesIter{
		seriesIters: newConsolidatedBlockSeriesIters(m.Blocks),
		index:       -1,
		meta:        m.Metadata,
		blocks:      m.Blocks,
	}, nil
}

// Close frees up resources
func (m MultiSeriesBlock) Close() error {
	// todo(braskin): Actually free up resources
	return errors.New("Close not implemented")
}

func newConsolidatedBlockStepIters(blocks ConsolidatedBlocks) []block.ValueStepIterator {
	seriesBlockIters := make([]block.ValueStepIterator, len(blocks))
	if len(blocks) == 0 {
		return seriesBlockIters
	}

	nsBlocksLen := len(blocks[0].NSBlocks)
	for i, seriesBlock := range blocks {
		nsBlockStepIters := make([]block.ValueStepIterator, nsBlocksLen)
		for j, nsBlock := range seriesBlock.NSBlocks {
			nsBlockStepIter := newNSBlockStepIter(nsBlock)
			nsBlockStepIters[j] = nsBlockStepIter
		}

		seriesBlockIters[i] = &consolidatedBlockStepIter{
			nsBlockStepIters: nsBlockStepIters,
		}
	}

	return seriesBlockIters
}

func newNSBlockStepIter(nsBlock NSBlock) *nsBlockStepIter {
	return &nsBlockStepIter{
		m3dbIters: nsBlock.SeriesIterators.Iters(),
		bounds:    nsBlock.Bounds,
		idx:       -1,
	}
}

func newConsolidatedBlockSeriesIters(blocks ConsolidatedBlocks) []block.ValueSeriesIterator {
	seriesBlockIters := make([]block.ValueSeriesIterator, len(blocks))
	if len(blocks) == 0 {
		return seriesBlockIters
	}

	nsBlocksLen := len(blocks[0].NSBlocks)
	for i, seriesBlock := range blocks {
		nsBlockSeriesIters := make([]block.ValueSeriesIterator, nsBlocksLen)
		for j, nsBlock := range seriesBlock.NSBlocks {
			nsBlockSeriesIter := newNSBlockSeriesIter(nsBlock)
			nsBlockSeriesIters[j] = nsBlockSeriesIter
		}

		seriesBlockIters[i] = &consolidatedBlockSeriesIter{
			nsBlockSeriesIters: nsBlockSeriesIters,
		}
	}

	return seriesBlockIters
}

func newNSBlockSeriesIter(nsBlock NSBlock) *nsBlockSeriesIter {
	return &nsBlockSeriesIter{
		m3dbIters: nsBlock.SeriesIterators.Iters(),
		bounds:    nsBlock.Bounds,
		idx:       -1,
	}
}

// Meta returns the metadata for the step iter
func (m *multiBlockStepIter) Meta() block.Metadata {
	return m.meta
}

// SeriesMeta returns metadata for the individual timeseries
func (m *multiBlockStepIter) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.blocks))
	for i, s := range m.blocks {
		metas[i].Name = s.Metadata.Tags[models.MetricName]
		metas[i].Tags = s.Metadata.Tags
	}
	return metas
}

// StepCount returns the total steps/columns
func (m *multiBlockStepIter) StepCount() int {
	if len(m.blocks) == 0 {
		return 0
	}
	//NB(braskin): inclusive of the end
	return m.blocks[0].Metadata.Bounds.Steps()
}

// Next moves to the next item
func (m *multiBlockStepIter) Next() bool {
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
	bounds := m.meta.Bounds
	t, err := bounds.TimeForIndex(m.index)
	if err != nil {
		return nil, err
	}

	values := make([]float64, len(m.seriesIters))
	for i, s := range m.seriesIters {
		values[i] = s.Current()
	}

	return block.NewColStep(t, values), nil
}

// TODO: Actually free up resources
func (m *multiBlockStepIter) Close() {}

// Meta returns the metadata for the block
func (m *multiBlockSeriesIter) Meta() block.Metadata {
	return m.meta
}

// Current returns the slice of vals and timestamps for that series
func (m *multiBlockSeriesIter) Current() (block.Series, error) {
	meta := m.blocks[m.index].Metadata
	seriesMeta := block.SeriesMeta{
		Tags: meta.Tags,
		Name: meta.Tags[models.MetricName],
	}

	values := m.seriesIters[m.index].Current()

	return block.NewSeries(values, seriesMeta), nil
}

// Next moves to the next item
func (m *multiBlockSeriesIter) Next() bool {
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

// SeriesMeta returns the metadata for each series in the block
func (m *multiBlockSeriesIter) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.seriesIters))
	for i, s := range m.blocks {
		metas[i] = block.SeriesMeta{
			Tags: s.Metadata.Tags,
			Name: s.Metadata.Tags[models.MetricName],
		}
	}

	return metas
}

// SeriesCount returns the number of series
func (m *multiBlockSeriesIter) SeriesCount() int {
	return len(m.blocks)
}

// TODO: Actually free up resources
func (m *multiBlockSeriesIter) Close() {}
