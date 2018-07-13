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
	"math"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/models"
)

// StepIter creates a new step iterator for a given MultiSeriesBlock
func (m MultiSeriesBlock) StepIter() (block.StepIter, error) {
	return &multiSeriesBlockStepIter{
		seriesIters: newConsolidatedSeriesBlockIters(m.Blocks),
		index:       -1,
		meta:        m.Metadata,
		blocks:      m.Blocks,
	}, nil
}

// SeriesIter creates a new series iterator for a given MultiSeriesBlock
func (m MultiSeriesBlock) SeriesIter() (block.SeriesIter, error) {
	// todo(braskin): implement SeriesIter()
	return nil, errors.New("SeriesIter not implemented")
}

// Close frees up resources
func (m MultiSeriesBlock) Close() error {
	// todo(braskin): Actually free up resources
	return errors.New("Close not implemented")
}

func newConsolidatedSeriesBlockIters(blocks ConsolidatedSeriesBlocks) []block.ValueIterator {
	seriesBlockIters := make([]block.ValueIterator, len(blocks))
	if len(blocks) == 0 {
		return seriesBlockIters
	}

	consolidatedNSBlocksLen := len(blocks[0].ConsolidatedNSBlocks)
	for i, seriesBlock := range blocks {
		consolidatedNSBlockIters := make([]block.ValueIterator, consolidatedNSBlocksLen)
		for j, nsBlock := range seriesBlock.ConsolidatedNSBlocks {
			nsBlockIter := newConsolidatedNSBlockIter(nsBlock)
			consolidatedNSBlockIters[j] = nsBlockIter
		}
		seriesBlockIters[i] = &consolidatedSeriesBlockIter{
			consolidatedNSBlockIters: consolidatedNSBlockIters,
		}
	}

	return seriesBlockIters
}

func newConsolidatedNSBlockIter(nsBlock ConsolidatedNSBlock) *consolidatedNSBlockIter {
	return &consolidatedNSBlockIter{
		m3dbIters: nsBlock.SeriesIterators.Iters(),
		bounds:    nsBlock.Bounds,
		idx:       -1,
	}
}

// Meta returns the metadata for the step iter
func (m *multiSeriesBlockStepIter) Meta() block.Metadata {
	return m.meta
}

// SeriesMeta returns metadata for the individual timeseries
func (m *multiSeriesBlockStepIter) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.blocks))
	for i, s := range m.blocks {
		metas[i].Name = s.Metadata.Tags[models.MetricName]
		metas[i].Tags = s.Metadata.Tags
	}
	return metas
}

// StepCount returns the total steps/columns
func (m *multiSeriesBlockStepIter) StepCount() int {
	if len(m.blocks) == 0 {
		return 0
	}
	//NB(braskin): inclusive of the end
	return m.blocks[0].Metadata.Bounds.Steps()
}

// Next moves to the next item
func (m *multiSeriesBlockStepIter) Next() bool {
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
func (m *multiSeriesBlockStepIter) Current() (block.Step, error) {
	values := make([]float64, len(m.seriesIters))
	for i, s := range m.seriesIters {
		values[i] = s.Current()
	}

	bounds := m.meta.Bounds
	t, err := bounds.TimeForIndex(m.index)
	if err != nil {
		return nil, err
	}

	return block.NewColStep(t, values), nil
}

// TODO: Actually free up resources
func (m *multiSeriesBlockStepIter) Close() {}

func (c *consolidatedSeriesBlockIter) Current() float64 {
	values := make([]float64, 0, 1)
	for _, iter := range c.consolidatedNSBlockIters {
		dp := iter.Current()
		values = append(values, dp)
	}

	// todo(braskin): until we have consolidation
	return values[0]
}

// Next moves to the next item
func (c *consolidatedSeriesBlockIter) Next() bool {
	if len(c.consolidatedNSBlockIters) == 0 {
		return false
	}

	for _, nsBlock := range c.consolidatedNSBlockIters {
		if !nsBlock.Next() {
			return false
		}
	}

	return true
}

// Close closes the underlaying iterators
func (c *consolidatedSeriesBlockIter) Close() {
	// todo(braskin): implement this function
}

// Next moves to the next item
func (c *consolidatedNSBlockIter) Next() bool {
	c.idx++
	// NB(braskin): this is inclusive of the last step in the iterator
	indexTime, err := c.bounds.TimeForIndex(c.idx)
	if err != nil {
		return false
	}

	lastDP := c.lastDP
	// NB(braskin): check to make sure that the current index time is after the last
	// seen datapoint and Next() on the underlaying m3db iterator returns true
	for indexTime.After(lastDP.Timestamp) && c.nextIterator() {
		lastDP, _, _ = c.m3dbIters[c.seriesIndex].Current()
		c.lastDP = lastDP
	}

	return true
}

func (c *consolidatedNSBlockIter) nextIterator() bool {
	// todo(braskin): check bounds as well
	if len(c.m3dbIters) == 0 {
		return false
	}

	for c.seriesIndex < len(c.m3dbIters) {
		if c.m3dbIters[c.seriesIndex].Next() {
			return true
		}
		c.seriesIndex++
	}

	return false
}

// Current returns the float64 value for that step
func (c *consolidatedNSBlockIter) Current() float64 {
	lastDP := c.lastDP
	// NB(braskin): if the last datapoint is after the current step, but before the (current step+1),
	// return that datapoint, otherwise return NaN
	indexTime, err := c.bounds.TimeForIndex(c.idx)
	if err != nil {
		return math.NaN()
	}
	if !indexTime.After(lastDP.Timestamp) && indexTime.Add(c.bounds.StepSize).After(lastDP.Timestamp) {
		return lastDP.Value
	}

	return math.NaN()
}

// Close closes the underlaying iterators
func (c *consolidatedNSBlockIter) Close() {
	// todo(braskin): implement this function
}
