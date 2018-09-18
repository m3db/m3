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

package storage

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
)

// FetchResultToBlockResult converts a fetch result into coordinator blocks
func FetchResultToBlockResult(result *FetchResult, query *FetchQuery) (block.Result, error) {
	multiBlock, err := NewMultiSeriesBlock(result.SeriesList, query)
	if err != nil {
		return block.Result{}, err
	}

	return block.Result{
		Blocks: []block.Block{NewMultiBlockWrapper(multiBlock)},
	}, nil
}

// NewMultiBlockWrapper returns a block wrapper over an unconsolidated block
func NewMultiBlockWrapper(unconsolidatedBlock block.UnconsolidatedBlock) block.Block {
	return &multiBlockWrapper{
		unconsolidated: unconsolidatedBlock,
	}
}

type multiBlockWrapper struct {
	unconsolidated block.UnconsolidatedBlock
	consolidated   block.Block
	mu             sync.Mutex
}

func (m *multiBlockWrapper) Unconsolidated() (block.UnconsolidatedBlock, error) {
	return m.unconsolidated, nil
}

func (m *multiBlockWrapper) StepIter() (block.StepIter, error) {
	if err := m.tryConsolidate(); err != nil {
		return nil, err
	}

	return m.consolidated.StepIter()
}

func (m *multiBlockWrapper) tryConsolidate() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.consolidated == nil {
		consolidated, err := m.unconsolidated.Consolidate()
		if err != nil {
			return err
		}

		m.consolidated = consolidated
	}

	return nil
}

func (m *multiBlockWrapper) SeriesIter() (block.SeriesIter, error) {
	if err := m.tryConsolidate(); err != nil {
		return nil, err
	}

	return m.consolidated.SeriesIter()
}

func (m *multiBlockWrapper) Close() error {
	return m.unconsolidated.Close()
}

type multiSeriesBlock struct {
	seriesList ts.SeriesList
	meta       block.Metadata
}

// NewMultiSeriesBlock returns a new unconsolidated block
func NewMultiSeriesBlock(seriesList ts.SeriesList, query *FetchQuery) (block.UnconsolidatedBlock, error) {
	meta := block.Metadata{
		Bounds: models.Bounds{
			Start:    query.Start,
			Duration: query.End.Sub(query.Start),
			StepSize: query.Interval,
		},
	}
	return multiSeriesBlock{seriesList: seriesList, meta: meta}, nil
}

func (m multiSeriesBlock) Meta() block.Metadata {
	return m.meta
}

func (m multiSeriesBlock) StepCount() int {
	// If series has fewer points then it should return NaNs
	return m.meta.Bounds.Steps()
}

func (m multiSeriesBlock) StepIter() (block.UnconsolidatedStepIter, error) {
	return newMultiSeriesBlockStepIter(m), nil
}

func (m multiSeriesBlock) SeriesIter() (block.UnconsolidatedSeriesIter, error) {
	return newMultiSeriesBlockSeriesIter(m), nil
}

func (m multiSeriesBlock) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.seriesList))
	for i, s := range m.seriesList {
		metas[i].Tags = s.Tags
		metas[i].Name = s.Name()
	}

	return metas
}

func (m multiSeriesBlock) Consolidate() (block.Block, error) {
	return &consolidatedBlock{
		unconsolidated:    m,
		consolidationFunc: block.TakeLast,
	}, nil
}

// TODO: Actually free up resources
func (m multiSeriesBlock) Close() error {
	return nil
}

type multiSeriesBlockStepIter struct {
	block  multiSeriesBlock
	index  int
	values [][]ts.Datapoints
}

func newMultiSeriesBlockStepIter(block multiSeriesBlock) block.UnconsolidatedStepIter {
	values := make([][]ts.Datapoints, len(block.seriesList))
	bounds := block.meta.Bounds
	for i, series := range block.seriesList {
		values[i] = series.Values().AlignToBounds(bounds)
	}

	return &multiSeriesBlockStepIter{
		index:  -1,
		block:  block,
		values: values,
	}
}

func (m *multiSeriesBlockStepIter) SeriesMeta() []block.SeriesMeta {
	return m.block.SeriesMeta()
}

func (m *multiSeriesBlockStepIter) Meta() block.Metadata {
	return m.block.Meta()
}

func (m *multiSeriesBlockStepIter) Next() bool {
	if len(m.values) == 0 {
		return false
	}

	m.index++
	return m.index < len(m.values[0])
}

func (m *multiSeriesBlockStepIter) Current() (block.UnconsolidatedStep, error) {
	values := make([]ts.Datapoints, len(m.values))
	for i, series := range m.values {
		values[i] = series[m.index]

	}

	bounds := m.block.meta.Bounds
	t, _ := bounds.TimeForIndex(m.index)
	return unconsolidatedStep{time: t, values: values}, nil
}

func (m *multiSeriesBlockStepIter) StepCount() int {
	// If series has fewer points then it should return NaNs
	return m.block.StepCount()
}

// TODO: Actually free up resources
func (m *multiSeriesBlockStepIter) Close() {
}

type multiSeriesBlockSeriesIter struct {
	block multiSeriesBlock
	index int
}

func (m *multiSeriesBlockSeriesIter) Meta() block.Metadata {
	return m.block.Meta()
}

func (m *multiSeriesBlockSeriesIter) SeriesMeta() []block.SeriesMeta {
	return m.block.SeriesMeta()
}

func newMultiSeriesBlockSeriesIter(block multiSeriesBlock) block.UnconsolidatedSeriesIter {
	return &multiSeriesBlockSeriesIter{block: block, index: -1}
}

func (m *multiSeriesBlockSeriesIter) SeriesCount() int {
	return len(m.block.seriesList)
}

func (m *multiSeriesBlockSeriesIter) Next() bool {
	m.index++
	return m.index < m.SeriesCount()
}

func (m *multiSeriesBlockSeriesIter) Current() (block.UnconsolidatedSeries, error) {
	s := m.block.seriesList[m.index]
	values := make([]ts.Datapoints, m.block.StepCount())
	seriesValues := s.Values().AlignToBounds(m.block.meta.Bounds)
	seriesLen := len(seriesValues)
	for i := 0; i < m.block.StepCount(); i++ {
		if i < seriesLen {
			values[i] = seriesValues[i]
		} else {
			values[i] = nil
		}
	}

	return block.NewUnconsolidatedSeries(values, block.SeriesMeta{
		Tags: s.Tags,
		Name: s.Name(),
	}), nil
}

func (m *multiSeriesBlockSeriesIter) Close() {
}

type unconsolidatedStep struct {
	time   time.Time
	values []ts.Datapoints
}

// Time for the step
func (s unconsolidatedStep) Time() time.Time {
	return s.time
}

// Values for the column
func (s unconsolidatedStep) Values() []ts.Datapoints {
	return s.values
}
