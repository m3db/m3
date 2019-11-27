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
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
)

// FetchResultToBlockResult converts a fetch result into coordinator blocks
func FetchResultToBlockResult(
	result *FetchResult,
	query *FetchQuery,
	lookbackDuration time.Duration,
	enforcer cost.ChainedEnforcer,
) (block.Result, error) {
	multiBlock, err := NewMultiSeriesBlock(result, query, lookbackDuration)
	if err != nil {
		return block.Result{
			Metadata: block.NewResultMetadata(),
		}, err
	}

	accountedBlock := block.NewAccountedBlock(
		NewMultiBlockWrapper(multiBlock), enforcer)

	return block.Result{
		Blocks:   []block.Block{accountedBlock},
		Metadata: result.Metadata,
	}, nil
}

// NewMultiBlockWrapper returns a block wrapper over an unconsolidated block.
func NewMultiBlockWrapper(
	unconsolidatedBlock block.UnconsolidatedBlock,
) block.Block {
	return &multiBlockWrapper{
		unconsolidated: unconsolidatedBlock,
	}
}

type multiBlockWrapper struct {
	consolidateError error
	once             sync.Once
	mu               sync.RWMutex
	consolidated     block.Block
	unconsolidated   block.UnconsolidatedBlock
}

func (m *multiBlockWrapper) Meta() block.Metadata {
	return m.unconsolidated.Meta()
}

func (m *multiBlockWrapper) Info() block.BlockInfo {
	return block.NewBlockInfo(block.BlockMultiSeries)
}

func (m *multiBlockWrapper) Unconsolidated() (block.UnconsolidatedBlock, error) {
	return m.unconsolidated, nil
}

func (m *multiBlockWrapper) StepIter() (block.StepIter, error) {
	if err := m.consolidate(); err != nil {
		return nil, err
	}

	return m.consolidated.StepIter()
}

func (m *multiBlockWrapper) consolidate() error {
	m.once.Do(func() {
		m.mu.Lock()
		consolidated, err := m.unconsolidated.Consolidate()
		if err != nil {
			m.consolidateError = err
			m.mu.Unlock()
			return
		}

		m.consolidated = consolidated
		m.mu.Unlock()
	})

	return m.consolidateError
}

func (m *multiBlockWrapper) SeriesIter() (block.SeriesIter, error) {
	if err := m.consolidate(); err != nil {
		return nil, err
	}

	return m.consolidated.SeriesIter()
}

func (m *multiBlockWrapper) Close() error {
	return m.unconsolidated.Close()
}

type multiSeriesBlock struct {
	consolidated     bool
	lookbackDuration time.Duration
	meta             block.Metadata
	seriesList       ts.SeriesList
}

// NewMultiSeriesBlock returns a new unconsolidated block from a fetch result.
func NewMultiSeriesBlock(
	fetchResult *FetchResult,
	query *FetchQuery,
	lookbackDuration time.Duration,
) (block.UnconsolidatedBlock, error) {
	meta := block.Metadata{
		Bounds: models.Bounds{
			Start:    query.Start,
			Duration: query.End.Sub(query.Start),
			StepSize: query.Interval,
		},

		ResultMetadata: fetchResult.Metadata,
	}

	return multiSeriesBlock{
		seriesList:       fetchResult.SeriesList,
		meta:             meta,
		lookbackDuration: lookbackDuration,
	}, nil
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
	m.consolidated = true
	return &consolidatedBlock{
		unconsolidated:    m,
		consolidationFunc: block.TakeLast,
	}, nil
}

func (c *consolidatedBlock) Meta() block.Metadata {
	return c.unconsolidated.Meta()
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

func newMultiSeriesBlockStepIter(
	b multiSeriesBlock,
) block.UnconsolidatedStepIter {
	values := make([][]ts.Datapoints, len(b.seriesList))
	bounds := b.meta.Bounds
	for i, s := range b.seriesList {
		if b.consolidated {
			values[i] = s.Values().AlignToBounds(bounds, b.lookbackDuration, nil)
		} else {
			values[i] = s.Values().AlignToBoundsNoWriteForward(bounds,
				b.lookbackDuration, nil)
		}
	}

	return &multiSeriesBlockStepIter{
		index:  -1,
		block:  b,
		values: values,
	}
}

func (m *multiSeriesBlockStepIter) SeriesMeta() []block.SeriesMeta {
	return m.block.SeriesMeta()
}

func (m *multiSeriesBlockStepIter) Next() bool {
	if len(m.values) == 0 {
		return false
	}

	m.index++
	return m.index < len(m.values[0])
}

func (m *multiSeriesBlockStepIter) Err() error {
	return nil
}

func (m *multiSeriesBlockStepIter) Current() block.UnconsolidatedStep {
	values := make([]ts.Datapoints, len(m.values))
	for i, series := range m.values {
		values[i] = series[m.index]
	}

	bounds := m.block.meta.Bounds
	t, _ := bounds.TimeForIndex(m.index)
	return unconsolidatedStep{time: t, values: values}
}

func (m *multiSeriesBlockStepIter) StepCount() int {
	// If series has fewer points then it should return NaNs
	return m.block.StepCount()
}

// TODO: Actually free up resources
func (m *multiSeriesBlockStepIter) Close() {
}

type multiSeriesBlockSeriesIter struct {
	block        multiSeriesBlock
	index        int
	consolidated bool
}

func (m *multiSeriesBlockSeriesIter) SeriesMeta() []block.SeriesMeta {
	return m.block.SeriesMeta()
}

func newMultiSeriesBlockSeriesIter(
	block multiSeriesBlock,
) block.UnconsolidatedSeriesIter {
	return &multiSeriesBlockSeriesIter{
		block:        block,
		index:        -1,
		consolidated: block.consolidated,
	}
}

func (m *multiSeriesBlockSeriesIter) SeriesCount() int {
	return len(m.block.seriesList)
}

func (m *multiSeriesBlockSeriesIter) Next() bool {
	m.index++
	return m.index < m.SeriesCount()
}

func (m *multiSeriesBlockSeriesIter) Current() block.UnconsolidatedSeries {
	s := m.block.seriesList[m.index]
	return block.NewUnconsolidatedSeries(s.Values().Datapoints(), block.SeriesMeta{
		Tags: s.Tags,
		Name: s.Name(),
	})
}

func (m *multiSeriesBlockSeriesIter) Err() error {
	return nil
}

func (m *multiSeriesBlockSeriesIter) Close() {
}

type unconsolidatedStep struct {
	time   time.Time
	values []ts.Datapoints
}

// Time for the step.
func (s unconsolidatedStep) Time() time.Time {
	return s.time
}

// Values for the column.
func (s unconsolidatedStep) Values() []ts.Datapoints {
	return s.values
}

// NewUnconsolidatedStep returns an unconsolidated step with given values.
func NewUnconsolidatedStep(
	time time.Time,
	values []ts.Datapoints,
) block.UnconsolidatedStep {
	return unconsolidatedStep{
		time:   time,
		values: values,
	}
}
