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

package test

import (
	"errors"
	"math"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
)

type multiSeriesBlock struct {
	consolidated     bool
	lookbackDuration time.Duration
	meta             block.Metadata
	seriesList       ts.SeriesList
}

func newMultiSeriesBlock(
	fetchResult *storage.FetchResult,
	query *storage.FetchQuery,
	lookbackDuration time.Duration,
) block.Block {
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
	}
}

func (m multiSeriesBlock) Meta() block.Metadata {
	return m.meta
}

func (m multiSeriesBlock) StepCount() int {
	// If series has fewer points then it should return NaNs
	return m.meta.Bounds.Steps()
}

func (m multiSeriesBlock) StepIter() (block.StepIter, error) {
	return newMultiSeriesBlockStepIter(m), nil
}

func (m multiSeriesBlock) SeriesIter() (block.SeriesIter, error) {
	return newMultiSeriesBlockSeriesIter(m), nil
}

func (m multiSeriesBlock) MultiSeriesIter(
	concurrency int,
) ([]block.SeriesIterBatch, error) {
	return nil, errors.New("batched iterator is not supported by multiSeriesBlock")
}

func (m multiSeriesBlock) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.seriesList))
	for i, s := range m.seriesList {
		metas[i].Tags = s.Tags
		metas[i].Name = s.Name()
	}

	return metas
}

func (m multiSeriesBlock) Info() block.BlockInfo {
	return block.NewBlockInfo(block.BlockTest)
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
) block.StepIter {
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

type unconsolidatedStep struct {
	time   time.Time
	values []float64
}

// Time for the step.
func (s unconsolidatedStep) Time() time.Time {
	return s.time
}

// Values for the column.
func (s unconsolidatedStep) Values() []float64 {
	return s.values
}

func (m *multiSeriesBlockStepIter) Current() block.Step {
	valsAtIndex := m.values[m.index]
	v := make([]float64, 0, len(valsAtIndex))
	for _, vv := range valsAtIndex {
		last := math.NaN()
		fs := vv.Values()
		if len(fs) > 0 {
			last = fs[len(fs)-1]
		}

		v = append(v, last)
	}

	t, _ := m.block.meta.Bounds.TimeForIndex(m.index)
	return unconsolidatedStep{time: t, values: v}
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
) block.SeriesIter {
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
