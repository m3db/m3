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
	"math"
	"time"

	"github.com/m3db/m3db/src/coordinator/block"
	"github.com/m3db/m3db/src/coordinator/ts"
)

// FetchResultToBlockResult converts a fetch result into coordinator blocks
func FetchResultToBlockResult(result *FetchResult, query *FetchQuery) (block.Result, error) {
	alignedSeriesList, err := result.SeriesList.Align(query.Start, query.End, query.Interval)
	if err != nil {
		return block.Result{}, err
	}

	multiBlock, err := newMultiSeriesBlock(alignedSeriesList, query)
	if err != nil {
		return block.Result{}, err
	}

	return block.Result{
		Blocks: []block.Block{multiBlock},
	}, nil
}

type multiSeriesBlock struct {
	seriesList ts.SeriesList
	meta       block.Metadata
}

func newMultiSeriesBlock(seriesList ts.SeriesList, query *FetchQuery) (multiSeriesBlock, error) {
	resolution, err := seriesList.Resolution()
	if err != nil {
		return multiSeriesBlock{}, err
	}

	meta := block.Metadata{
		Bounds: block.Bounds{
			Start:    query.Start,
			End:      query.End,
			StepSize: resolution,
		},
	}
	return multiSeriesBlock{seriesList: seriesList, meta: meta}, nil
}

func (m multiSeriesBlock) Meta() block.Metadata {
	return m.meta
}

func (m multiSeriesBlock) Steps() int {
	// If series has lesser points then it should return NaNs
	return m.meta.Bounds.Steps()
}

func (m multiSeriesBlock) Series() int {
	if len(m.seriesList) == 0 {
		return 0
	}

	return len(m.seriesList)
}

func (m multiSeriesBlock) StepIter() block.StepIter {
	return &multiSeriesBlockStepIter{block: m, index: -1}
}

func (m multiSeriesBlock) SeriesIter() block.SeriesIter {
	return newMultiSeriesBlockSeriesIter(m)
}

func (m multiSeriesBlock) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.seriesList))
	for i, s := range m.seriesList {
		metas[i].Tags = s.Tags
		metas[i].Name = s.Name()
	}

	return metas
}

// TODO: Actually free up resources
func (m multiSeriesBlock) Close() error {
	return nil
}

type multiSeriesBlockStepIter struct {
	block multiSeriesBlock
	index int
}

func (m *multiSeriesBlockStepIter) Next() bool {
	if len(m.block.seriesList) == 0 {
		return false
	}

	m.index++
	return m.index < m.block.Steps()
}

func (m *multiSeriesBlockStepIter) Current() block.Step {
	values := make([]float64, len(m.block.seriesList))
	seriesLen := m.block.seriesList[0].Len()
	for i, s := range m.block.seriesList {
		if m.index < seriesLen {
			values[i] = s.Values().ValueAt(m.index)
		} else {
			values[i] = math.NaN()
		}
	}

	bounds := m.block.meta.Bounds
	t := bounds.Start.Add(time.Duration(m.index) * bounds.StepSize)
	return block.NewColStep(t, values)
}

// TODO: Actually free up resources
func (m *multiSeriesBlockStepIter) Close() {
}

type multiSeriesBlockSeriesIter struct {
	block multiSeriesBlock
	index int
}

func newMultiSeriesBlockSeriesIter(block multiSeriesBlock) block.SeriesIter {
	return &multiSeriesBlockSeriesIter{block: block, index: -1}
}

func (m *multiSeriesBlockSeriesIter) Next() bool {
	m.index++
	return m.index < len(m.block.seriesList)
}

func (m *multiSeriesBlockSeriesIter) Current() block.Series {
	s := m.block.seriesList[m.index]
	seriesLen := s.Values().Len()
	values := make([]float64, m.block.Steps())
	for i := 0; i < m.block.Steps(); i++ {
		if i < seriesLen {
			values[i] = s.Values().ValueAt(i)
		} else {
			values[i] = math.NaN()
		}
	}

	return block.NewSeries(values, block.SeriesMeta{
		Tags: s.Tags,
		Name: s.Name(),
	})
}

func (m *multiSeriesBlockSeriesIter) Close() {
}
