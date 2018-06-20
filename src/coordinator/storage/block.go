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

func (m multiSeriesBlock) StepIter() block.StepIter {
	return &multiSeriesBlockStepIter{block: m}
}

func (m multiSeriesBlock) SeriesIter() block.SeriesIter {
	return newMultiSeriesBlockSeriesIter(m)
}

func (m multiSeriesBlock) SeriesMeta() []block.SeriesMeta {
	metas := make([]block.SeriesMeta, len(m.seriesList))
	for i, s := range m.seriesList {
		metas[i].Tags = s.Tags
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
	return m.index < m.block.seriesList[0].Values().Len()
}

func (m *multiSeriesBlockStepIter) Current() block.Step {
	values := make([]float64, len(m.block.seriesList))
	for i, s := range m.block.seriesList {
		values[i] = s.Values().ValueAt(i)
	}

	bounds := m.block.meta.Bounds
	t := bounds.Start.Add(time.Duration(m.index) * bounds.StepSize)
	return block.NewColStep(t, values)
}

func (m *multiSeriesBlockStepIter) Steps() int {
	if len(m.block.seriesList) == 0 {
		return 0
	}

	return m.block.seriesList[0].Values().Len()
}

// TODO: Actually free up resources
func (m *multiSeriesBlockStepIter) Close() {
}

type multiSeriesBlockSeriesIter struct {
	block multiSeriesBlock
	index int
}

func newMultiSeriesBlockSeriesIter(block multiSeriesBlock) *multiSeriesBlockSeriesIter {
	return &multiSeriesBlockSeriesIter{block: block, index: -1}
}

func (m *multiSeriesBlockSeriesIter) Next() bool {
	m.index++
	return m.index < len(m.block.seriesList)
}

func (m *multiSeriesBlockSeriesIter) Current() block.Series {
	s := m.block.seriesList[m.index]
	values := make([]float64, s.Len())
	for i := 0; i < s.Len(); i++ {
		values[i] = s.Values().ValueAt(i)
	}
	return block.NewSeries(values, m.block.meta.Bounds, s.Name())
}

func (m *multiSeriesBlockSeriesIter) Close()  {
}
