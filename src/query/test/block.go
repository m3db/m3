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
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
)

type multiSeriesBlock struct {
	consolidated     bool
	lookbackDuration time.Duration
	meta             block.Metadata
	seriesList       ts.SeriesList
	query            *storage.FetchQuery
	enableBatched    bool
}

func newMultiSeriesBlock(
	fetchResult *storage.FetchResult,
	query *storage.FetchQuery,
	lookbackDuration time.Duration,
	enableBatched bool,
) block.Block {
	meta := block.Metadata{
		Bounds: models.Bounds{
			Start:    xtime.ToUnixNano(query.Start),
			Duration: query.End.Sub(query.Start),
			StepSize: query.Interval,
		},

		ResultMetadata: fetchResult.Metadata,
	}

	return multiSeriesBlock{
		seriesList:       fetchResult.SeriesList,
		meta:             meta,
		lookbackDuration: lookbackDuration,
		enableBatched:    enableBatched,
		query:            query,
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
	return nil, errors.New("step iterator is not supported by test block")
}

func (m multiSeriesBlock) SeriesIter() (block.SeriesIter, error) {
	return newMultiSeriesBlockSeriesIter(m), nil
}

func (m multiSeriesBlock) MultiSeriesIter(
	concurrency int,
) ([]block.SeriesIterBatch, error) {
	if !m.enableBatched {
		return nil,
			errors.New("batched iterator is not supported by this test block")
	}

	batches := make([]ts.SeriesList, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		batches = append(batches, make(ts.SeriesList, 0, 10))
	}

	// round-robin series.
	for i, seriesList := range m.seriesList {
		batches[i%concurrency] = append(batches[i%concurrency], seriesList)
	}

	seriesIterBatches := make([]block.SeriesIterBatch, 0, concurrency)
	for _, batch := range batches {
		insideBlock := newMultiSeriesBlock(&storage.FetchResult{
			SeriesList: batch,
			Metadata:   m.meta.ResultMetadata,
		}, m.query, m.lookbackDuration, false)
		it, err := insideBlock.SeriesIter()
		if err != nil {
			return nil, err
		}

		seriesIterBatches = append(seriesIterBatches, block.SeriesIterBatch{
			Iter: it,
			Size: len(batch),
		})
	}

	return seriesIterBatches, nil
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
	return block.NewUnconsolidatedSeries(
		s.Values().Datapoints(),
		block.SeriesMeta{
			Tags: s.Tags,
			Name: s.Name(),
		},
		block.UnconsolidatedSeriesStats{Enabled: true},
	)
}

func (m *multiSeriesBlockSeriesIter) Err() error {
	return nil
}

func (m *multiSeriesBlockSeriesIter) Close() {
}
