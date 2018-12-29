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

package m3db

import (
	"math"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	xts "github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type encodedSeriesIter struct {
	idx          int
	err          error
	meta         block.Metadata
	bounds       models.Bounds
	series       block.Series
	seriesMeta   []block.SeriesMeta
	seriesIters  []encoding.SeriesIterator
	consolidator *consolidators.SeriesLookbackConsolidator
}

func (b *encodedBlock) seriesIter() block.SeriesIter {
	cs := b.consolidation
	bounds := cs.bounds
	consolidator := consolidators.NewSeriesLookbackConsolidator(
		b.lookback,
		bounds.StepSize,
		cs.currentTime,
		cs.consolidationFn,
	)
	return &encodedSeriesIter{
		idx:          -1,
		meta:         b.meta,
		bounds:       bounds,
		seriesMeta:   b.seriesMetas,
		seriesIters:  b.seriesBlockIterators,
		consolidator: consolidator,
	}
}

func (it *encodedSeriesIter) Err() error {
	return it.err
}

func (it *encodedSeriesIter) Current() block.Series {
	return it.series
}

func (it *encodedSeriesIter) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	next := it.idx < len(it.seriesIters)
	if !next {
		return false
	}

	it.consolidator.Reset(it.bounds.Start)
	iter := it.seriesIters[it.idx]
	values := make([]float64, it.bounds.Steps())
	xts.Memset(values, math.NaN())
	i := 0
	currentTime := it.bounds.Start
	for iter.Next() {
		dp, _, _ := iter.Current()
		ts := dp.Timestamp

		if !ts.After(currentTime) {
			it.consolidator.AddPoint(dp)
			continue
		}

		for {
			values[i] = it.consolidator.ConsolidateAndMoveToNext()
			i++
			currentTime = currentTime.Add(it.bounds.StepSize)

			if !ts.After(currentTime) {
				it.consolidator.AddPoint(dp)
				break
			}
		}
	}

	if it.err = iter.Err(); it.err != nil {
		return false
	}

	// Consolidate any remaining points iff has not been finished
	// Fill up any missing values with NaNs
	for ; i < len(values) && !it.consolidator.Empty(); i++ {
		values[i] = it.consolidator.ConsolidateAndMoveToNext()
	}

	it.series = block.NewSeries(values, it.seriesMeta[it.idx])
	return next
}

func (it *encodedSeriesIter) SeriesCount() int {
	return len(it.seriesIters)
}

func (it *encodedSeriesIter) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedSeriesIter) Meta() block.Metadata {
	return it.meta
}

func (it *encodedSeriesIter) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
