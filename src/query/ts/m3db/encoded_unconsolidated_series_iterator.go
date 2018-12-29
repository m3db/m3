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
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	xts "github.com/m3db/m3/src/query/ts"
)

type encodedSeriesIterUnconsolidated struct {
	idx         int
	err         error
	meta        block.Metadata
	series      block.UnconsolidatedSeries
	seriesMeta  []block.SeriesMeta
	seriesIters []encoding.SeriesIterator
}

func (it *encodedSeriesIterUnconsolidated) Current() block.UnconsolidatedSeries {
	return it.series
}

func (it *encodedSeriesIterUnconsolidated) Err() error {
	return it.err
}

func (it *encodedSeriesIterUnconsolidated) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	next := it.idx < len(it.seriesIters)
	if next {
		iter := it.seriesIters[it.idx]
		values := make(xts.Datapoints, 0, initBlockReplicaLength)
		for iter.Next() {
			dp, _, _ := iter.Current()
			values = append(values,
				xts.Datapoint{
					Timestamp: dp.Timestamp,
					Value:     dp.Value,
				})
		}

		if it.err = iter.Err(); it.err != nil {
			return false
		}

		alignedValues := values.AlignToBounds(it.meta.Bounds)
		it.series = block.NewUnconsolidatedSeries(alignedValues, it.seriesMeta[it.idx])
	}

	return next
}

func (it *encodedSeriesIterUnconsolidated) SeriesCount() int {
	return len(it.seriesIters)
}

func (it *encodedSeriesIterUnconsolidated) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedSeriesIterUnconsolidated) Meta() block.Metadata {
	return it.meta
}

func (it *encodedSeriesIterUnconsolidated) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
