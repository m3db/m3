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
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/ts"
)

type encodedSeriesIter struct {
	idx              int
	lookbackDuration time.Duration
	err              error
	meta             block.Metadata
	datapoints       ts.Datapoints
	series           block.UnconsolidatedSeries
	seriesMeta       []block.SeriesMeta
	seriesIters      []encoding.SeriesIterator
	instrumented     bool
}

func (b *encodedBlock) SeriesIter() (block.SeriesIter, error) {
	return &encodedSeriesIter{
		idx:              -1,
		meta:             b.meta,
		seriesMeta:       b.seriesMetas,
		seriesIters:      b.seriesBlockIterators,
		lookbackDuration: b.options.LookbackDuration(),
		instrumented:     b.instrumented,
	}, nil
}

func (it *encodedSeriesIter) Current() block.UnconsolidatedSeries {
	return it.series
}

func (it *encodedSeriesIter) Err() error {
	return it.err
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

	iter := it.seriesIters[it.idx]
	if it.datapoints == nil {
		it.datapoints = make(ts.Datapoints, 0, initBlockReplicaLength)
	} else {
		it.datapoints = it.datapoints[:0]
	}

	var (
		decodeTime  time.Duration
		decodeStart time.Time
	)
	if it.instrumented {
		decodeStart = time.Now()
	}

	for iter.Next() {
		dp, _, _ := iter.Current()
		it.datapoints = append(it.datapoints,
			ts.Datapoint{
				Timestamp: dp.Timestamp,
				Value:     dp.Value,
			})
	}

	if it.instrumented {
		decodeTime = time.Since(decodeStart)
	}

	if it.err = iter.Err(); it.err != nil {
		return false
	}

	it.series = block.NewUnconsolidatedSeries(block.UnconsolidatedSeriesOptions{
		Datapoints:   it.datapoints,
		Meta:         it.seriesMeta[it.idx],
		StatsEnabled: it.instrumented,
		Stats: block.UnconsolidatedSeriesStats{
			DecodeTime: decodeTime,
		},
	})

	return next
}

func (it *encodedSeriesIter) SeriesCount() int {
	return len(it.seriesIters)
}

func (it *encodedSeriesIter) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedSeriesIter) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
