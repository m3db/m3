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
	"github.com/m3db/m3/src/query/storage"
	xts "github.com/m3db/m3/src/query/ts"
)

type encodedStepIterUnconsolidated struct {
	lastBlock      bool
	idx            int
	err            error
	meta           block.Metadata
	seriesMeta     []block.SeriesMeta
	seriesIters    []encoding.SeriesIterator
	expandedSeries [][]xts.Datapoints
}

func (b *encodedBlockUnconsolidated) StepIter() (
	block.UnconsolidatedStepIter,
	error,
) {
	return &encodedStepIterUnconsolidated{
		idx:            -1,
		meta:           b.meta,
		seriesMeta:     b.seriesMetas,
		seriesIters:    b.seriesBlockIterators,
		lastBlock:      b.lastBlock,
		expandedSeries: make([][]xts.Datapoints, len(b.seriesBlockIterators)),
	}, nil
}

func (it *encodedStepIterUnconsolidated) Current() block.UnconsolidatedStep {
	stepTime, _ := it.meta.Bounds.TimeForIndex(it.idx)
	stepValues := make([]xts.Datapoints, len(it.expandedSeries))
	for j, series := range it.expandedSeries {
		stepValues[j] = series[it.idx]
	}

	step := storage.NewUnconsolidatedStep(stepTime, stepValues)
	return step
}

func (it *encodedStepIterUnconsolidated) decodeSeries() bool {
	values := make(xts.Datapoints, 0, initBlockReplicaLength)
	for i, iter := range it.seriesIters {
		values = values[:0]
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

		it.expandedSeries[i] = values.AlignToBounds(it.meta.Bounds)
	}

	return true
}

func (it *encodedStepIterUnconsolidated) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	if it.idx == 0 {
		// decode the series on initial Next() call
		if success := it.decodeSeries(); !success {
			return success
		}
	}

	return it.idx < it.meta.Bounds.Steps()
}

func (it *encodedStepIterUnconsolidated) StepCount() int {
	// Returns the projected step count, post-consolidation
	return it.meta.Bounds.Steps()
}

func (it *encodedStepIterUnconsolidated) Err() error {
	return it.err
}

func (it *encodedStepIterUnconsolidated) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedStepIterUnconsolidated) Meta() block.Metadata {
	return it.meta
}

func (it *encodedStepIterUnconsolidated) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
