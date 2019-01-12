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
	"github.com/m3db/m3/src/query/storage"
	xts "github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type encodedStepIterUnconsolidated struct {
	lastBlock      bool
	idx            int
	err            error
	meta           block.Metadata
	seriesMeta     []block.SeriesMeta
	seriesIters    []encoding.SeriesIterator
	expandedSeries [][]xts.Datapoints

	stepTime    time.Time
	lookback    time.Duration
	accumulator *consolidators.StepLookbackAccumulator
	seriesPeek  []peekValue
}

func (b *encodedBlockUnconsolidated) StepIter() (
	block.UnconsolidatedStepIter,
	error,
) {
	cs := b.consolidation
	accumulator := consolidators.NewStepLookbackAccumulator(
		b.lookback,
		cs.bounds.StepSize,
		cs.currentTime,
		len(b.seriesBlockIterators),
	)

	return &encodedStepIterUnconsolidated{
		idx:            -1,
		meta:           b.meta,
		seriesMeta:     b.seriesMetas,
		seriesIters:    b.seriesBlockIterators,
		lastBlock:      b.lastBlock,
		expandedSeries: make([][]xts.Datapoints, len(b.seriesBlockIterators)),

		lookback:    b.lookback,
		accumulator: accumulator,
		seriesPeek:  make([]peekValue, len(b.seriesBlockIterators)),
	}, nil
}

func (it *encodedStepIterUnconsolidated) Current() block.UnconsolidatedStep {
	points := it.accumulator.AccumulateAndMoveToNext()
	return storage.NewUnconsolidatedStep(it.stepTime, points)
}

func (it *encodedStepIterUnconsolidated) nextForStep(
	i int,
	stepTime time.Time,
) {
	peek := it.seriesPeek[i]
	if peek.finished {
		// No next value in this iterator
		return
	}

	if peek.started {
		point := peek.point
		if point.Timestamp.After(stepTime) {
			// This point exists further than the current step
			// There are next values, but current step is empty.
			return
		}

		// clear peeked point.
		it.seriesPeek[i].started = false
		// Currently at a potentially viable data point.
		// Record previously peeked value, and all potentially valid values.
		it.accumulator.AddPointForIterator(point, i)

		// If at boundary, add the point as the current value.
		if point.Timestamp.Equal(stepTime) {
			return
		}
	}

	iter := it.seriesIters[i]
	// Read through iterator until finding a data point outside of the
	// range of this step; then set the next peek value.
	for iter.Next() {
		dp, _, _ := iter.Current()

		// If this datapoint is before the current timestamp, add it to
		// the accumulator
		if !dp.Timestamp.After(stepTime) {
			it.seriesPeek[i].started = false
			it.accumulator.AddPointForIterator(dp, i)
		} else {
			// This point exists further than the current step; set peeked value
			// to this point.
			it.seriesPeek[i].point = dp
			it.seriesPeek[i].started = true
			return
		}
	}
}

func (it *encodedStepIterUnconsolidated) Next() bool {
	if it.err != nil {
		return false
	}

	it.idx++
	next := it.idx < it.meta.Bounds.Steps()

	if !next {
		return false
	}

	stepTime, err := it.meta.Bounds.TimeForIndex(it.idx)
	if err != nil {
		it.err = err
		return false
	}

	for i, iter := range it.seriesIters {
		it.nextForStep(i, stepTime)
		if it.err = iter.Err(); it.err != nil {
			return false
		}
	}

	it.stepTime = stepTime
	return next
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
