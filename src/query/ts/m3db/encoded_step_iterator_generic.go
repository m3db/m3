// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type encodedStepIterWithCollector struct {
	lastBlock bool
	err       error

	stepTime   time.Time
	meta       block.Metadata
	seriesMeta []block.SeriesMeta

	collector   consolidators.StepCollector
	seriesPeek  []peekValue
	seriesIters []encoding.SeriesIterator
}

func newEncodedStepIterWithCollector(
	lastBlock bool,
	collector consolidators.StepCollector,
	seriesIters []encoding.SeriesIterator,
) *encodedStepIterWithCollector {
	return &encodedStepIterWithCollector{
		lastBlock:   lastBlock,
		collector:   collector,
		seriesIters: seriesIters,
		// TODO: pool
		seriesPeek: make([]peekValue, len(seriesIters)),
	}
}

// Moves to the next step for the i-th series in the block, populating
// the collector for that step. Will keep reading values until either
// hitting the next step boundary and returning, or until encountering
// a point beyond the step boundary. This point is then added to a stored
// peeked value that is consumed on the next pass.
func (it *encodedStepIterWithCollector) nextForStep(
	i int,
	stepTime time.Time,
) {
	peek := it.seriesPeek[i]
	if peek.finished {
		// No next value in this iterator.
		return
	}

	if peek.started {
		point := peek.point
		if point.Timestamp.After(stepTime) {
			// This point exists further than the current step
			// There are next values, but this point should be NaN.
			return
		}

		// Currently at a potentially viable data point.
		// Record previously peeked value, and all potentially valid
		// values, then apply consolidation function to them to get the
		// consolidated point.
		it.collector.AddPointForIterator(point, i)
		// clear peeked point.
		it.seriesPeek[i].started = false
		// If this point is currently at the boundary, finish here as there is no
		// need to check any additional points in the enclosed iterator.
		if point.Timestamp.Equal(stepTime) {
			return
		}
	}

	iter := it.seriesIters[i]
	// Read through iterator until finding a data point outside of the
	// range of this consolidated step; then consolidate those points into
	// a value, set the next peek value.
	for iter.Next() {
		dp, _, _ := iter.Current()

		// If this datapoint is before the current timestamp, add it as a
		// consolidation candidate.
		if !dp.Timestamp.After(stepTime) {
			it.seriesPeek[i].started = false
			it.collector.AddPointForIterator(dp, i)
		} else {
			// This point exists further than the current step.
			// Set peeked value to this point, then consolidate the retrieved
			// series.
			it.seriesPeek[i].point = dp
			it.seriesPeek[i].started = true
			return
		}
	}

	if err := iter.Err(); err != nil {
		it.err = err
	}
}

func (it *encodedStepIterWithCollector) nextAtTime(time time.Time) {
	for i := range it.seriesIters {
		it.nextForStep(i, it.stepTime)
	}
}

func (it *encodedStepIterWithCollector) Next() bool {
	if it.err != nil {
		return false
	}

	bounds := it.meta.Bounds
	if bounds.End().Before(it.stepTime) {
		return false
	}

	for i := range it.seriesIters {
		it.nextForStep(i, it.stepTime)
	}

	if it.err != nil {
		return false
	}

	it.stepTime = it.stepTime.Add(bounds.StepSize)
	return !bounds.End().Before(it.stepTime)
}

func (it *encodedStepIterWithCollector) StepCount() int {
	return it.meta.Bounds.Steps()
}

func (it *encodedStepIterWithCollector) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedStepIterWithCollector) Meta() block.Metadata {
	return it.meta
}

func (it *encodedStepIterWithCollector) Err() error {
	return it.err
}

func (it *encodedStepIterWithCollector) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
