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
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type peekValue struct {
	started  bool
	finished bool
	point    ts.Datapoint
}

type encodedStepIter struct {
	mu           sync.RWMutex
	lastBlock    bool
	started      bool
	currentTime  time.Time
	err          error
	bounds       models.Bounds
	meta         block.Metadata
	seriesMeta   []block.SeriesMeta
	seriesIters  []encoding.SeriesIterator
	seriesPeek   []peekValue
	consolidator *consolidators.LookbackConsolidator
}

func (b *encodedBlock) stepIter() block.StepIter {
	cs := b.consolidation
	consolidator := consolidators.NewLookbackConsolidator(
		time.Minute,
		cs.bounds.StepSize,
		cs.currentTime,
		len(b.seriesBlockIterators),
		cs.consolidationFn,
	)
	return &encodedStepIter{
		lastBlock:    b.lastBlock,
		currentTime:  cs.currentTime,
		bounds:       cs.bounds,
		meta:         b.meta,
		seriesMeta:   b.seriesMetas,
		seriesIters:  b.seriesBlockIterators,
		consolidator: consolidator,
	}
}

type encodedStep struct {
	time   time.Time
	values []float64
}

func (s *encodedStep) Time() time.Time   { return s.time }
func (s *encodedStep) Values() []float64 { return s.values }

func (it *encodedStepIter) Current() (block.Step, error) {
	it.mu.RLock()
	currentTime := it.currentTime
	if !it.started || currentTime.After(it.bounds.End()) {
		it.mu.RUnlock()
		return nil, fmt.Errorf("out of bounds")
	}

	if it.err != nil {
		it.mu.RUnlock()
		return nil, it.err
	}

	step := &encodedStep{
		time:   currentTime,
		values: it.consolidator.ConsolidateAndMoveToNext(),
	}

	it.mu.RUnlock()
	return step, nil
}

func (it *encodedStepIter) nextConsolidatedForStep(i int) {
	peek := it.seriesPeek[i]
	if peek.finished {
		// No next value in this iterator
		return
	}

	if peek.started {
		point := peek.point
		if point.Timestamp.After(it.currentTime) {
			// This point exists further than the current step
			// There are next values, but this point should be NaN
			return
		}

		// Currently at a potentially viable data point.
		// Record previously peeked value, and all potentially valid
		// values, then apply consolidation function to them to get the
		// consolidated point.
		it.consolidator.AddPointForIterator(point, i)
		// clear peeked point.
		it.seriesPeek[i].started = false
		// If at boundary, add the point as the current value.
		if point.Timestamp.Equal(it.currentTime) {
			return
		}
	}

	iter := it.seriesIters[i]
	// Read through iterator until finding a data point outside of the
	// range of this consolidated step; then consolidate those points into
	// a value, set the next peek value, and return true.
	for iter.Next() {
		dp, _, _ := iter.Current()

		// If this datapoint is before the current timestamp, add it as a
		// consolidation candidate.
		if !dp.Timestamp.After(it.currentTime) {
			it.seriesPeek[i].started = false
			it.consolidator.AddPointForIterator(dp, i)
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

func (it *encodedStepIter) nextConsolidated() {
	end := it.bounds.End()
	// Check that current time is not before end since end is exclusive
	if it.currentTime.After(end) {
		return
	}

	for i := range it.seriesIters {
		it.nextConsolidatedForStep(i)
	}
}

// Need to run an initial step; if there are any values
// that appear at exactly the start, they must be added.
func (it *encodedStepIter) initialStep() {
	it.seriesPeek = make([]peekValue, len(it.seriesIters))
	for i, iter := range it.seriesIters {
		if iter.Next() {
			dp, _, _ := iter.Current()
			if dp.Timestamp.Equal(it.bounds.Start) {
				it.consolidator.AddPointForIterator(dp, i)
			} else {
				it.seriesPeek[i] = peekValue{
					point: ts.Datapoint{
						Timestamp: dp.Timestamp,
						Value:     dp.Value,
					},
					started: true,
				}
			}
		}

		if err := iter.Err(); err != nil {
			it.err = err
			return
		}
	}
}

func (it *encodedStepIter) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if !it.started {
		it.initialStep()
		it.started = true
	} else {
		it.currentTime = it.currentTime.Add(it.bounds.StepSize)
		it.nextConsolidated()
	}

	nextTime := it.currentTime.Add(it.bounds.StepSize)
	// Has next values if the next step is before end boundary.
	return !it.bounds.End().Before(nextTime)
}

func (it *encodedStepIter) StepCount() int {
	return it.bounds.Steps()
}

func (it *encodedStepIter) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedStepIter) Meta() block.Metadata {
	return it.meta
}

func (it *encodedStepIter) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
