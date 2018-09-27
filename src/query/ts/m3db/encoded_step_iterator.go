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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/models"
)

type peekValue struct {
	finished bool
	point    ts.Datapoint
}

type consolidationSettings struct {
	consolidationFn ConsolidationFunc
	currentTime     time.Time
	bounds          *models.Bounds
	currentValues   []float64
	seriesPeek      []peekValue
}

type encodedStepIter struct {
	mu                    sync.RWMutex
	exhausted             bool
	started               bool
	meta                  block.Metadata
	seriesMeta            []block.SeriesMeta
	consolidationSettings *consolidationSettings
	seriesIters           []encoding.SeriesIterator
}

func makeEncodedStepIter(
	meta block.Metadata,
	seriesMeta []block.SeriesMeta,
	seriesIters []encoding.SeriesIterator,
) block.StepIter {
	return &encodedStepIter{
		meta:        meta,
		seriesMeta:  seriesMeta,
		seriesIters: seriesIters,
	}
}

type encodedStep struct {
	time   time.Time
	values []float64
}

func (s *encodedStep) Time() time.Time   { return s.time }
func (s *encodedStep) Values() []float64 { return s.values }

func (it *encodedStepIter) currentUnconsolidated() (block.Step, error) {
	var t time.Time
	vals := make([]float64, len(it.seriesIters))
	for i, iter := range it.seriesIters {
		dp, _, _ := iter.Current()
		if i == 0 {
			t = dp.Timestamp
		}

		vals[i] = dp.Value
	}

	return &encodedStep{
		time:   t,
		values: vals,
	}, nil
}

func (it *encodedStepIter) currentConsolidated() (block.Step, error) {
	cs := it.consolidationSettings

	return &encodedStep{
		time:   cs.currentTime,
		values: cs.currentValues,
	}, nil
}

func (it *encodedStepIter) Current() (block.Step, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if !it.started || it.exhausted {
		panic("out of bounds")
	}

	if it.consolidationSettings == nil {
		return it.currentUnconsolidated()
	}

	return it.currentConsolidated()
}

func (it *encodedStepIter) nextConsolidatedForStep(i int) bool {
	cs := it.consolidationSettings

	nextTime := cs.currentTime.Add(cs.bounds.StepSize)
	peek := cs.seriesPeek[i]
	iter := it.seriesIters[i]
	if peek.finished {
		// No next value in this iterator
		cs.currentValues[i] = math.NaN()
		return false
	}

	point := peek.point
	if point.Timestamp.After(nextTime) {
		// This point exists further than the current step
		// There are next values, but this point should be NaN
		cs.currentValues[i] = math.NaN()
		return true
	}

	// Currently at a potentially viable data point.
	// Record previously peeked value, and all potentially valid
	// values, then apply consolidation function to them to get the
	// consolidated point.
	iteratorPoints := make([]ts.Datapoint, 0, initBlockReplicaLength)
	if !point.Timestamp.Before(cs.currentTime) {
		// add peeked point if it's valid
		iteratorPoints = append(iteratorPoints, point)
	}

	// Read through iterator until finding a data point outside of the
	// range of this consolidated step; then consolidate those points into
	// a value, set the next peek value, and return true
	for iter.Next() {
		dp, _, _ := iter.Current()
		if dp.Timestamp.Before(cs.currentTime) {
			// skip this datapoint
			continue
		}

		if dp.Timestamp.After(nextTime) {
			// This point exists further than the current step.
			// Set peeked value to this point, then consolidate the retrieved
			// series.
			cs.seriesPeek[i].point = dp
			cs.currentValues[i] = cs.consolidationFn(iteratorPoints)

			return true
		}

		// If this is a valid point within the range, add it as
		// a candidate for consolidation
		iteratorPoints = append(iteratorPoints, dp)
	}

	cs.currentValues[i] = cs.consolidationFn(iteratorPoints)

	return false
}

func (it *encodedStepIter) nextConsolidated() bool {
	cs := it.consolidationSettings
	if cs.currentTime.After(cs.bounds.End()) {
		it.exhausted = true
		return false
	}

	var anyNext bool
	for i := range cs.seriesPeek {
		if it.nextConsolidatedForStep(i) {
			anyNext = true
		}
	}

	if !anyNext {
		it.exhausted = true
	}

	return anyNext
}

func (it *encodedStepIter) nextUnconsolidated() bool {
	var anyNext bool
	for _, iter := range it.seriesIters {
		if iter.Next() {
			anyNext = true
		}
	}

	return anyNext
}

func (it *encodedStepIter) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	it.started = true
	if it.exhausted {
		return false
	}

	cs := it.consolidationSettings
	if cs == nil {
		return it.nextUnconsolidated()
	}

	next := it.nextConsolidated()
	cs.currentTime = cs.currentTime.Add(cs.bounds.StepSize)
	return next
}

func (it *encodedStepIter) StepCount() int {
	cs := it.consolidationSettings
	if cs != nil {
		return cs.bounds.Steps()
	}

	// TODO: check to see if this can panic where it shouldn't
	panic("cannot request step count from unconsolidated step iterator")
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
