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
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	xts "github.com/m3db/m3/src/query/ts"
)

type encodedStepIterUnconsolidated struct {
	mu          sync.RWMutex
	lastBlock   bool
	exhausted   bool
	started     bool
	meta        block.Metadata
	validIters  []bool
	seriesMeta  []block.SeriesMeta
	seriesIters []encoding.SeriesIterator
}

func (b *encodedBlock) stepIterUnconsolidated() block.UnconsolidatedStepIter {
	return &encodedStepIterUnconsolidated{
		meta:        b.meta,
		seriesMeta:  b.seriesMetas,
		seriesIters: b.seriesBlockIterators,
		lastBlock:   b.lastBlock,
	}
}

type encodedUnconsolidatedStep struct {
	time   time.Time
	values []xts.Datapoints
}

func (s *encodedUnconsolidatedStep) Time() time.Time          { return s.time }
func (s *encodedUnconsolidatedStep) Values() []xts.Datapoints { return s.values }

func (it *encodedStepIterUnconsolidated) CurrentUnconsolidated() (
	block.UnconsolidatedStep,
	error,
) {
	it.mu.RLock()
	if !it.started || it.exhausted {
		it.mu.RUnlock()
		panic("out of bounds")
	}

	var t time.Time
	values := make([]xts.Datapoints, len(it.seriesIters))
	for i, iter := range it.seriesIters {
		// Skip this iterator if it's not valid
		if !it.validIters[i] {
			continue
		}

		dp, _, _ := iter.Current()
		if i == 0 {
			t = dp.Timestamp
		}

		values[i] = []xts.Datapoint{{
			Timestamp: dp.Timestamp,
			Value:     dp.Value,
		}}
	}

	step := &encodedUnconsolidatedStep{
		time:   t,
		values: values,
	}

	it.mu.RUnlock()
	return step, nil
}

func (it *encodedStepIterUnconsolidated) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.exhausted {
		return false
	}

	if !it.started {
		it.validIters = make([]bool, len(it.seriesIters))
		it.started = true
	}

	var anyNext bool
	for i, iter := range it.seriesIters {
		if iter.Next() {
			anyNext = true
			it.validIters[i] = true
		} else {
			it.validIters[i] = false
		}
	}

	if !anyNext {
		it.exhausted = true
	}

	return anyNext

}

func (it *encodedStepIterUnconsolidated) StepCount() int {
	panic("cannot request step count from unconsolidated step iterator")
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
