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
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	xts "github.com/m3db/m3/src/query/ts"
)

type encodedStepIterUnconsolidated struct {
	mu          sync.RWMutex
	lastBlock   bool
	exhausted   bool
	err         error
	meta        block.Metadata
	validIters  []bool
	seriesMeta  []block.SeriesMeta
	seriesIters []encoding.SeriesIterator
}

func (it *encodedStepIterUnconsolidated) Current() (
	block.UnconsolidatedStep,
	error,
) {
	it.mu.RLock()
	if it.exhausted {
		it.mu.RUnlock()
		return nil, fmt.Errorf("out of bounds")
	}

	if it.err != nil {
		it.mu.RUnlock()
		return nil, it.err
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

	step := storage.NewUnconsolidatedStep(t, values)
	it.mu.RUnlock()
	return step, nil
}

func (it *encodedStepIterUnconsolidated) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()
	if it.exhausted {
		return false
	}

	var anyNext bool
	for i, iter := range it.seriesIters {
		if iter.Next() {
			anyNext = true
			it.validIters[i] = true
		} else {
			it.validIters[i] = false
		}

		if err := iter.Err(); err != nil {
			it.err = err
		}
	}

	if !anyNext {
		it.exhausted = true
	}

	return anyNext
}

func (it *encodedStepIterUnconsolidated) StepCount() int {
	// Returns the projected step count, post-consolidation
	return it.meta.Bounds.Steps()
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
