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

package m3

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
)

type encodedStepIter struct {
	collectors []*consolidators.StepLookbackConsolidator
	encodedStepIterWithCollector
	values []float64
}

func (b *encodedBlock) StepIter() (
	block.StepIter,
	error,
) {
	var (
		cs       = b.consolidation
		iters    = b.seriesBlockIterators
		lookback = b.options.LookbackDuration()

		collectors       = make([]*consolidators.StepLookbackConsolidator, len(iters))
		seriesCollectors = make([]consolidators.StepCollector, len(iters))
	)

	for i := range iters {
		collectors[i] = consolidators.NewStepLookbackConsolidator(
			lookback,
			cs.bounds.StepSize,
			cs.currentTime,
			cs.consolidationFn,
		)
	}

	for i := range collectors {
		seriesCollectors[i] = collectors[i]
	}

	iter := &encodedStepIter{
		collectors: collectors,
		values:     make([]float64, 0, len(iters)),
		encodedStepIterWithCollector: encodedStepIterWithCollector{
			lastBlock: b.lastBlock,

			stepTime:   cs.currentTime,
			blockEnd:   b.meta.Bounds.End(),
			meta:       b.meta,
			seriesMeta: b.seriesMetas,

			seriesPeek:       make([]peekValue, len(iters)),
			seriesIters:      iters,
			seriesCollectors: seriesCollectors,

			workerPool: b.options.ReadWorkerPool(),
		},
	}

	iter.encodedStepIterWithCollector.updateFn = iter.update
	return iter, nil
}

func (it *encodedStepIter) update() {
	it.values = it.values[:0]
	for _, consolidator := range it.collectors {
		it.values = append(it.values, consolidator.ConsolidateAndMoveToNext())
	}
}

func (it *encodedStepIter) Current() block.Step {
	return block.NewColStep(
		it.stepTime,
		it.values,
	)
}
