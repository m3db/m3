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

// type encodedStepIterUnconsolidated struct {
// 	collectors []*consolidators.StepLookbackAccumulator
// 	encodedStepIterWithCollector
// 	points []ts.Datapoints
// }

// func (b *encodedBlockUnconsolidated) StepIter() (
// 	block.UnconsolidatedStepIter,
// 	error,
// ) {
// 	var (
// 		cs       = b.consolidation
// 		iters    = b.seriesBlockIterators
// 		lookback = b.options.LookbackDuration()

// 		collectors       = make([]*consolidators.StepLookbackAccumulator, len(iters))
// 		seriesCollectors = make([]consolidators.StepCollector, len(iters))
// 	)

// 	for i := range iters {
// 		collectors[i] = consolidators.NewStepLookbackAccumulator(
// 			lookback,
// 			cs.bounds.StepSize,
// 			cs.currentTime,
// 		)
// 	}

// 	for i := range collectors {
// 		seriesCollectors[i] = collectors[i]
// 	}

// 	iter := &encodedStepIterUnconsolidated{
// 		collectors: collectors,
// 		points:     make([]ts.Datapoints, 0, len(iters)),
// 		encodedStepIterWithCollector: encodedStepIterWithCollector{
// 			lastBlock: b.lastBlock,

// 			stepTime:   cs.currentTime,
// 			blockEnd:   b.meta.Bounds.End(),
// 			meta:       b.meta,
// 			seriesMeta: b.seriesMetas,

// 			seriesPeek:       make([]peekValue, len(iters)),
// 			seriesIters:      iters,
// 			seriesCollectors: seriesCollectors,

// 			workerPool: b.options.ReadWorkerPool(),
// 		},
// 	}

// 	iter.encodedStepIterWithCollector.updateFn = iter.update
// 	return iter, nil
// }

// func (it *encodedStepIterUnconsolidated) update() {
// 	it.points = it.points[:0]
// 	for _, consolidator := range it.collectors {
// 		it.points = append(it.points, consolidator.AccumulateAndMoveToNext())
// 	}
// }

// type unconsolidatedStep struct {
// 	time   time.Time
// 	values []ts.Datapoints
// }

// // Time for the step.
// func (s unconsolidatedStep) Time() time.Time {
// 	return s.time
// }

// // Values for the column.
// func (s unconsolidatedStep) Values() []ts.Datapoints {
// 	return s.values
// }

// func (it *encodedStepIterUnconsolidated) Current() block.UnconsolidatedStep {
// 	return storage.NewUnconsolidatedStep(it.stepTime, it.points)
// }
