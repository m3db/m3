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

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type encodedStepIterUnconsolidated struct {
	err               error
	stepTime          time.Time
	meta              block.Metadata
	seriesMeta        []block.SeriesMeta
	accumulator       *consolidators.StepLookbackAccumulator
	collectorIterator *encodedStepIterWithCollector
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

	collectorIterator := newEncodedStepIterWithCollector(
		b.lastBlock, accumulator, b.seriesBlockIterators)

	return &encodedStepIterUnconsolidated{
		stepTime:          cs.currentTime,
		meta:              b.meta,
		seriesMeta:        b.seriesMetas,
		accumulator:       accumulator,
		collectorIterator: collectorIterator,
	}, nil
}

func (it *encodedStepIterUnconsolidated) Current() block.UnconsolidatedStep {
	points := it.accumulator.AccumulateAndMoveToNext()
	return storage.NewUnconsolidatedStep(it.stepTime, points)
}

func (it *encodedStepIterUnconsolidated) Next() bool {
	if it.err != nil {
		return false
	}

	bounds := it.meta.Bounds
	if bounds.End().Before(it.stepTime) {
		return false
	}

	it.collectorIterator.nextAtTime(it.stepTime)
	it.stepTime = it.stepTime.Add(bounds.StepSize)
	if it.err = it.collectorIterator.err; it.err != nil {
		return false
	}

	return !bounds.End().Before(it.stepTime)
}

func (it *encodedStepIterUnconsolidated) StepCount() int {
	// Returns the projected step count, post-consolidation
	return it.meta.Bounds.Steps()
}

func (it *encodedStepIterUnconsolidated) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.collectorIterator.err
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
