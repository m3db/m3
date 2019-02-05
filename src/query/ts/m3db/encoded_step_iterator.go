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
	"github.com/m3db/m3/src/query/ts/m3db/consolidators"
)

type encodedStepIter struct {
	lastBlock         bool
	err               error
	stepTime          time.Time
	meta              block.Metadata
	seriesMeta        []block.SeriesMeta
	consolidator      *consolidators.StepLookbackConsolidator
	collectorIterator *encodedStepIterWithCollector
}

func (b *encodedBlock) stepIter() block.StepIter {
	cs := b.consolidation
	consolidator := consolidators.NewStepLookbackConsolidator(
		b.lookback,
		cs.bounds.StepSize,
		cs.currentTime,
		len(b.seriesBlockIterators),
		cs.consolidationFn,
	)

	collectorIterator := newEncodedStepIterWithCollector(b.lastBlock,
		consolidator, b.seriesBlockIterators)

	return &encodedStepIter{
		lastBlock:         b.lastBlock,
		stepTime:          cs.currentTime,
		meta:              b.meta,
		seriesMeta:        b.seriesMetas,
		consolidator:      consolidator,
		collectorIterator: collectorIterator,
	}
}

type encodedStep struct {
	time   time.Time
	values []float64
}

func (s *encodedStep) Time() time.Time   { return s.time }
func (s *encodedStep) Values() []float64 { return s.values }

func (it *encodedStepIter) Current() block.Step {
	return &encodedStep{
		time:   it.stepTime,
		values: it.consolidator.ConsolidateAndMoveToNext(),
	}
}

func (it *encodedStepIter) Next() bool {
	if it.err != nil {
		return false
	}

	bounds := it.meta.Bounds
	checkNextTime := it.stepTime.Add(bounds.StepSize * 2)
	if bounds.End().Before(checkNextTime) {
		return false
	}

	it.collectorIterator.nextAtTime(it.stepTime)
	it.stepTime = it.stepTime.Add(bounds.StepSize)
	if it.err = it.collectorIterator.err; it.err != nil {
		return false
	}

	nextTime := it.stepTime.Add(bounds.StepSize)
	// Has next values if the next step is before end boundary.
	return !bounds.End().Before(nextTime)
}

func (it *encodedStepIter) StepCount() int {
	return it.meta.Bounds.Steps()
}

func (it *encodedStepIter) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedStepIter) Meta() block.Metadata {
	return it.meta
}

func (it *encodedStepIter) Err() error {
	if it.err != nil {
		return it.err
	}

	return it.collectorIterator.err
}

func (it *encodedStepIter) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
