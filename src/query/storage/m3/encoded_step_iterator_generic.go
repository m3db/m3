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

package m3

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"
)

type updateFn func()

type encodedStepIterWithCollector struct {
	lastBlock bool
	finished  bool
	err       error

	stepTime   xtime.UnixNano
	bufferTime xtime.UnixNano
	blockEnd   xtime.UnixNano
	meta       block.Metadata
	seriesMeta []block.SeriesMeta

	seriesCollectors []consolidators.StepCollector
	seriesPeek       []peekValue
	seriesIters      []encoding.SeriesIterator

	updateFn updateFn

	workerPool xsync.PooledWorkerPool
	wg         sync.WaitGroup
}

// Moves to the next step for the i-th series in the block, populating
// the collector for that step. Will keep reading values until either
// hitting the next step boundary and returning, or until encountering
// a point beyond the step boundary. This point is then added to a stored
// peeked value that is consumed on the next pass.
func nextForStep(
	peek peekValue,
	iter encoding.SeriesIterator,
	collector consolidators.StepCollector,
	stepTime xtime.UnixNano,
) (peekValue, consolidators.StepCollector, error) {
	if peek.finished {
		// No next value in this iterator.
		return peek, collector, nil
	}

	if peek.started {
		point := peek.point
		if point.TimestampNanos.After(stepTime) {
			// This point exists further than the current step
			// There are next values, but this point should be NaN.
			return peek, collector, nil
		}

		// Currently at a potentially viable data point.
		// Record previously peeked value, and all potentially valid
		// values, then apply consolidation function to them to get the
		// consolidated point.
		collector.AddPoint(point)
		// clear peeked point.
		peek.started = false
		// If this point is currently at the boundary, finish here as there is no
		// need to check any additional points in the enclosed iterator.
		if point.TimestampNanos.Equal(stepTime) {
			return peek, collector, nil
		}
	}

	// Read through iterator until finding a data point outside of the
	// range of this consolidated step; then consolidate those points into
	// a value, set the next peek value.
	for iter.Next() {
		dp, _, _ := iter.Current()

		// If this datapoint is before the current timestamp, add it as a
		// consolidation candidate.
		if !dp.TimestampNanos.After(stepTime) {
			peek.started = false
			collector.AddPoint(dp)
		} else {
			// This point exists further than the current step.
			// Set peeked value to this point, then consolidate the retrieved
			// series.
			peek.point = dp
			peek.started = true
			return peek, collector, nil
		}
	}

	peek.finished = true
	return peek, collector, iter.Err()
}

func (it *encodedStepIterWithCollector) nextParallel(steps int) error {
	var (
		multiErr     xerrors.MultiError
		multiErrLock sync.Mutex
	)

	for i := range it.seriesIters {
		var (
			i         = i
			peek      = it.seriesPeek[i]
			iter      = it.seriesIters[i]
			collector = it.seriesCollectors[i]
		)

		it.wg.Add(1)
		it.workerPool.Go(func() {
			var err error
			for i := 0; i < steps && err == nil; i++ {
				peek, collector, err = nextForStep(peek, iter, collector,
					it.bufferTime.Add(time.Duration(i)*it.meta.Bounds.StepSize))
				collector.BufferStep()
			}

			it.seriesPeek[i] = peek
			it.seriesCollectors[i] = collector
			if err != nil {
				multiErrLock.Lock()
				multiErr = multiErr.Add(err)
				multiErrLock.Unlock()
			}
			it.wg.Done()
		})
	}

	it.wg.Wait()
	if it.err = multiErr.FinalError(); it.err != nil {
		return it.err
	}

	return nil
}

func (it *encodedStepIterWithCollector) nextSequential(steps int) error {
	for i, iter := range it.seriesIters {
		var (
			peek      = it.seriesPeek[i]
			collector = it.seriesCollectors[i]
			err       error
		)

		for i := 0; i < steps && err == nil; i++ {
			peek, collector, err = nextForStep(
				peek,
				iter,
				collector,
				it.bufferTime.Add(time.Duration(i)*it.meta.Bounds.StepSize),
			)
			collector.BufferStep()
		}

		it.seriesPeek[i] = peek
		it.seriesCollectors[i] = collector
		if err != nil {
			return err
		}
	}

	return nil
}

func (it *encodedStepIterWithCollector) Next() bool {
	if it.err != nil || it.finished {
		return false
	}

	if !it.bufferTime.After(it.stepTime) {
		it.bufferTime = it.stepTime

		steps := int(it.blockEnd.Sub(it.stepTime) / it.meta.Bounds.StepSize)
		if steps > consolidators.BufferSteps {
			steps = consolidators.BufferSteps
		}

		if steps > 0 {
			// NB: If no reader worker pool configured, use sequential iteration.
			if it.workerPool == nil {
				it.err = it.nextSequential(steps)
			} else {
				it.err = it.nextParallel(steps)
			}

			bufferedDuration := time.Duration(steps) * it.meta.Bounds.StepSize
			it.bufferTime = it.bufferTime.Add(bufferedDuration)
		}
	}

	if it.err != nil {
		return false
	}

	it.stepTime = it.stepTime.Add(it.meta.Bounds.StepSize)
	next := !it.blockEnd.Before(it.stepTime)
	if next && it.updateFn != nil {
		it.updateFn()
	}

	if !next {
		it.finished = true
	}

	return next
}

func (it *encodedStepIterWithCollector) StepCount() int {
	return it.meta.Bounds.Steps()
}

func (it *encodedStepIterWithCollector) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedStepIterWithCollector) Err() error {
	return it.err
}

func (it *encodedStepIterWithCollector) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
