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

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/query/block"
)

type encodedSeriesIter struct {
	mu                    sync.RWMutex
	idx                   int
	meta                  block.Metadata
	seriesMeta            []block.SeriesMeta
	consolidationSettings *consolidationSettings
	seriesIters           []encoding.SeriesIterator
}

func makeEncodedSeriesIter(
	meta block.Metadata,
	seriesMeta []block.SeriesMeta,
	seriesIters []encoding.SeriesIterator,
) block.SeriesIter {
	return &encodedSeriesIter{
		idx:         -1,
		meta:        meta,
		seriesMeta:  seriesMeta,
		seriesIters: seriesIters,
	}
}

func (it *encodedSeriesIter) currentUnconsolidated() (block.Series, error) {
	iter := it.seriesIters[it.idx]
	values := make([]float64, 0, initBlockReplicaLength)
	for iter.Next() {
		dp, _, _ := iter.Current()
		values = append(values, dp.Value)
	}

	return block.NewSeries(values, it.seriesMeta[it.idx]), nil
}

func (it *encodedSeriesIter) currentConsolidated() (block.Series, error) {
	iter := it.seriesIters[it.idx]
	cs := it.consolidationSettings
	values := make([]float64, cs.bounds.Steps())
	i := 0
	nextTime := cs.currentTime.Add(cs.bounds.StepSize)
	consolidationPoints := make([]ts.Datapoint, 0, initBlockReplicaLength)
	for iter.Next() {
		point, _, _ := iter.Current()
		time := point.Timestamp
		if time.Before(cs.currentTime) {
			// drop points before the current period
			continue
		}

		if !time.After(nextTime) {
			// within requested period; add point to consolidation list and continue
			consolidationPoints = append(consolidationPoints, point)
			continue
		}

		// consolidate points read so far, and clear counter
		values[i] = cs.consolidationFn(consolidationPoints)
		i++
		consolidationPoints = consolidationPoints[:0]

		// add in nans until the next point is in a valid consolidation block
		for ; nextTime.After(time); nextTime.Add(cs.bounds.StepSize) {
			values[i] = math.NaN()
			i++
			cs.currentTime = cs.currentTime.Add(cs.bounds.StepSize)
		}

		// add this point to next list of consolidations
		consolidationPoints = append(consolidationPoints, point)
	}

	// Consolidate any remaining points iff has not been finished

	// Fill up any missing values with NaNs

	return block.NewSeries(nil, it.seriesMeta[it.idx]), nil
}

func (it *encodedSeriesIter) Current() (block.Series, error) {
	it.mu.RLock()
	defer it.mu.RUnlock()

	if it.consolidationSettings == nil {
		return it.currentUnconsolidated()
	}

	return it.currentConsolidated()
}

func (it *encodedSeriesIter) Next() bool {
	it.mu.Lock()
	next := it.idx < len(it.seriesIters)
	it.idx++
	it.mu.Unlock()
	return next
}

func (it *encodedSeriesIter) SeriesCount() int {
	return len(it.seriesIters)
}

func (it *encodedSeriesIter) SeriesMeta() []block.SeriesMeta {
	return it.seriesMeta
}

func (it *encodedSeriesIter) Meta() block.Metadata {
	return it.meta
}

func (it *encodedSeriesIter) Close() {
	// noop, as the resources at the step may still be in use;
	// instead call Close() on the encodedBlock that generated this
}
