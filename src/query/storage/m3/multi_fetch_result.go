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
	"fmt"
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/storage"
	xerrors "github.com/m3db/m3x/errors"
)

type multiFetchResult interface {
	Add(
		attrs storage.Attributes,
		iterators encoding.SeriesIterators,
		err error,
	)

	FinalResult() (encoding.SeriesIterators, error)

	Close() error
}

// TODO: use a better seriesIterators merge here
type multiResult struct {
	sync.Mutex
	dedupeMap map[string][]multiResultSeries
	seenIters []encoding.SeriesIterators // track known iterators to avoid leaking
	err       xerrors.MultiError

	pools encoding.IteratorPools
}

func newMultiFetchResult(pools encoding.IteratorPools) multiFetchResult {
	return &multiResult{
		pools: pools,
	}
}

type multiResultSeries struct {
	attrs storage.Attributes
	iter  encoding.SeriesIterator
}

func (r *multiResult) Close() error {
	for _, iters := range r.seenIters {
		iters.Close()
	}

	return nil
}

func (r *multiResult) FinalResult() (encoding.SeriesIterators, error) {
	r.Lock()
	defer r.Unlock()
	err := r.err.FinalError()
	if err != nil {
		return nil, err
	}

	numSeries := len(r.dedupeMap)
	if numSeries == 0 {
		return encoding.EmptySeriesIterators, nil
	}

	// can short-cicuit in this case
	if len(r.seenIters) == 1 {
		return r.seenIters[0], nil
	}

	// otherwise have to create a new seriesiters
	iter := r.pools.MutableSeriesIterators().Get(numSeries)
	iter.Reset(numSeries)

	i := 0
	for _, res := range r.dedupeMap {
		if len(res) != 1 {
			return nil, fmt.Errorf("internal error during result dedupe, expected 1 id, observed: %d", len(res))
		}
		iter.SetAt(i, res[0].iter)
		i++
	}

	return iter, nil
}

func (r *multiResult) Add(
	attrs storage.Attributes,
	iterators encoding.SeriesIterators,
	err error,
) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	r.seenIters = append(r.seenIters, iterators)
	if !r.err.Empty() {
		// don't need to do anything if the final result is going to be an error
		return
	}

	iters := iterators.Iters()
	if r.dedupeMap == nil {
		r.dedupeMap = make(map[string][]multiResultSeries, len(iters))
	}

	for _, iter := range iters {
		id := iter.ID().String()
		r.dedupeMap[id] = append(r.dedupeMap[id], multiResultSeries{
			attrs: attrs,
			iter:  iter,
		})
	}

	r.dedupe()
}

func (r *multiResult) dedupe() {
	for id, serieses := range r.dedupeMap {
		if len(serieses) < 2 {
			continue
		}

		// find max resolution
		maxIdx := 0
		maxValue := serieses[0].attrs
		for i := 1; i < len(serieses); i++ {
			series := serieses[i].attrs
			if series.Resolution > maxValue.Resolution {
				maxIdx = i
				maxValue = series
			}
		}

		maxSeries := serieses[maxIdx]

		// reset slice to reuse
		for idx := range serieses {
			serieses[idx] = multiResultSeries{}
		}
		serieses = serieses[:0]

		// update map with new entry
		serieses = append(serieses, maxSeries)
		r.dedupeMap[id] = serieses
	}
}
