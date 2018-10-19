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

// TODO: use a better seriesIterators merge here
type multiResult struct {
	sync.Mutex
	fanout         queryFanoutType
	seenFirstAttrs storage.Attributes
	seenIters      []encoding.SeriesIterators // track known iterators to avoid leaking
	finalResult    encoding.MutableSeriesIterators
	dedupeMap      map[string]multiResultSeries
	err            xerrors.MultiError

	pools encoding.IteratorPools
}

func newMultiFetchResult(
	fanout queryFanoutType,
	pools encoding.IteratorPools,
) MultiFetchResult {
	return &multiResult{
		fanout: fanout,
		pools:  pools,
	}
}

type multiResultSeries struct {
	attrs storage.Attributes
	iter  encoding.SeriesIterator
}

func (r *multiResult) Close() error {
	r.Lock()
	defer r.Unlock()

	for _, iters := range r.seenIters {
		iters.Close()
	}
	r.seenIters = nil

	if r.finalResult != nil {
		// NB(r): Since all the series iterators in the final result are held onto
		// by the original iters in the seenIters slice we allow those iterators
		// to free iterators held onto by final result, and reset the slice for
		// the final result to zero so we avoid double returning the iterators
		// themselves.
		r.finalResult.Reset(0)
		r.finalResult.Close()
		r.finalResult = nil
	}

	r.dedupeMap = nil
	r.err = xerrors.NewMultiError()

	return nil
}

func (r *multiResult) FinalResult() (encoding.SeriesIterators, error) {
	r.Lock()
	defer r.Unlock()

	err := r.err.FinalError()
	if err != nil {
		return nil, err
	}
	if r.finalResult != nil {
		return r.finalResult, nil
	}

	if len(r.seenIters) == 0 {
		return encoding.EmptySeriesIterators, nil
	}

	// can short-cicuit in this case
	if len(r.seenIters) == 1 {
		return r.seenIters[0], nil
	}

	// otherwise have to create a new seriesiters
	numSeries := len(r.dedupeMap)
	r.finalResult = r.pools.MutableSeriesIterators().Get(numSeries)
	r.finalResult.Reset(numSeries)

	i := 0
	for _, res := range r.dedupeMap {
		r.finalResult.SetAt(i, res.iter)
		i++
	}

	return r.finalResult, nil
}

func (r *multiResult) Add(
	attrs storage.Attributes,
	newIterators encoding.SeriesIterators,
	err error,
) {
	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	if len(r.seenIters) == 0 {
		// store the first attributes seen
		r.seenFirstAttrs = attrs
	}
	r.seenIters = append(r.seenIters, newIterators)

	// Need to check the error to bail early after accumulating the iterators
	// otherwise when we close the the multi fetch result
	if !r.err.Empty() {
		// don't need to do anything if the final result is going to be an error
		return
	}

	if len(r.seenIters) < 2 {
		// don't need to create the de-dupe map until we need to actually need to
		// dedupe between two results
		return
	}

	if len(r.seenIters) == 2 {
		// need to backfill the dedupe map from the first result first
		first := r.seenIters[0]
		r.dedupeMap = make(map[string]multiResultSeries, first.Len())
		r.addOrUpdateDedupeMap(r.seenFirstAttrs, first)
	}

	// Now de-duplicate
	r.addOrUpdateDedupeMap(attrs, newIterators)
}

func (r *multiResult) addOrUpdateDedupeMap(
	attrs storage.Attributes,
	newIterators encoding.SeriesIterators,
) {
	for _, iter := range newIterators.Iters() {
		id := iter.ID().String()

		existing, exists := r.dedupeMap[id]
		if !exists {
			// Does not exist, new addition
			r.dedupeMap[id] = multiResultSeries{
				attrs: attrs,
				iter:  iter,
			}
			continue
		}

		var existsBetter bool
		switch r.fanout {
		case namespaceCoversAllQueryRange:
			// Already exists and resolution of result we are adding is not as precise
			existsBetter = existing.attrs.Resolution <= attrs.Resolution
		case namespaceCoversPartialQueryRange:
			// Already exists and either has longer retention, or the same retention
			// and result we are adding is not as precise
			existsLongerRetention := existing.attrs.Retention > attrs.Retention
			existsSameRetentionEqualOrBetterResolution :=
				existing.attrs.Retention == attrs.Retention &&
					existing.attrs.Resolution <= attrs.Resolution
			existsBetter = existsLongerRetention || existsSameRetentionEqualOrBetterResolution
		default:
			r.err = r.err.Add(fmt.Errorf("unknown query fanout type: %d", r.fanout))
			return
		}
		if existsBetter {
			// Existing result is already better
			continue
		}

		// Override
		r.dedupeMap[id] = multiResultSeries{
			attrs: attrs,
			iter:  iter,
		}
	}
}
