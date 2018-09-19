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
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/storage"
	xerrors "github.com/m3db/m3x/errors"
)

// TODO: use a better seriesIterators merge here
type multiFetchResult struct {
	sync.Mutex
	iterators        encoding.SeriesIterators
	err              xerrors.MultiError
	dedupeFirstAttrs storage.Attributes
	dedupeMap        map[string]multiFetchResultSeries
	// Need to keep track of seen iterators, otherwise they are not
	// properly cleaned up and returned to the pool; can also leak
	// duplciate series iterators.
	seenIters []encoding.SeriesIterators
}

type multiFetchResultSeries struct {
	idx   int
	attrs storage.Attributes
}

func (r *multiFetchResult) cleanup() error {
	for _, iters := range r.seenIters {
		iters.Close()
	}

	return nil
}

func (r *multiFetchResult) add(
	attrs storage.Attributes,
	iterators encoding.SeriesIterators,
	err error,
) {
	r.Lock()
	defer r.Unlock()
	r.seenIters = append(r.seenIters, iterators)

	if err != nil {
		r.err = r.err.Add(err)
		return
	}

	if r.iterators == nil {
		r.iterators = iterators
		r.dedupeFirstAttrs = attrs
		return
	}

	iters := iterators.Iters()
	// Need to dedupe
	if r.dedupeMap == nil {
		r.dedupeMap = make(map[string]multiFetchResultSeries, len(iters))
		for idx, s := range iters {
			r.dedupeMap[s.ID().String()] = multiFetchResultSeries{
				idx:   idx,
				attrs: r.dedupeFirstAttrs,
			}
		}
	}

	for _, s := range iters {
		id := s.ID().String()
		existing, exists := r.dedupeMap[id]
		if exists && existing.attrs.Resolution <= attrs.Resolution {
			// Already exists and resolution of result we are adding is not as precise
			continue
		}

		// Does not exist already or more precise, add result
		var idx int
		currentIters := r.iterators.Iters()
		if !exists {
			idx = len(currentIters)
			currentIters = append(currentIters, s)
		} else {
			idx = existing.idx
			currentIters[idx] = s
		}

		// TODO: use a pool here
		r.iterators = encoding.NewSeriesIterators(currentIters, nil)
		r.dedupeMap[id] = multiFetchResultSeries{
			idx:   idx,
			attrs: attrs,
		}
	}
}
